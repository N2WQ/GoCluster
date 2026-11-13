// File role: Runs bounded directed VOACAP forecasts and selects the requested band.
// Worker scheduling, cache ownership, and persistence live in voacap_fallback.go.
package pathreliability

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"dxcluster/internal/voacap"
)

type VOACAPRunnerClosedForecaster struct {
	cfg    VOACAPFallbackConfig
	runner voacap.Runner
}

func NewVOACAPRunnerClosedForecaster(cfg VOACAPFallbackConfig) VOACAPRunnerClosedForecaster {
	return VOACAPRunnerClosedForecaster{
		cfg:    cfg,
		runner: voacap.NewRunner(cfg.VOACAPHome),
	}
}

func (f VOACAPRunnerClosedForecaster) ForecastClosed(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
	if job.WindowStartUTC.IsZero() || f.cfg.ForecastHours < 1 || f.cfg.ForecastHours > 24 {
		return VOACAPClosedForecast{}, fmt.Errorf("VOACAP requires an explicit window start and 1..24 forecast hours")
	}
	start := job.WindowStartUTC.UTC().Truncate(time.Hour)
	// A rolling window owns at most 24 hours and therefore at most two UTC
	// calendars. Complete clock-day decks avoid the native TIME wrap ambiguity.
	var calendars [2]struct {
		month    time.Time
		forecast VOACAPClosedForecast
	}
	var paths [2]string
	count := 0
	forecast := VOACAPClosedForecast{WindowStartUTC: start}
	for i := 0; i < f.cfg.ForecastHours; i++ {
		at := start.Add(time.Duration(i) * time.Hour)
		month := time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC)
		if count > 0 && calendars[count-1].month.Equal(month) {
			continue
		}
		if count == len(calendars) {
			return VOACAPClosedForecast{}, fmt.Errorf("VOACAP window spans more than two calendar months")
		}
		monthJob := job
		monthJob.WindowStartUTC = month
		monthly, path, elapsed, err := f.forecastCalendar(ctx, monthJob)
		if err != nil {
			return VOACAPClosedForecast{}, err
		}
		calendars[count].month, calendars[count].forecast = month, monthly
		paths[count] = path
		forecast.Elapsed += elapsed
		count++
	}
	forecast.Records = make([]VOACAPHourlyForecast, 0, f.cfg.ForecastHours)
	for i := 0; i < f.cfg.ForecastHours; i++ {
		at := start.Add(time.Duration(i) * time.Hour)
		calendar := 0
		if count == 2 && at.Year() == calendars[1].month.Year() && at.Month() == calendars[1].month.Month() {
			calendar = 1
		}
		record, ok := monthlyForecastRecord(calendars[calendar].forecast.Records, at.Hour())
		if !ok {
			return VOACAPClosedForecast{}, fmt.Errorf("VOACAP output missing bidirectional %s prediction at %s", job.Request.Band, at.Format(time.RFC3339))
		}
		forecast.Records = append(forecast.Records, record)
	}
	forecast.OutputPath = strings.Join(paths[:count], ";")
	return forecast, nil
}

func monthlyForecastRecord(records []VOACAPHourlyForecast, hour int) (VOACAPHourlyForecast, bool) {
	for _, record := range records {
		if record.HourUTC == hour {
			return record, true
		}
	}
	return VOACAPHourlyForecast{}, false
}

func (f VOACAPRunnerClosedForecaster) forecastCalendar(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, string, time.Duration, error) {
	receive, receiveResult, err := f.runDirectionalForecast(ctx, job, voacapDeckReceive)
	if err != nil {
		return VOACAPClosedForecast{}, "", 0, err
	}
	// Parsed rows are small, bounded values. Do not keep the receive output
	// buffer alive while the next invocation allocates another bounded buffer.
	receiveResult.Output = nil
	transmit, transmitResult, err := f.runDirectionalForecast(ctx, job, voacapDeckTransmit)
	if err != nil {
		return VOACAPClosedForecast{}, "", 0, err
	}
	combined, err := combineDirectionalForecasts(receive, transmit, job.Request.Band)
	return combined, receiveResult.OutputPath + ";" + transmitResult.OutputPath, receiveResult.Elapsed + transmitResult.Elapsed, err
}

func (f VOACAPRunnerClosedForecaster) runDirectionalForecast(ctx context.Context, job VOACAPClosedJob, direction string) (VOACAPClosedForecast, voacap.RunResult, error) {
	deck, err := f.buildDeck(job, direction)
	if err != nil {
		return VOACAPClosedForecast{}, voacap.RunResult{}, err
	}
	outputName := fmt.Sprintf("%s_%s_%d_%d_%s_%d_%s.out",
		f.cfg.OutputNamePrefix,
		job.WindowStartUTC.Format("010215"),
		job.Request.UserCell,
		job.Request.DXCell,
		job.Request.Band,
		job.SSN,
		direction)
	result, err := f.runner.Run(ctx, voacap.RunRequest{
		Deck:                  deck,
		OutputName:            outputName,
		Timeout:               time.Duration(f.cfg.VOACAPTimeoutSeconds) * time.Second,
		RemoveOutputAfterRead: true,
	})
	if err != nil {
		return VOACAPClosedForecast{}, result, err
	}
	records, err := voacap.ParsePredictions(result.Output)
	if err != nil {
		return VOACAPClosedForecast{}, result, err
	}
	forecast, err := closedForecastFromRecords(records, job.Request.Band)
	if err != nil {
		return VOACAPClosedForecast{}, result, err
	}
	return forecast, result, nil
}

func (f VOACAPRunnerClosedForecaster) buildDeck(job VOACAPClosedJob, direction string) ([]byte, error) {
	userLat, userLon, ok := GridCenterLatLon(job.Request.UserGrid)
	if !ok {
		return nil, fmt.Errorf("invalid user grid %q", job.Request.UserGrid)
	}
	dxLat, dxLon, ok := GridCenterLatLon(job.Request.DXGrid)
	if !ok {
		return nil, fmt.Errorf("invalid DX grid %q", job.Request.DXGrid)
	}
	transmit := voacap.DeckEndpoint{
		Label:     "TRANSMITTER",
		Latitude:  userLat,
		Longitude: userLon,
	}
	receive := voacap.DeckEndpoint{
		Label:     "RECEIVER",
		Latitude:  dxLat,
		Longitude: dxLon,
	}
	switch direction {
	case voacapDeckReceive:
		transmit.Latitude = dxLat
		transmit.Longitude = dxLon
		receive.Latitude = userLat
		receive.Longitude = userLon
	case voacapDeckTransmit:
	default:
		return nil, fmt.Errorf("unknown VOACAP deck direction %q", direction)
	}
	method, _, err := voacap.RecommendedPathMethod(transmit, receive)
	if err != nil {
		return nil, err
	}
	return voacap.BuildPathDeck(voacap.PathDeckRequest{
		Comment:              fmt.Sprintf("GoCluster VOACAP closed fallback %s %d", job.Request.Band, job.SSN),
		Transmit:             transmit,
		Receive:              receive,
		SSN:                  job.SSN,
		Now:                  job.WindowStartUTC,
		ForecastHours:        24,
		StartVOACAPHour:      1,
		CenterFrequenciesMHz: f.cfg.CenterFrequenciesMHz,
		Method:               method,
	})
}

func combineDirectionalForecasts(receiveForecast, transmitForecast VOACAPClosedForecast, band string) (VOACAPClosedForecast, error) {
	transmitByHour := make(map[int]VOACAPHourlyForecast, len(transmitForecast.Records))
	for _, record := range transmitForecast.Records {
		transmitByHour[record.HourUTC] = record
	}
	combined := VOACAPClosedForecast{
		Records: make([]VOACAPHourlyForecast, 0, len(receiveForecast.Records)),
	}
	for _, receive := range receiveForecast.Records {
		transmit, ok := transmitByHour[receive.HourUTC]
		if !ok {
			continue
		}
		combined.Records = append(combined.Records, VOACAPHourlyForecast{
			FT8SNRDB:                        receive.FT8SNRDB,
			VOACAPSNRDBHz:                   receive.VOACAPSNRDBHz,
			HourUTC:                         receive.HourUTC,
			FrequencyMHz:                    receive.FrequencyMHz,
			ReqSNRReliability:               math.Min(receive.ReqSNRReliability, transmit.ReqSNRReliability),
			HasReqSNRReliability:            receive.HasReqSNRReliability && transmit.HasReqSNRReliability,
			ReceiveFT8SNRDB:                 receive.FT8SNRDB,
			TransmitFT8SNRDB:                transmit.FT8SNRDB,
			ReceiveVOACAPSNRDBHz:            receive.VOACAPSNRDBHz,
			TransmitVOACAPSNRDBHz:           transmit.VOACAPSNRDBHz,
			ReceiveReqSNRReliability:        receive.ReqSNRReliability,
			TransmitReqSNRReliability:       transmit.ReqSNRReliability,
			HasDirectionalReqSNRReliability: receive.HasReqSNRReliability && transmit.HasReqSNRReliability,
			HasDirectionalSNR:               true,
		})
	}
	if len(combined.Records) == 0 {
		return VOACAPClosedForecast{}, fmt.Errorf("VOACAP output has no common bidirectional prediction records for band %s", normalizeBand(band))
	}
	return combined, nil
}

func closedForecastFromRecords(records []voacap.PredictionRecord, band string) (VOACAPClosedForecast, error) {
	band = normalizeBand(band)
	byHour := make(map[int]VOACAPHourlyForecast)
	hourOrder := make([]int, 0)
	for _, record := range records {
		if bandForMHz(record.FrequencyMHz) != band {
			continue
		}
		hourly := VOACAPHourlyForecast{
			FT8SNRDB:             record.FT8SNRDB,
			VOACAPSNRDBHz:        record.VOACAPSNRDBHz,
			HourUTC:              record.HourUTC,
			FrequencyMHz:         record.FrequencyMHz,
			ReqSNRReliability:    record.Reliability,
			HasReqSNRReliability: record.HasReliability,
		}
		existing, ok := byHour[record.HourUTC]
		if !ok {
			hourOrder = append(hourOrder, record.HourUTC)
			byHour[record.HourUTC] = hourly
			continue
		}
		if record.FT8SNRDB > existing.FT8SNRDB {
			byHour[record.HourUTC] = hourly
		}
	}
	if len(hourOrder) == 0 {
		return VOACAPClosedForecast{}, fmt.Errorf("VOACAP output has no prediction records for band %s", band)
	}
	forecast := VOACAPClosedForecast{
		Records: make([]VOACAPHourlyForecast, 0, len(hourOrder)),
	}
	for _, hour := range hourOrder {
		forecast.Records = append(forecast.Records, byHour[hour])
	}
	return forecast, nil
}
