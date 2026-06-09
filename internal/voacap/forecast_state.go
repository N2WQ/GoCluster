package voacap

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type ForecastTransition string

const (
	ForecastTransitionFetchUnchanged ForecastTransition = "fetch_unchanged"
	ForecastTransitionStale          ForecastTransition = "stale_observation"
	ForecastTransitionFresh          ForecastTransition = "fresh_observation"
	ForecastTransitionForecastNeeded ForecastTransition = "forecast_needed"
	ForecastTransitionSuccess        ForecastTransition = "forecast_success"
	ForecastTransitionFailure        ForecastTransition = "forecast_failure"
)

type ForecastState struct {
	ETag                       string    `json:"etag,omitempty"`
	LastModified               string    `json:"last_modified,omitempty"`
	LastObservedAtUTC          time.Time `json:"last_observed_at_utc,omitempty"`
	LastRawSSN                 int       `json:"last_raw_ssn,omitempty"`
	EWMA                       float64   `json:"ewma,omitempty"`
	EWMAInitialized            bool      `json:"ewma_initialized,omitempty"`
	LastForecastSSN            float64   `json:"last_forecast_ssn,omitempty"`
	LastForecastSSNInitialized bool      `json:"last_forecast_ssn_initialized,omitempty"`
	LastForecastAtUTC          time.Time `json:"last_forecast_at_utc,omitempty"`
	LastForecastOutputPath     string    `json:"last_forecast_output_path,omitempty"`
	LastForecastOutputBytes    int       `json:"last_forecast_output_bytes,omitempty"`
	LastForecastError          string    `json:"last_forecast_error,omitempty"`
}

type ForecastDecision struct {
	Transition       ForecastTransition
	Observation      SunspotObservation
	EWMA             float64
	Delta            float64
	ForecastRequired bool
	Reason           string
}

func ApplyFetchUnchanged(st *ForecastState, cfg ExperimentConfig) (ForecastDecision, error) {
	if st == nil {
		return ForecastDecision{}, fmt.Errorf("forecast state is required")
	}
	if !st.EWMAInitialized {
		return ForecastDecision{
			Transition: ForecastTransitionFetchUnchanged,
			EWMA:       st.EWMA,
			Reason:     "HTTP 304 or unchanged source payload without initialized EWMA",
		}, nil
	}
	decision, err := forecastDecisionFromCurrentEWMA(st, cfg, ForecastTransitionFetchUnchanged, "HTTP 304 or unchanged source payload")
	if err != nil {
		return ForecastDecision{}, err
	}
	return decision, nil
}

func ApplySunspotObservation(st *ForecastState, observation SunspotObservation, cfg ExperimentConfig) (ForecastDecision, error) {
	if st == nil {
		return ForecastDecision{}, fmt.Errorf("forecast state is required")
	}
	if err := cfg.Validate(); err != nil {
		return ForecastDecision{}, err
	}
	if st.EWMAInitialized && !observation.ObservedAtUTC.After(st.LastObservedAtUTC) {
		decision, err := forecastDecisionFromCurrentEWMA(st, cfg, ForecastTransitionStale, "observation time is not newer than state")
		if err != nil {
			return ForecastDecision{}, err
		}
		decision.Observation = observation
		return decision, nil
	}

	ewma, err := UpdateSunspotEWMA(st.EWMA, st.LastObservedAtUTC, st.EWMAInitialized, observation, cfg.EWMAHalfLife())
	if err != nil {
		return ForecastDecision{}, err
	}
	trigger, delta, err := ShouldRecomputeVOACAP(ewma.Average, st.LastForecastSSN, st.LastForecastSSNInitialized, cfg.RecomputeThreshold())
	if err != nil {
		return ForecastDecision{}, err
	}

	st.LastObservedAtUTC = observation.ObservedAtUTC
	st.LastRawSSN = observation.RawWolfEstimate
	st.EWMA = ewma.Average
	st.EWMAInitialized = ewma.Initialized

	transition := ForecastTransitionFresh
	reason := "fresh observation below recompute threshold"
	if trigger {
		transition = ForecastTransitionForecastNeeded
		reason = "initial forecast required"
		if st.LastForecastSSNInitialized {
			reason = "EWMA delta reached recompute threshold"
		}
	}
	return ForecastDecision{
		Transition:       transition,
		Observation:      observation,
		EWMA:             ewma.Average,
		Delta:            delta,
		ForecastRequired: trigger,
		Reason:           reason,
	}, nil
}

func forecastDecisionFromCurrentEWMA(st *ForecastState, cfg ExperimentConfig, noForecastTransition ForecastTransition, noForecastReason string) (ForecastDecision, error) {
	if err := cfg.Validate(); err != nil {
		return ForecastDecision{}, err
	}
	trigger, delta, err := ShouldRecomputeVOACAP(st.EWMA, st.LastForecastSSN, st.LastForecastSSNInitialized, cfg.RecomputeThreshold())
	if err != nil {
		return ForecastDecision{}, err
	}
	if !trigger {
		return ForecastDecision{
			Transition: noForecastTransition,
			EWMA:       st.EWMA,
			Delta:      delta,
			Reason:     noForecastReason,
		}, nil
	}
	reason := "initial forecast required"
	if st.LastForecastSSNInitialized {
		reason = "EWMA delta reached recompute threshold"
	}
	return ForecastDecision{
		Transition:       ForecastTransitionForecastNeeded,
		EWMA:             st.EWMA,
		Delta:            delta,
		ForecastRequired: true,
		Reason:           reason,
	}, nil
}

func MarkForecastSuccess(st *ForecastState, smoothedSSN float64, outputPath string, outputBytes int, now time.Time) (ForecastDecision, error) {
	if st == nil {
		return ForecastDecision{}, fmt.Errorf("forecast state is required")
	}
	if math.IsNaN(smoothedSSN) || math.IsInf(smoothedSSN, 0) || smoothedSSN < 0 {
		return ForecastDecision{}, fmt.Errorf("forecast SSN must be finite and >= 0")
	}
	st.LastForecastSSN = round2(smoothedSSN)
	st.LastForecastSSNInitialized = true
	st.LastForecastAtUTC = now.UTC()
	st.LastForecastOutputPath = strings.TrimSpace(outputPath)
	st.LastForecastOutputBytes = outputBytes
	st.LastForecastError = ""
	return ForecastDecision{
		Transition: ForecastTransitionSuccess,
		EWMA:       st.EWMA,
		Reason:     "forecast completed",
	}, nil
}

func MarkForecastFailure(st *ForecastState, err error, now time.Time) ForecastDecision {
	if st == nil {
		return ForecastDecision{
			Transition: ForecastTransitionFailure,
			Reason:     "forecast state is missing",
		}
	}
	st.LastForecastAtUTC = now.UTC()
	if err == nil {
		st.LastForecastError = "forecast failed"
	} else {
		st.LastForecastError = err.Error()
	}
	return ForecastDecision{
		Transition: ForecastTransitionFailure,
		EWMA:       st.EWMA,
		Reason:     st.LastForecastError,
	}
}
