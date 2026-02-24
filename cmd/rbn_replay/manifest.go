package main

import "dxcluster/download"

type replayManifest struct {
	DateUTC string `json:"date_utc"`

	ConfigLoadedFrom string `json:"config_loaded_from"`
	ArchiveDir       string `json:"archive_dir"`
	OutputDir        string `json:"output_dir"`

	InputZipPath string            `json:"input_zip_path"`
	InputZipMeta download.Metadata `json:"input_zip_meta"`

	StartedAtUTC  string `json:"started_at_utc"`
	FinishedAtUTC string `json:"finished_at_utc"`
	GoVersion     string `json:"go_version"`

	CSV struct {
		Header             []string `json:"header"`
		RecordsTotal       int64    `json:"records_total"`
		RecordsProcessed   int64    `json:"records_processed"`
		RecordsSkippedMode int64    `json:"records_skipped_mode"`
		RecordsSkippedBad  int64    `json:"records_skipped_bad"`
	} `json:"csv"`

	Outputs struct {
		RunbookSamplesLog      string `json:"runbook_samples_log"`
		IntervalsCSV           string `json:"intervals_csv"`
		ThresholdHitsCSV       string `json:"threshold_hits_csv"`
		DisagreementsSampleCSV string `json:"disagreements_sample_csv"`
		GatesJSON              string `json:"gates_json"`
		ManifestJSON           string `json:"manifest_json"`
	} `json:"outputs"`

	Results struct {
		ComparableDecisions uint64  `json:"comparable_decisions"`
		AgreementPct        float64 `json:"agreement_pct"`
		DWPct               float64 `json:"dw_pct"`
		SPPct               float64 `json:"sp_pct"`
		UCPct               float64 `json:"uc_pct"`

		Drops struct {
			QueueFull     uint64 `json:"queue_full"`
			MaxKeys       uint64 `json:"max_keys"`
			MaxCandidates uint64 `json:"max_candidates"`
			MaxReporters  uint64 `json:"max_reporters"`
		} `json:"drops"`
	} `json:"results"`
}
