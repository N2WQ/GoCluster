package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	spotpkg "dxcluster/spot"
	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"
)

type Config struct {
	DataDir         string          `yaml:"data_dir"`
	DBTemplate      string          `yaml:"db_template"`
	BustedTemplate  string          `yaml:"busted_template"`
	CSVTemplate     string          `yaml:"csv_template"`
	ReportTemplate  string          `yaml:"report_template"`
	RBNZipTemplate  string          `yaml:"rbn_zip_template"`
	DownloadRBN     bool            `yaml:"download_rbn"`
	CreateReportDir bool            `yaml:"create_report_dir"`
	ContestDates    string          `yaml:"contest_dates_file"`
	Stability       StabilityConfig `yaml:"stability"`
	OpenAI          OpenAIConfig    `yaml:"openai"`
}

type StabilityConfig struct {
	BucketMinutes   int     `yaml:"bucket_minutes"`
	WindowMinutes   int     `yaml:"window_minutes"`
	MinFollowOn     int     `yaml:"min_follow_on"`
	FreqToleranceHz float64 `yaml:"freq_tolerance_hz"`
}

type OpenAIConfig struct {
	Enabled               bool    `yaml:"enabled"`
	APIKey                string  `yaml:"api_key"`
	Model                 string  `yaml:"model"`
	Endpoint              string  `yaml:"endpoint"`
	MaxTokens             int     `yaml:"max_tokens"` // sent as max_completion_tokens in payload
	Temperature           float64 `yaml:"temperature"`
	SystemPrompt          string  `yaml:"system_prompt"`
	OutputTemplate        string  `yaml:"output_template"`
	IncludeConfigSnapshot bool    `yaml:"include_config_snapshot"`
	ClusterConfigPath     string  `yaml:"cluster_config_path"`
}

type bustedRow struct {
	Bad    string
	Corr   string
	Freq   float64
	Band   string
	Status string
}

type bandStat struct {
	Total       int
	Matched     int
	Absent      int
	PresentMiss int
}

type reasonRow struct {
	Reason string
	Count  int64
}

type spot struct {
	Ts   int64
	Freq float64
}

type stabilityBucket struct {
	Start  int64
	Total  int
	Stable int
}

type openAIRequest struct {
	Model               string            `json:"model"`
	MaxCompletionTokens int               `json:"max_completion_tokens,omitempty"`
	Temperature         float64           `json:"temperature,omitempty"`
	Messages            []openAIMessage   `json:"messages"`
	ResponseFormat      map[string]string `json:"response_format,omitempty"`
	ReasoningEffort     string            `json:"reasoning_effort,omitempty"`
	Verbosity           string            `json:"verbosity,omitempty"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func defaultConfig() Config {
	return Config{
		DataDir:         ".",
		DBTemplate:      "data/logs/callcorr_debug_modified_{DATE}.db",
		BustedTemplate:  "data/logs/Busted-{DATE_BUSTED}.txt",
		CSVTemplate:     "data/logs/{DATE_COMPACT}.csv",
		RBNZipTemplate:  "https://data.reversebeacon.net/rbn_history/{DATE_COMPACT}.zip",
		ReportTemplate:  "data/reports/analysis-{DATE}.txt",
		DownloadRBN:     true,
		CreateReportDir: true,
		ContestDates:    "",
		Stability: StabilityConfig{
			BucketMinutes:   60,
			WindowMinutes:   60,
			MinFollowOn:     2,
			FreqToleranceHz: 1000,
		},
		OpenAI: OpenAIConfig{
			Enabled:               false,
			Model:                 "gpt-5-nano",
			Endpoint:              "https://api.openai.com/v1/chat/completions",
			MaxTokens:             5000,
			Temperature:           1,
			SystemPrompt:          "",
			OutputTemplate:        "data/reports/analysis-{DATE}.llm.txt",
			IncludeConfigSnapshot: true,
			ClusterConfigPath:     "data/config",
		},
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func readConfig(path string) Config {
	cfg := defaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg
		}
		log.Fatalf("failed to read config: %v", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}
	return cfg
}

func expandTemplate(tmpl string, dt time.Time) string {
	repl := map[string]string{
		"{DATE}":         dt.Format("2006-01-02"),
		"{DATE_COMPACT}": dt.Format("20060102"),
		"{DATE_BUSTED}":  dt.Format("02-Jan-2006"),
	}
	out := tmpl
	for k, v := range repl {
		out = strings.ReplaceAll(out, k, v)
	}
	return out
}

func bandFor(freq float64) string {
	switch {
	case freq < 1800:
		return "LF"
	case freq < 2000:
		return "160m"
	case freq < 4000:
		return "80m"
	case freq < 8000:
		return "40m"
	case freq < 11000:
		return "30m"
	case freq < 15000:
		return "20m"
	case freq < 19000:
		return "17m"
	case freq < 22000:
		return "15m"
	case freq < 25000:
		return "12m"
	case freq < 30000:
		return "10m"
	default:
		return "6m+"
	}
}

func parseBusted(path string, year int, minTs, maxTs int64) (int, []bustedRow, map[string]struct{}) {
	file, err := os.Open(path)
	must(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	totalLines := 0
	rows := make([]bustedRow, 0, 4096)
	unique := make(map[string]struct{})

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		totalLines++
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 6 {
			continue
		}

		freq, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			continue
		}

		corrStart := 4
		corrEnd := len(parts) - 2
		if corrEnd <= corrStart {
			continue
		}
		snrIdx := len(parts) - 3
		if snrIdx >= corrStart {
			if _, err := strconv.Atoi(parts[snrIdx]); err == nil {
				corrEnd = snrIdx
			}
		}
		if corrEnd <= corrStart {
			continue
		}
		corr := strings.TrimSpace(strings.Join(parts[corrStart:corrEnd], " "))
		if corr == "" {
			continue
		}

		tsStr := fmt.Sprintf("%s-%d %s", parts[0], year, parts[1])
		ts, err := time.Parse("02-Jan-2006 1504Z", tsStr)
		if err != nil {
			continue
		}
		epoch := ts.Unix()
		if epoch < minTs || epoch > maxTs {
			continue
		}

		row := bustedRow{
			Bad:  strings.ToUpper(parts[2]),
			Corr: strings.ToUpper(corr),
			Freq: freq,
			Band: bandFor(freq),
		}
		key := row.Bad + "|" + row.Corr
		unique[key] = struct{}{}
		rows = append(rows, row)
	}
	must(scanner.Err())
	return totalLines, rows, unique
}

func percent(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) * 100 / float64(total)
}

func label(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

func ensureDir(path string) {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return
	}
	_ = os.MkdirAll(dir, 0o755)
}

func readFileIfExists(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func downloadFile(url, dest string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: status %s", url, resp.Status)
	}

	ensureDir(dest)
	tmp := dest + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dest)
}

func unzip(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		destPath := filepath.Join(destDir, f.Name)
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(destPath, f.Mode()); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			rc.Close()
			return err
		}
		if _, err := io.Copy(out, rc); err != nil {
			rc.Close()
			out.Close()
			return err
		}
		rc.Close()
		out.Close()
	}
	return nil
}

func loadContestDates(path string) map[string]struct{} {
	out := make(map[string]struct{})
	if path == "" {
		return out
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, ln := range strings.Split(string(data), "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" || strings.HasPrefix(ln, "#") {
			continue
		}
		out[ln] = struct{}{}
	}
	return out
}

func loadRBN(csvPath string) (map[string][]spot, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	spots := make(map[string][]spot)
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	isHeader := true
	for scanner.Scan() {
		line := scanner.Text()
		if isHeader {
			isHeader = false
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 11 {
			continue
		}
		freqKhz, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			continue
		}
		dx := strings.ToUpper(strings.TrimSpace(parts[5]))
		if dx == "" {
			continue
		}
		ts, err := time.Parse("2006-01-02 15:04:05", parts[10])
		if err != nil {
			continue
		}
		spots[dx] = append(spots[dx], spot{Ts: ts.Unix(), Freq: freqKhz})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	for k := range spots {
		sort.Slice(spots[k], func(i, j int) bool { return spots[k][i].Ts < spots[k][j].Ts })
	}
	return spots, nil
}

type correction struct {
	Ts     int64
	Winner string
	Freq   float64
	Band   string
}

func binarySearchSpots(sp []spot, target int64) int {
	lo, hi := 0, len(sp)
	for lo < hi {
		mid := (lo + hi) / 2
		if sp[mid].Ts < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

type bandStability struct {
	Total  int
	Stable int
}

func computeStability(db *sql.DB, csvPath string, minTs int64, cfg StabilityConfig) (int, int, []stabilityBucket, map[string]bandStability, error) {
	rbnSpots, err := loadRBN(csvPath)
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("load RBN CSV: %w", err)
	}

	corrections := make([]correction, 0, 8192)
	rows, err := db.Query("select ts, upper(trim(winner)), freq_khz from decisions where decision='applied'")
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("query applied decisions: %w", err)
	}
	for rows.Next() {
		var ts int64
		var w string
		var f float64
		if err := rows.Scan(&ts, &w, &f); err != nil {
			rows.Close()
			return 0, 0, nil, nil, fmt.Errorf("scan applied decision row: %w", err)
		}
		b := spotpkg.FreqToBand(f)
		if b == "" {
			b = "unknown"
		}
		corrections = append(corrections, correction{Ts: ts, Winner: w, Freq: f, Band: b})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, nil, nil, fmt.Errorf("iterate applied decisions: %w", err)
	}
	rows.Close()

	if cfg.BucketMinutes <= 0 {
		cfg.BucketMinutes = 60
	}
	if cfg.WindowMinutes <= 0 {
		cfg.WindowMinutes = 60
	}
	if cfg.MinFollowOn <= 0 {
		cfg.MinFollowOn = 2
	}
	if cfg.FreqToleranceHz <= 0 {
		cfg.FreqToleranceHz = 1000
	}

	tolKhz := cfg.FreqToleranceHz / 1000.0
	horizon := int64(cfg.WindowMinutes * 60)
	bucketSize := int64(cfg.BucketMinutes * 60)

	bucketMap := make(map[int64]*stabilityBucket)
	bandMap := make(map[string]bandStability)
	stableCount := 0

	for _, corr := range corrections {
		list := rbnSpots[corr.Winner]
		totalHorizon := 0
		if len(list) > 0 {
			startIdx := binarySearchSpots(list, corr.Ts)
			for i := startIdx; i < len(list); i++ {
				if list[i].Ts > corr.Ts+horizon {
					break
				}
				if math.Abs(list[i].Freq-corr.Freq) <= tolKhz {
					totalHorizon++
					if totalHorizon >= cfg.MinFollowOn {
						break
					}
				}
			}
		}

		if totalHorizon >= cfg.MinFollowOn {
			stableCount++
		}
		// Track band-level counts.
		bs := bandMap[corr.Band]
		bs.Total++
		if totalHorizon >= cfg.MinFollowOn {
			bs.Stable++
		}
		bandMap[corr.Band] = bs

		bucketStart := minTs + ((corr.Ts - minTs) / bucketSize * bucketSize)
		b := bucketMap[bucketStart]
		if b == nil {
			b = &stabilityBucket{Start: bucketStart}
			bucketMap[bucketStart] = b
		}
		b.Total++
		if totalHorizon >= cfg.MinFollowOn {
			b.Stable++
		}
	}

	buckets := make([]stabilityBucket, 0, len(bucketMap))
	for _, v := range bucketMap {
		buckets = append(buckets, *v)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Start < buckets[j].Start })

	return stableCount, len(corrections), buckets, bandMap, nil
}

// generateLLM requests recommendations from OpenAI using the assembled report and optional config snapshot.
// It respects OPENAI_API_KEY when api_key is blank in the config. When enabled but no key is found, it errors.
func generateLLM(cfg Config, analysisDate time.Time, report []string) (string, error) {
	key := strings.TrimSpace(cfg.OpenAI.APIKey)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	}
	if key == "" {
		return "", fmt.Errorf("OPENAI_API_KEY missing; set openai.api_key or environment variable")
	}
	model := strings.TrimSpace(cfg.OpenAI.Model)
	if model == "" {
		model = "gpt-5-nano"
	}
	endpoint := strings.TrimSpace(cfg.OpenAI.Endpoint)
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1/chat/completions"
	}

	userContent := strings.Join(report, "\n")
	if cfg.OpenAI.IncludeConfigSnapshot {
		cfgPath := cfg.OpenAI.ClusterConfigPath
		if cfgPath == "" {
			cfgPath = "data/config"
		}
		cfgPath = filepath.Clean(filepath.Join(cfg.DataDir, cfgPath))
		if snapshot := readFileIfExists(cfgPath); snapshot != "" {
			userContent += "\n\nCluster config snapshot:\n" + snapshot
		}
	}

	systemPrompt := strings.TrimSpace(cfg.OpenAI.SystemPrompt)
	if systemPrompt == "" {
		systemPrompt = "You are an RF/cluster QA analyst. Read the daily call-correction metrics and propose specific parameter adjustments."
	}

	reqBody := openAIRequest{
		Model:               model,
		MaxCompletionTokens: cfg.OpenAI.MaxTokens,
		Messages: []openAIMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userContent},
		},
	}
	// GPT-5 series: use text response, minimal reasoning effort, low verbosity; skip temperature.
	if strings.HasPrefix(model, "gpt-5") {
		reqBody.ResponseFormat = map[string]string{"type": "text"}
		reqBody.ReasoningEffort = "minimal"
		reqBody.Verbosity = "low"
	} else if cfg.OpenAI.Temperature > 0 {
		reqBody.Temperature = cfg.OpenAI.Temperature
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal OpenAI request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("build OpenAI request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("call OpenAI: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read OpenAI response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("OpenAI HTTP %s: %s", resp.Status, string(body))
	}

	var oaResp openAIResponse
	if err := json.Unmarshal(body, &oaResp); err != nil {
		return "", fmt.Errorf("parse OpenAI response: %w", err)
	}
	if oaResp.Error != nil {
		return "", fmt.Errorf("OpenAI error: %s", oaResp.Error.Message)
	}
	if len(oaResp.Choices) == 0 {
		return "", fmt.Errorf("OpenAI response had no choices")
	}
	return strings.TrimSpace(oaResp.Choices[0].Message.Content), nil
}

// loadDotEnv loads KEY=VALUE pairs from a .env-style file into the process environment.
// Lines starting with # are ignored; only the first '=' is treated as a separator.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Allow optional "export KEY=VAL" style.
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(line[len("export "):])
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key == "" {
			continue
		}
		_ = os.Setenv(key, val)
	}
}

func main() {
	configPath := flag.String("config", "cmd/daily_analysis/daily_analysis.yaml", "Path to analysis config YAML")
	dateFlag := flag.String("date", "", "Analysis date YYYY-MM-DD (defaults to yesterday)")
	skipDownload := flag.Bool("skip-download", false, "Skip downloading RBN zip")
	flag.Parse()

	// Load .env first so OPENAI_API_KEY (and other secrets) are available before reading config.
	loadDotEnv(".env")
	if configPath != nil && *configPath != "" {
		loadDotEnv(filepath.Join(filepath.Dir(*configPath), ".env"))
	}

	cfg := readConfig(*configPath)

	analysisDate := time.Now().AddDate(0, 0, -1)
	if *dateFlag != "" {
		d, err := time.Parse("2006-01-02", *dateFlag)
		must(err)
		analysisDate = d
	}

	expand := func(t string) string { return expandTemplate(t, analysisDate) }
	dbPath := filepath.Clean(filepath.Join(cfg.DataDir, expand(cfg.DBTemplate)))
	bustedPath := filepath.Clean(filepath.Join(cfg.DataDir, expand(cfg.BustedTemplate)))
	csvPath := filepath.Clean(filepath.Join(cfg.DataDir, expand(cfg.CSVTemplate)))
	reportPath := filepath.Clean(filepath.Join(cfg.DataDir, expand(cfg.ReportTemplate)))
	rbnZip := expand(cfg.RBNZipTemplate)
	if !strings.Contains(rbnZip, "://") {
		rbnZip = filepath.Clean(filepath.Join(cfg.DataDir, rbnZip))
	}

	if _, err := os.Stat(dbPath); err != nil {
		log.Fatalf("database not found: %s (%v)", dbPath, err)
	}
	if _, err := os.Stat(bustedPath); err != nil {
		log.Fatalf("reference busted log not found: %s (%v)", bustedPath, err)
	}

	if cfg.DownloadRBN && !*skipDownload {
		if _, err := os.Stat(rbnZip); err != nil {
			if strings.HasPrefix(rbnZip, "http://") || strings.HasPrefix(rbnZip, "https://") {
				log.Printf("Downloading RBN archive: %s", rbnZip)
				dest := filepath.Clean(filepath.Join(cfg.DataDir, expand(cfg.CSVTemplate)))
				destDir := filepath.Dir(dest)
				localZip := filepath.Join(destDir, filepath.Base(rbnZip))
				must(downloadFile(rbnZip, localZip))
				rbnZip = localZip
			} else {
				log.Fatalf("RBN zip not found: %s", rbnZip)
			}
		}
	}

	if _, err := os.Stat(csvPath); err != nil {
		if strings.HasSuffix(strings.ToLower(rbnZip), ".zip") {
			log.Printf("Extracting RBN archive %s", rbnZip)
			must(unzip(rbnZip, filepath.Dir(csvPath)))
		}
	}

	contestDates := loadContestDates(cfg.ContestDates)
	isContest := false
	if cfg.ContestDates != "" {
		if _, ok := contestDates[analysisDate.Format("2006-01-02")]; ok {
			isContest = true
		}
	}

	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	must(err)
	defer db.Close()

	var minTs, maxTs, decisionCount int64
	must(db.QueryRow("select min(ts), max(ts), count(*) from decisions").Scan(&minTs, &maxTs, &decisionCount))

	var appliedCount, uniqueApplied int64
	must(db.QueryRow("select count(*) from decisions where decision='applied'").Scan(&appliedCount))
	must(db.QueryRow("select count(*) from (select distinct subject, winner from decisions where decision='applied')").Scan(&uniqueApplied))

	appliedSet := make(map[string]struct{})
	rows, err := db.Query("select distinct upper(trim(subject)) || '|' || upper(trim(winner)) from decisions where decision='applied'")
	must(err)
	for rows.Next() {
		var key string
		must(rows.Scan(&key))
		appliedSet[key] = struct{}{}
	}
	must(rows.Err())
	rows.Close()

	subjectSet := make(map[string]struct{})
	subjRows, err := db.Query("select distinct upper(trim(subject)) from decisions")
	must(err)
	for subjRows.Next() {
		var s string
		must(subjRows.Scan(&s))
		subjectSet[s] = struct{}{}
	}
	must(subjRows.Err())
	subjRows.Close()

	totalBustedLines, bustedRows, bustedUnique := parseBusted(bustedPath, analysisDate.Year(), minTs, maxTs)

	matchedPairs := 0
	for key := range bustedUnique {
		if _, ok := appliedSet[key]; ok {
			matchedPairs++
		}
	}

	bandStats := make(map[string]*bandStat)
	matchedRowsCount := 0
	absentRows := 0
	presentMissRows := 0

	for i := range bustedRows {
		row := &bustedRows[i]
		bs := bandStats[row.Band]
		if bs == nil {
			bs = &bandStat{}
			bandStats[row.Band] = bs
		}
		bs.Total++

		key := row.Bad + "|" + row.Corr
		if _, ok := appliedSet[key]; ok {
			row.Status = "matched"
			bs.Matched++
			matchedRowsCount++
		} else if _, ok := subjectSet[row.Bad]; !ok {
			row.Status = "absent"
			bs.Absent++
			absentRows++
		} else {
			row.Status = "present_miss"
			bs.PresentMiss++
			presentMissRows++
		}
	}

	rejectReasons := make([]reasonRow, 0)
	rejRows, err := db.Query("select coalesce(reason,''), count(*) from decisions where decision='rejected' group by coalesce(reason,'')")
	must(err)
	for rejRows.Next() {
		var r string
		var c int64
		must(rejRows.Scan(&r, &c))
		rejectReasons = append(rejectReasons, reasonRow{Reason: r, Count: c})
	}
	must(rejRows.Err())
	rejRows.Close()
	sort.Slice(rejectReasons, func(i, j int) bool { return rejectReasons[i].Count > rejectReasons[j].Count })

	d3Reasons := make([]reasonRow, 0)
	d3Rows, err := db.Query("select coalesce(reason,''), count(*) from decisions where decision='rejected' and distance=3 group by coalesce(reason,'')")
	must(err)
	for d3Rows.Next() {
		var r string
		var c int64
		must(d3Rows.Scan(&r, &c))
		d3Reasons = append(d3Reasons, reasonRow{Reason: r, Count: c})
	}
	must(d3Rows.Err())
	d3Rows.Close()
	sort.Slice(d3Reasons, func(i, j int) bool { return d3Reasons[i].Count > d3Reasons[j].Count })

	var totalConfRejects, nearConfD12, nearConfD3 int64
	must(db.QueryRow("select count(*) from decisions where decision='rejected' and reason='confidence'").Scan(&totalConfRejects))
	must(db.QueryRow("select count(*) from decisions where decision='rejected' and reason='confidence' and distance<=2 and winner_confidence between 55 and 59").Scan(&nearConfD12))
	must(db.QueryRow("select count(*) from decisions where decision='rejected' and reason='confidence' and distance=3 and winner_confidence between 60 and 64").Scan(&nearConfD3))

	report := make([]string, 0, 128)
	report = append(report, fmt.Sprintf("Daily Call Correction Analysis - %s", analysisDate.Format("2006-01-02")))
	report = append(report, fmt.Sprintf("Database: %s", dbPath))
	report = append(report, fmt.Sprintf("Reference: %s", bustedPath))
	report = append(report, fmt.Sprintf("RBN CSV:   %s", csvPath))
	report = append(report, "")
	report = append(report, fmt.Sprintf("Coverage window: %s to %s UTC (decisions: %d)",
		time.Unix(minTs, 0).UTC().Format(time.RFC3339),
		time.Unix(maxTs, 0).UTC().Format(time.RFC3339),
		decisionCount,
	))
	report = append(report, fmt.Sprintf("Applied corrections: %d (unique pairs: %d)", appliedCount, uniqueApplied))
	report = append(report, fmt.Sprintf("Reference lines (all): %d", totalBustedLines))
	report = append(report, fmt.Sprintf("Reference pairs in DB window: %d", len(bustedUnique)))
	report = append(report, fmt.Sprintf("Matched pairs: %d", matchedPairs))

	pairRecall := 0.0
	if len(bustedUnique) > 0 {
		pairRecall = float64(matchedPairs) * 100 / float64(len(bustedUnique))
	}
	report = append(report, fmt.Sprintf("Pair-level recall: %.1f%%", pairRecall))
	if cfg.ContestDates != "" {
		if isContest {
			report = append(report, "Profile: contest day (contest_dates_file)")
		} else {
			report = append(report, "Profile: non-contest day (contest_dates_file)")
		}
	}
	report = append(report, "")

	report = append(report, "Band recall (reference rows within DB window):")
	report = append(report, "Band  Total Matched Recall Absent PresentMiss")
	bands := make([]string, 0, len(bandStats))
	for b := range bandStats {
		bands = append(bands, b)
	}
	sort.Strings(bands)
	for _, band := range bands {
		bs := bandStats[band]
		rec := 0.0
		if bs.Total > 0 {
			rec = float64(bs.Matched) * 100 / float64(bs.Total)
		}
		report = append(report, fmt.Sprintf("%-4s %5d %7d %6.1f%% %6d %11d",
			band, bs.Total, bs.Matched, rec, bs.Absent, bs.PresentMiss))
	}
	report = append(report, "")

	totalRef := len(bustedRows)
	report = append(report, "Miss classification (rows within window):")
	report = append(report, fmt.Sprintf("  Total reference rows: %d", totalRef))
	report = append(report, fmt.Sprintf("  Matched: %d (%.1f%%)", matchedRowsCount, percent(matchedRowsCount, totalRef)))
	report = append(report, fmt.Sprintf("  Absent subjects: %d (%.1f%%)", absentRows, percent(absentRows, totalRef)))
	report = append(report, fmt.Sprintf("  Present but missed: %d (%.1f%%)", presentMissRows, percent(presentMissRows, totalRef)))
	report = append(report, "")

	report = append(report, "Reject reasons:")
	for _, r := range rejectReasons {
		report = append(report, fmt.Sprintf("  %-15s %7d", label(r.Reason), r.Count))
	}
	report = append(report, "")

	report = append(report, "Distance=3 reject reasons:")
	for _, r := range d3Reasons {
		report = append(report, fmt.Sprintf("  %-15s %7d", label(r.Reason), r.Count))
	}
	report = append(report, "")

	report = append(report, "Confidence gating headroom:")
	report = append(report, fmt.Sprintf("  Total confidence rejects: %d", totalConfRejects))
	report = append(report, fmt.Sprintf("  Dist<=2 with 55-59%% confidence: %d", nearConfD12))
	report = append(report, fmt.Sprintf("  Dist=3 with 60-64%% confidence: %d", nearConfD3))

	// Temporal stability using RBN CSV
	if _, err := os.Stat(csvPath); err == nil {
		stableCount, totalStab, buckets, bandStab, stabErr := computeStability(db, csvPath, minTs, cfg.Stability)
		if stabErr != nil {
			log.Fatalf("stability computation failed: %v", stabErr)
		}
		report = append(report, "")
		report = append(report, fmt.Sprintf("Temporal stability (window %d min, min follow-on %d, freq tol %.0f Hz):",
			cfg.Stability.WindowMinutes, cfg.Stability.MinFollowOn, cfg.Stability.FreqToleranceHz))
		if totalStab > 0 {
			report = append(report, fmt.Sprintf("  Overall: %d/%d (%.1f%%) corrections showed follow-on winner spots",
				stableCount, totalStab, percent(stableCount, totalStab)))
			report = append(report, "  Band stability:")
			bandKeys := make([]string, 0, len(bandStab))
			for k := range bandStab {
				bandKeys = append(bandKeys, k)
			}
			sort.Strings(bandKeys)
			for _, b := range bandKeys {
				bs := bandStab[b]
				report = append(report, fmt.Sprintf("    %-4s %5d %5d %5.1f%%", b, bs.Total, bs.Stable, percent(bs.Stable, bs.Total)))
			}
			report = append(report, "  Rolling stability by bucket:")
			report = append(report, "  BucketStartUTC          Total  Stable  Rate")
			for _, b := range buckets {
				report = append(report, fmt.Sprintf("  %s %6d %7d %5.1f%%",
					time.Unix(b.Start, 0).UTC().Format("2006-01-02 15:04"),
					b.Total, b.Stable, percent(b.Stable, b.Total)))
			}
		} else {
			report = append(report, "  No applied corrections to evaluate for stability.")
		}
	} else {
		report = append(report, "")
		report = append(report, "Temporal stability: skipped (RBN CSV not found).")
	}

	// Optional LLM recommendations.
	if cfg.OpenAI.Enabled {
		llmContent, err := generateLLM(cfg, analysisDate, report)
		if err != nil {
			log.Printf("OpenAI request failed: %v (continuing without LLM recommendations)", err)
		}
		if llmContent != "" {
			report = append(report, "")
			report = append(report, "LLM Recommendations:")
			report = append(report, llmContent)
		}
	}

	if cfg.CreateReportDir {
		ensureDir(reportPath)
	}
	f, err := os.Create(reportPath)
	must(err)
	defer f.Close()

	writer := bufio.NewWriter(f)
	for _, line := range report {
		_, err := writer.WriteString(line + "\n")
		must(err)
	}
	must(writer.Flush())

	for _, line := range report {
		fmt.Println(line)
	}
}
