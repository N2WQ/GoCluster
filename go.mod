module dxcluster

go 1.26

require gopkg.in/yaml.v3 v3.0.1

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/Kl1mn/h3-go v0.0.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240606204812-0bbfbd93a7ce // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.16.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.15.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	modernc.org/libc v1.61.13 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.8.2 // indirect
)

require (
	github.com/ThingsIXFoundation/h3-light v0.0.0-20230404055559-b4bb7d4d9c47
	github.com/agnivade/levenshtein v1.2.1
	github.com/cockroachdb/pebble v1.1.5
	github.com/dustin/go-humanize v1.0.1
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/gdamore/tcell/v2 v2.8.1
	github.com/json-iterator/go v1.1.12
	github.com/uber/h3-go/v4 v4.4.0
	github.com/zeebo/xxh3 v1.0.2
	github.com/ziutek/telnet v0.1.0
	golang.org/x/sys v0.38.0
	golang.org/x/term v0.35.0
	golang.org/x/time v0.14.0
	howett.net/plist v1.0.1
	modernc.org/sqlite v1.36.1
)

require (
	github.com/gdamore/encoding v1.0.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/rivo/tview v0.42.0
	github.com/rivo/uniseg v0.4.7 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/eclipse/paho.mqtt.golang => ./third_party/paho.mqtt.golang
