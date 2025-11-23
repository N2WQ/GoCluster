//go:build sample_telnet
// +build sample_telnet

package main
import (
  "database/sql"
  "fmt"
  "log"
  "time"
  _ "modernc.org/sqlite"
  "dxcluster/spot"
)
func main() {
  db, err := sql.Open("sqlite", "data/records/spots.db")
  if err != nil { log.Fatal(err) }
  defer db.Close()

  modes, err := db.Query("SELECT DISTINCT mode FROM spot_records ORDER BY mode")
  if err != nil { log.Fatal(err) }
  defer modes.Close()

  for modes.Next() {
    var mode string
    if err := modes.Scan(&mode); err != nil { log.Fatal(err) }
    row := db.QueryRow(`SELECT dx_call, de_call, frequency, band, report, observed_at, comment, source_type, source_node, is_beacon,
      dx_continent, dx_country, dx_cq_zone, dx_itu_zone, dx_grid,
      de_continent, de_country, de_cq_zone, de_itu_zone, de_grid FROM spot_records WHERE mode=? ORDER BY id DESC LIMIT 1`, mode)

    s := &spot.Spot{}
    var observed int64
    var sourceType string
    var isBeacon int
    var dxMeta, deMeta spot.CallMetadata

    err := row.Scan(
      &s.DXCall, &s.DECall, &s.Frequency, &s.Band, &s.Report, &observed, &s.Comment,
      &sourceType, &s.SourceNode, &isBeacon,
      &dxMeta.Continent, &dxMeta.Country, &dxMeta.CQZone, &dxMeta.ITUZone, &dxMeta.Grid,
      &deMeta.Continent, &deMeta.Country, &deMeta.CQZone, &deMeta.ITUZone, &deMeta.Grid,
    )
    if err != nil { log.Fatal(err) }

    s.Mode = mode
    s.Time = time.Unix(observed, 0).UTC()
    s.SourceType = spot.SourceType(sourceType)
    s.IsBeacon = isBeacon != 0
    s.DXMetadata = dxMeta
    s.DEMetadata = deMeta

    fmt.Println(s.FormatDXCluster())
  }
}
