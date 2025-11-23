//go:build sample_spots
// +build sample_spots

package main
import (
  "database/sql"
  "encoding/json"
  "fmt"
  "log"
  _ "modernc.org/sqlite"
)
type Sample struct {
  ID        int     `json:"id"`
  Mode      string  `json:"mode"`
  DXCall    string  `json:"dx_call"`
  DECall    string  `json:"de_call"`
  Frequency float64 `json:"frequency"`
  DXMeta    string  `json:"dx_meta"`
}
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
    row := db.QueryRow(`SELECT id, dx_call, de_call, frequency, dx_continent, dx_cq_zone, dx_grid FROM spot_records WHERE mode=? ORDER BY id DESC LIMIT 1`, mode)
    var s Sample
    var dxContinent, dxGrid sql.NullString
    var dxCQ sql.NullInt64
    if err := row.Scan(&s.ID, &s.DXCall, &s.DECall, &s.Frequency, &dxContinent, &dxCQ, &dxGrid); err != nil { log.Fatal(err) }
    s.Mode = mode
    s.DXMeta = fmt.Sprintf("%s CQ %d %s", safeString(dxContinent), safeInt(dxCQ), safeString(dxGrid))
    b, _ := json.Marshal(s)
    fmt.Println(string(b))
  }
}
func safeString(ns sql.NullString) string {
  if ns.Valid {
    return ns.String
  }
  return ""
}
func safeInt(ni sql.NullInt64) int64 {
  if ni.Valid {
    return ni.Int64
  }
  return 0
}
