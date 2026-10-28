package bulletinhistory

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// FuzzSnapshotDecode tests the real bounded JSON admission without starting a
// writer or touching disk. Assertions describe the public storage limits,
// independently of the validation routine used by decodeSnapshot.
func FuzzSnapshotDecode(f *testing.F) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := snapshot{
		Version: 1, Sequence: 1, Watermark: now,
		WWV: []Record{{Time: now, Sequence: 1, Text: "WWV 🛰"}},
	}
	validBytes, err := json.Marshal(valid)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(validBytes)
	f.Add([]byte(`{"version":1,"wwv":`))
	f.Add([]byte(`{"version":1} {"version":1}`))
	f.Add([]byte(`{"version":0}`))
	f.Add([]byte{0xff, 0xfe, '{'})
	future := valid
	future.Watermark = now.Add(48 * time.Hour)
	futureBytes, err := json.Marshal(future)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(futureBytes)
	tooLong := valid
	tooLong.WWV = []Record{{Time: now, Sequence: 1, Text: strings.Repeat("🛰", 257)}}
	tooLongBytes, err := json.Marshal(tooLong)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(tooLongBytes)
	tooMany := valid
	tooMany.WWV = make([]Record, 49)
	for i := range tooMany.WWV {
		tooMany.WWV[i] = Record{Time: now, Sequence: 1, Text: "x"}
	}
	tooManyBytes, err := json.Marshal(tooMany)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(tooManyBytes)

	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := decodeSnapshot(data)
		if len(data) > 2<<20 {
			if err == nil {
				t.Fatal("accepted oversized snapshot")
			}
			return
		}
		if err != nil {
			return
		}
		if decoded.Version != 1 || len(decoded.WWV) > 48 || len(decoded.WCY) > 48 || len(decoded.Identities) > 4096 {
			t.Fatal("accepted unsupported version or unbounded collection")
		}
		futureLimit := time.Now().UTC().Add(time.Hour)
		if decoded.Watermark.After(futureLimit) || decoded.WWVLossAt.After(futureLimit) || decoded.WCYLossAt.After(futureLimit) ||
			(!decoded.WWVLossAt.IsZero() && decoded.WWVLossAt.After(decoded.Watermark)) ||
			(!decoded.WCYLossAt.IsZero() && decoded.WCYLossAt.After(decoded.Watermark)) {
			t.Fatal("accepted invalid watermark or loss timestamp")
		}
		for _, records := range [][]Record{decoded.WWV, decoded.WCY} {
			for i, record := range records {
				if record.Time.IsZero() || record.Time.After(futureLimit) || record.Time.After(decoded.Watermark) ||
					record.Sequence == 0 || record.Sequence > decoded.Sequence || !utf8.ValidString(record.Text) || utf8.RuneCountInString(record.Text) > 256 {
					t.Fatal("accepted invalid record")
				}
				if i > 0 {
					previous := records[i-1]
					if previous.Time.After(record.Time) || (previous.Time.Equal(record.Time) && previous.Sequence > record.Sequence) {
						t.Fatal("accepted out-of-order records")
					}
				}
			}
		}
		for _, identity := range decoded.Identities {
			if (identity.Kind != "WWV" && identity.Kind != "WCY") || identity.Time.IsZero() || identity.Time.After(futureLimit) || identity.Time.After(decoded.Watermark) {
				t.Fatal("accepted invalid deduplication identity")
			}
		}
	})
}
