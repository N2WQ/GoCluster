package peer

import (
	"fmt"

	"dxcluster/spot"
)

// Keys for dedup caches.
func pc92Key(f *Frame) string {
	return fmt.Sprintf("pc92:%s:%s", f.Type, f.Raw)
}

func dxKey(f *Frame, s *spot.Spot) string {
	return fmt.Sprintf("dx:%s:%s:%s:%.1f:%d", f.Type, s.DXCall, s.DECall, s.Frequency, s.Time.Unix())
}

func wwvKey(f *Frame) string {
	return fmt.Sprintf("wwv:%s:%s", f.Type, f.Raw)
}
