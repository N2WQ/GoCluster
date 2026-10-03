//go:build sqlite3_qualification && !windows

package peer

import "context"

func topologyQualificationModes() []string                                       { return []string{"native"} }
func topologyQualificationContext(ctx context.Context, _ string) context.Context { return ctx }
