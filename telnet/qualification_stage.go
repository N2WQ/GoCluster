//go:build qualification

package telnet

import "fmt"

// QualificationStageFanout is a bounded view of the actual current shard
// assignment. It borrows no clients, login strings or slice backing.
type QualificationStageFanout struct {
	Workers int
	Mask    uint32
	Count   int
	Clients [100]QualificationStageClient
}

type QualificationStageClient struct {
	SessionID uint64
	Worker    uint32
}

func (s *Server) QualificationStageFanout() (QualificationStageFanout, error) {
	var out QualificationStageFanout
	shards := s.cachedClientShards()
	if len(shards) == 0 || len(shards) > 32 {
		return out, fmt.Errorf("unsupported diagnostic shard count %d", len(shards))
	}
	out.Workers = len(shards)
	for worker, clients := range shards {
		for _, client := range clients {
			if client == nil {
				continue
			}
			if out.Count == len(out.Clients) {
				return out, fmt.Errorf("diagnostic client capacity exceeded")
			}
			out.Mask |= uint32(1) << uint(worker)
			out.Clients[out.Count] = QualificationStageClient{SessionID: client.peerSessionID, Worker: uint32(worker)}
			out.Count++
		}
	}
	return out, nil
}
