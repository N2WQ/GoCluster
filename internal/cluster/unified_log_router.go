package cluster

import "sync"

// Paho captures log.Writer once for process-global debug loggers. This stable
// router lets that captured writer follow the current runtime without retaining
// an old fanout and its owned resources after shutdown.
type processLogRouter struct {
	mu             sync.Mutex
	target         *systemLogFanout
	users          int
	retiring       bool
	holdRetirement bool
	drained        chan struct{}
}

var sharedProcessLogRouter processLogRouter

func (r *processLogRouter) install(target *systemLogFanout) bool {
	if target == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.target != nil || r.retiring {
		return false
	}
	r.target = target
	r.drained = make(chan struct{})
	return true
}

// clear withdraws admission without waiting for an already admitted writer.
// Its signal closes only after every writer has released the old fanout.
func (r *processLogRouter) clear(target *systemLogFanout) <-chan struct{} {
	return r.clearGeneration(target, false)
}

// clearForRetirement keeps the generation gate closed even after its final
// borrower returns, until the old fanout has actually cleared its references.
func (r *processLogRouter) clearForRetirement(target *systemLogFanout) <-chan struct{} {
	return r.clearGeneration(target, true)
}

func (r *processLogRouter) clearGeneration(target *systemLogFanout, hold bool) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if target == nil || r.target != target {
		return nil
	}
	done := r.drained
	r.target = nil
	r.retiring = true
	r.holdRetirement = hold
	if r.users == 0 {
		close(done)
		if !hold {
			r.drained = nil
			r.retiring = false
		}
	}
	return done
}

func (r *processLogRouter) finishRetirement(done <-chan struct{}) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if done == nil || r.drained != done || !r.retiring || !r.holdRetirement || r.users != 0 {
		return false
	}
	r.drained = nil
	r.retiring = false
	r.holdRetirement = false
	return true
}

func (r *processLogRouter) Write(p []byte) (int, error) {
	r.mu.Lock()
	target := r.target
	if target == nil || r.users == int(^uint(0)>>1) {
		r.mu.Unlock()
		return len(p), nil
	}
	r.users++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.users--
		if r.users == 0 && r.retiring {
			close(r.drained)
			if !r.holdRetirement {
				r.drained = nil
				r.retiring = false
			}
		}
		r.mu.Unlock()
	}()
	return target.Write(p)
}
