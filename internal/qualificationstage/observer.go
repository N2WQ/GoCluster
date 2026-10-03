//go:build qualification

package qualificationstage

import "sync/atomic"

// Event contains borrowed text. An observer must finish using it before return.
type Event struct {
	Stage   Stage
	Comment string
	Worker  int
}

type callback struct{ observe func(Event) }
type Registration struct{ published atomic.Pointer[callback] }

var current atomic.Pointer[Registration]

// Install never replaces an existing owner. Its caller owns callback retirement.
func Install(observe func(Event)) (*Registration, bool) {
	if observe == nil {
		return nil, false
	}
	r, ok := Reserve()
	if ok {
		Publish(r, observe)
	}
	return r, ok
}

// Reserve admits a constructor before it allocates backing. Until Publish,
// observations have no callback; the warm fixture has not offered inputs yet.
func Reserve() (*Registration, bool) {
	r := new(Registration)
	return r, current.CompareAndSwap(nil, r)
}

func Publish(r *Registration, observe func(Event)) {
	r.published.Store(&callback{observe: observe})
}

func Remove(r *Registration) bool {
	return r != nil && current.CompareAndSwap(r, nil)
}

// Observe has no queue and retains no event. Removal does not join callbacks
// already loaded here; the owner must close and recheck its own lifetime guard.
func Observe(stage Stage, comment string, worker int) {
	if r := current.Load(); r != nil {
		if callback := r.published.Load(); callback != nil {
			callback.observe(Event{stage, comment, worker})
		}
	}
}
