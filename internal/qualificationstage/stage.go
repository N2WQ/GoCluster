// Package qualificationstage marks existing handoffs without changing them.
// Ordinary builds erase Observe; qualification observers borrow their inputs.
package qualificationstage

type Stage uint8

const (
	PrimaryReady Stage = iota
	OutputReceived
	DeliveryStart
	BroadcastReady
	BroadcastReceived
	WorkerDispatched
	WorkerStarted
)
