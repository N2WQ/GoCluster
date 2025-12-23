package peer

// WWVEvent describes a parsed WWV/WWC frame.
type WWVEvent struct {
	Kind   string // PC23 or PC73
	Date   string
	Hour   string
	SFI    string
	A      string
	K      string
	Extra  []string
	Origin string
	Hop    int
}
