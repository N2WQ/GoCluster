// flood_replay imports private captures and compares explicitly hypothetical
// reporting-station policies offline. It never connects to a cluster.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"dxcluster/internal/floodcorpus"
)

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("use import, synthetic or compare")
	}
	f := flag.NewFlagSet("flood_replay", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	switch args[0] {
	case "import":
		input := f.String("input", "", "")
		output := f.String("output", "", "")
		direction := f.String("direction", "", "")
		transport := f.String("transport", "", "")
		if f.Parse(args[1:]) != nil || f.NArg() != 0 || *input == "" || *output == "" {
			return errors.New("invalid import arguments")
		}
		m, err := floodcorpus.Convert(*input, *output, *direction, *transport)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(struct {
			Events   int   `json:"events"`
			Complete bool  `json:"complete"`
			Pending  bool  `json:"pending_tail"`
			Unsent   int64 `json:"unsent_bytes"`
		}{m.Events, m.Complete, m.PendingTail, m.Unsent})
	case "synthetic":
		output := f.String("output", "", "")
		if f.Parse(args[1:]) != nil || f.NArg() != 0 || *output == "" {
			return errors.New("invalid synthetic arguments")
		}
		m, err := floodcorpus.Generate(*output)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(struct {
			Events int `json:"events"`
		}{m.Events})
	case "compare":
		input := f.String("input", "", "")
		output := f.String("output", "", "")
		threshold := f.Int("threshold", 0, "")
		window := f.Int("window-seconds", 0, "")
		cap := f.Int("actor-cap", 0, "")
		partial := f.Bool("allow-partial", false, "")
		if f.Parse(args[1:]) != nil || f.NArg() != 0 || *input == "" {
			return errors.New("invalid comparison arguments")
		}
		r, err := floodcorpus.Compare(*input, floodcorpus.Options{Threshold: *threshold, WindowSeconds: *window, ActorCap: *cap, AllowPartial: *partial})
		if err != nil {
			return err
		}
		if *output != "" {
			if strings.HasPrefix(*output, `\\`) || strings.HasPrefix(*output, "//") {
				return errors.New("local output required")
			}
			file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return errors.New("comparison output must be new")
			}
			err = json.NewEncoder(file).Encode(r)
			closeErr := file.Close()
			if err != nil || closeErr != nil {
				return errors.New("comparison output failed")
			}
		}
		return json.NewEncoder(out).Encode(r)
	default:
		return errors.New("use import, synthetic or compare")
	}
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
