package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"dxcluster/cty"
)

func main() {
	dataPath := flag.String("data", "data/cty/cty.plist", "path to cty.plist data file")
	flag.Parse()

	db, err := cty.LoadCTYDatabase(*dataPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading CTY database: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("loaded CTY database with %d entries\n", len(db.Keys))
	fmt.Println("enter callsigns (Ctrl+C to quit)")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		call := strings.TrimSpace(scanner.Text())
		if call == "" {
			continue
		}
		info, ok := db.LookupCallsign(call)
		if !ok {
			fmt.Println("no matching prefix")
			continue
		}
		fmt.Printf("%s -> prefix=%s, country=%s, CQ=%d, ITU=%d, lat=%.4f, lon=%.4f\n",
			call, info.Prefix, info.Country, info.CQZone, info.ITUZone, info.Latitude, info.Longitude)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "input error: %v\n", err)
	}
}
