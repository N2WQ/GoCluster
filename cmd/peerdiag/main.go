// peerdiag is the isolated diagnostic file owner for GoCluster peers. It must
// remain a minimal companion: importing the cluster package would initialize
// unrelated model/database state outside the diagnostic resource reservation.
package main

import (
	"flag"
	"os"

	"dxcluster/internal/peerdiag"
)

func main() {
	address := flag.String("address", "", "parent loopback endpoint")
	token := flag.String("token", "", "parent authentication token")
	flag.Parse()
	if peerdiag.RunHelper(*address, *token) != nil {
		os.Exit(1)
	}
}
