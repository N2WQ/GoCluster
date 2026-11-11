package cty

import (
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestSnapshotIdentityConcurrentFirstUse(t *testing.T) {
	var db CTYDatabase
	start := make(chan struct{})
	identities := make(chan *SnapshotIdentity, 64)
	var workers sync.WaitGroup
	for i := 0; i < cap(identities); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			identities <- db.SnapshotIdentity()
		}()
	}
	close(start)
	workers.Wait()
	close(identities)
	first := db.SnapshotIdentity()
	if first == nil || reflect.TypeOf(*first).Size() == 0 {
		t.Fatal("identity must be a nonzero-size object")
	}
	for identity := range identities {
		if identity != first {
			t.Fatal("concurrent first access published different tokens")
		}
	}
	for i := 0; i < 4; i++ {
		other := new(CTYDatabase)
		if i%2 == 0 {
			var err error
			other, err = LoadCTYDatabaseFromReader(strings.NewReader(samplePLIST))
			if err != nil {
				t.Fatal(err)
			}
		}
		identity := other.SnapshotIdentity()
		if identity == nil || identity == first || identity != other.SnapshotIdentity() {
			t.Fatal("independent database did not retain its own stable identity")
		}
	}
	if (*CTYDatabase)(nil).SnapshotIdentity() != nil {
		t.Fatal("nil database acquired an identity")
	}
}
