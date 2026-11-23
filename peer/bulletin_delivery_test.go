package peer

import (
	"reflect"
	"testing"
	"time"
)

func TestPC93AndWWVSourcedBulletinDelivery(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	snapshot := LocalMembership{Revision: 9, RawCount: 1, Complete: true, Users: []LocalUser{{Login: "K1USER", SessionID: 42, IP: "192.0.2.22"}}}
	p.manager.SetMembershipProvider(func() LocalMembership { return snapshot })
	type record struct {
		kind, from, line string
		at               time.Time
	}
	var got []record
	p.manager.SetBulletinDelivery(func(kind, line, from string, at time.Time) {
		got = append(got, record{kind, from, line, at})
	}, func(login string, id, revision uint64, line, from string, at time.Time) bool {
		if login != "K1USER" || id != 42 || revision != 9 {
			t.Fatalf("TALK owner changed: %s, %d, %d", login, id, revision)
		}
		got = append(got, record{"TALK", from, line, at})
		return true
	})
	receiveControllerWire(t, p, source, "PC93^N2AAA^43200^*^K1FROM^*^hello^N2AAA^192.0.2.11^H10^", now)
	receiveControllerWire(t, p, source, "PC93^N2AAA^43201^K1USER^K1FROM^*^private^N2AAA^192.0.2.11^H10^", now.Add(time.Second))
	p.manager.broadcastWWV(WWVEvent{Kind: "PC23", SFI: "150", Origin: "N2AAA", Logger: "K1WX"}, now.Add(2*time.Second))
	p.manager.broadcastWWV(WWVEvent{Kind: "PC73", K: "2", Origin: "N2AAA"}, now.Add(3*time.Second))
	want := []record{
		{"ANNOUNCE", "K1FROM", "To ALL de K1FROM: hello", now},
		{"TALK", "K1FROM", "To K1USER de K1FROM: private", now.Add(time.Second)},
		{"WWV", "K1WX", "WWV de K1WX : SFI=150", now.Add(2 * time.Second)},
		{"WCY", "N2AAA", "WCY de N2AAA : K=2", now.Add(3 * time.Second)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bulletin metadata and private-field exclusion: got %+v want %+v", got, want)
	}
}
