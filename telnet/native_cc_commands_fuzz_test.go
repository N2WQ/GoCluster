package telnet

import (
	"testing"

	"dxcluster/filter"
)

func FuzzNativeCCCommandOwnership(f *testing.F) {
	for _, seed := range []string{"SET/NOFT8", "SET/FILTER DOC/PASS 291,1", "SET/FILTER DOS/REJECT NY", "SET/FILTER DXBM/PASS 20-CW", "SET/NOFILTER", "UNSET/SELF", "PASS RULE 1 MODE=CW", "SET/FT8 extra"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > 256 {
			return
		}
		c, engine, _ := nativeCCCommandFixture(t)
		before := nativeCCConfiguration(t, c)
		settings, pointer, grid, dialect := c.configuredSettings, c.filter, c.grid, c.dialect
		outcome := engine.stageOwned(c, line)
		after := nativeCCConfiguration(t, c)
		if !outcome.publish && !before.Equal(after) {
			t.Fatal("refused or unknown command changed native state")
		}
		if c.filter != pointer || c.configuredSettings != settings || c.grid != grid || c.dialect != dialect {
			t.Fatal("native edit escaped its filter owner")
		}
		canonical, _, err := filter.PrepareCCConfiguration(after)
		if err != nil || !canonical.Equal(after) {
			t.Fatalf("accepted command left invalid/noncanonical state: %v", err)
		}
	})
}
