package filter

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func FuzzStorageDecodeNativeV6Owners(f *testing.F) {
	for _, raw := range []string{v6StorageMinimalRecord, "configuration_version: 4\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: [{slot: 0, action: pass, expr: 'MODE=CW'}]}, settings: {}}\n",
		v6StorageMinimalRecord + "recent_ips: [&ip abc, *ip]\n",
		v6StorageMinimalRecord + "preset: {name: TEST, baseline: {configuration_version: 4}}\n"} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > MaxUserRecordBytes {
			return
		}
		document := &yaml.Node{}
		if err := yaml.Unmarshal([]byte(raw), document); err != nil {
			return
		}
		if document.Kind != yaml.DocumentNode || len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
			return
		}
		// yaml.Unmarshal itself permits trailing documents. Compare only its
		// single-node callback boundary to the explicit DTO node boundary.
		var dto V6UserRecord
		dtoErr := decodeV6StorageNode(document, storageDecodeLimits{work: maxUserRecordWork}, &dto)
		if dtoErr == nil {
			_, _, dtoErr = prepareV6UserRecord(&dto)
		}
		var native UserRecord
		nativeErr := yaml.Unmarshal([]byte(raw), &native)
		if (dtoErr == nil) != (nativeErr == nil) {
			t.Fatalf("native callback/strict DTO acceptance differs: %v / %v", nativeErr, dtoErr)
		}
		if dtoErr == nil && !recordConfiguration(&native).V6().Equal(dto.Configuration) {
			canonical, _, err := PrepareV6Configuration(dto.Configuration)
			if err != nil || !recordConfiguration(&native).V6().Equal(canonical) {
				t.Fatal("native callback changed canonical definitions or exact settings")
			}
		}
	})
}
