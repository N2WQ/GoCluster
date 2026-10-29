package filter

import "testing"

func TestNativeStateLegacyAndMergeFieldsProtected(t *testing.T) {
	usePresetTestDir(t)
	for _, raw := range []string{
		"dxstates: {CT: true}\n",
		"configuration_version: 2\ndxstates: {CT: true}\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: [], dxstates: {CT: true}}, settings: {}}\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: [], <<: {dxstates: {CT: true}}}, settings: {}}\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: [{slot: 0, action: pass, expr: 'DXSTATE=BAD'}]}, settings: {}}\n",
	} {
		assertMinSNRStorageProtected(t, raw)
	}
}
