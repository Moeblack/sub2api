package service

import (
	"reflect"
	"strings"
	"testing"
)

// Native call IDs must remain opaque even when an old Codex-compatible prefix
// repair would have changed or hashed them. Exercise paired, asynchronous and
// compacted history with the same identifier rather than testing only one item.
func FuzzOpenAINativeCallIDPreservation(f *testing.F) {
	f.Add("call_original:task/7", false)
	f.Add("ctc_custom-call", true)
	f.Add("call_"+strings.Repeat("long-original-", 12), true)
	f.Add("", false)
	f.Add(" id with whitespace ", true)
	f.Fuzz(func(t *testing.T, callID string, async bool) {
		if len(callID) > 4096 {
			t.Skip("bound a single fuzz case's memory")
		}
		for _, kinds := range [][2]string{
			{"function_call", "function_call_output"},
			{"custom_tool_call", "custom_tool_call_output"},
			{"tool_search_call", "tool_search_output"},
		} {
			call := map[string]any{"type": kinds[0], "call_id": callID, "name": "lookup", "async": async}
			output := map[string]any{"type": kinds[1], "call_id": callID, "output": ""}
			if kinds[0] == "tool_search_call" {
				call["execution"], output["execution"] = "client", "client"
			}
			compaction := map[string]any{"type": "compaction", "encrypted_content": "opaque-state"}
			configuration := map[string]any{"type": "configuration_update", "reasoning": map[string]any{"effort": "high"}}
			reference := map[string]any{"type": "item_reference", "id": callID}
			input := []any{compaction, call, configuration, reference, output}
			got := filterCodexInputWithOptions(input, codexInputFilterOptions{
				PreserveReferences: true, PreserveNativeCallIDs: true,
			})
			if len(got) != len(input) {
				t.Fatalf("%s: history item disappeared", kinds[0])
			}
			itemAt := func(index int) map[string]any {
				t.Helper()
				item, ok := got[index].(map[string]any)
				if !ok {
					t.Fatalf("%s: history item %d must remain an object, got %T", kinds[0], index, got[index])
				}
				return item
			}
			for _, index := range []int{1, 4} {
				item := itemAt(index)
				if gotID, exists := item["call_id"]; !exists || gotID != callID {
					t.Fatalf("%s: native call ID changed at item %d", kinds[0], index)
				}
			}
			if itemAt(3)["id"] != callID {
				t.Fatal("native item reference changed")
			}
			if itemAt(1)["async"] != async || itemAt(4)["output"] != "" {
				t.Fatal("async marker or empty output changed")
			}
			if !reflect.DeepEqual(got[0], compaction) || !reflect.DeepEqual(got[2], configuration) {
				t.Fatal("opaque compaction or configuration update changed")
			}
			if call["call_id"] != callID || output["call_id"] != callID || reference["id"] != callID {
				t.Fatal("original caller-owned history was mutated")
			}
		}
	})
}
