package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestReasoningConfigurationPolicy_HTTPAndWebSocketRejectHistoryChanges(t *testing.T) {
	for _, action := range []string{ReasoningEffortOverLimitDeny, ReasoningEffortOverLimitDowngrade} {
		t.Run(action, func(t *testing.T) {
			body := []byte(`{ "model":"gpt-6-astra", "reasoning":{"mode":"standard","effort":"low"}, "input":[{"type":"configuration_update","reasoning":{"effort":"high"},"opaque":{"large":9007199254740993}},{"role":"user","content":"hard work"},{"type":"configuration_update","reasoning":{"effort":"low"}},{"role":"user","content":"next"}] }`)
			ctx := WithOpenAIReasoningEffortPolicy(context.Background(), "low", nil, action)
			got, changed, err := ApplyOpenAIReasoningEffortPolicyFromContext(ctx, body)
			require.Error(t, err)
			require.True(t, IsReasoningEffortPolicyDenied(err))
			require.False(t, changed)
			require.Equal(t, body, got, "even overridden historical updates cannot be rewritten")
			got, err = applyOpenAIWSReasoningEffortPolicy(body, &OpenAIWSIngressHooks{MaxReasoningEffort: "low", MaxReasoningEffortOverLimit: action})
			require.Error(t, err)
			require.True(t, IsReasoningEffortPolicyDenied(err))
			require.Equal(t, body, got)
		})
	}
}

func TestReasoningConfigurationPolicy_ModelScopedMapping(t *testing.T) {
	for _, target := range []string{"low", ReasoningEffortMappingDeny} {
		body := []byte(`{"model":"gpt-6-astra","reasoning":{"effort":"low"},"input":[{"type":"configuration_update","reasoning":{"effort":"high"}},{"role":"user","content":"next"}]}`)
		mapping := []ReasoningEffortMapping{{From: "high", To: target, Model: "gpt-6-astra", MatchType: "exact"}}
		got, changed, err := ApplyReasoningEffortPolicy(body, "", mapping, "")
		require.Error(t, err)
		require.True(t, IsReasoningEffortPolicyDenied(err))
		require.False(t, changed)
		require.Equal(t, body, got)
		mapping[0].Model = "different-client-model"
		got, changed, err = ApplyReasoningEffortPolicy(body, "", mapping, "")
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, body, got)
	}
}

func TestReasoningConfigurationPolicy_PreservesAllowedAndUnrestrictedHistories(t *testing.T) {
	body := []byte(`{ "model":"gpt-6-astra", "reasoning":{"mode":"standard","effort":"low","context":"all_turns"}, "input":[{"type":"function_call","call_id":"original:42","name":"lookup","arguments":"{}","async":true},{"type":"configuration_update","reasoning":{"effort":"high"},"opaque":{"large":9007199254740993}},{"role":"user","content":"next"},{"type":"function_call_output","call_id":"original:42","output":"late output"}] }`)
	for _, limit := range []string{"", "high"} {
		got, changed, err := ApplyReasoningEffortPolicy(body, limit, nil, "")
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, body, got)
	}
	// Do not cap the top-level prefix after a compatible update is present.
	body = []byte(`{"model":"gpt-6-astra","reasoning":{"effort":"high"},"input":[{"type":"configuration_update","reasoning":{"effort":"low"}}]}`)
	got, changed, err := ApplyReasoningEffortPolicy(body, "low", nil, "")
	require.ErrorContains(t, err, "request-level reasoning prefix")
	require.False(t, changed)
	require.Equal(t, body, got)
}

func TestReasoningConfigurationPolicy_RejectsAmbiguousMembers(t *testing.T) {
	for _, body := range []string{
		`{"model":"gpt-6-astra","input":[{"type":"configuration_update","reasoning":{"effort":"low","effort":"high"}}]}`,
		`{"model":"gpt-6-astra","input":[{"type":"message","type":"configuration_update","reasoning":{"effort":"high"}}]}`,
		`{"model":"gpt-6-astra","input":[],"input":[{"type":"configuration_update","reasoning":{"effort":"high"}}]}`,
		`{"model":"safe-model","model":"gpt-6-astra","input":[{"type":"configuration_update","reasoning":{"effort":"high"}}]}`,
		`{"model":"gpt-6-astra","input":[{"type":"configuration_update","reasoning":{"effort":"low"},"reasoning":{"effort":"high"}}]}`,
	} {
		got, changed, err := ApplyReasoningEffortPolicy([]byte(body), "low", nil, "")
		require.ErrorContains(t, err, "duplicate JSON member")
		require.True(t, IsReasoningEffortPolicyDenied(err))
		require.False(t, changed)
		require.Equal(t, body, string(got))
		got, changed, err = ApplyReasoningEffortPolicy([]byte(body), "", nil, "")
		require.NoError(t, err, "unrestricted forwarding does not mutate or reinterpret caller bytes")
		require.False(t, changed)
		require.Equal(t, body, string(got))
	}
}

func TestReasoningConfigurationEffort_UsesLatestVisibleUpdate(t *testing.T) {
	body := []byte(`{"model":"gpt-6-astra","previous_response_id":"resp_prior","reasoning":{"effort":"low"},"input":[{"type":"configuration_update","reasoning":{"effort":"high"}},{"role":"user","content":"first"},{"type":"configuration_update","reasoning":{"effort":"max"}},{"role":"user","content":"next"},{"type":"configuration_update","reasoning":{}}]}`)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	for _, effort := range []*string{
		extractOpenAIReasoningEffortFromBody(body, "gpt-6-astra"),
		extractOpenAIReasoningEffort(decoded, "gpt-6-astra"),
		CanonicalRequestedReasoningEffort(body, "gpt-6-astra"),
		CanonicalRequestedReasoningEffortFromReqBody(decoded, "gpt-6-astra"),
	} {
		require.NotNil(t, effort)
		require.Equal(t, "max", *effort)
	}
	turn := newOpenAIWSNativeTurn(2, body, body, "gpt-6-astra", nil, nil)
	require.NotNil(t, turn.result.ReasoningEffort)
	require.Equal(t, "max", *turn.result.ReasoningEffort)
	require.NotNil(t, turn.result.RequestedReasoningEffort)
	require.Equal(t, "max", *turn.result.RequestedReasoningEffort)
	require.Equal(t, "low", gjson.GetBytes(turn.payload, "reasoning.effort").String(), "metadata must not rewrite cached request prefix")
	// Server-side inheritance is not visible here; do not report a discovered
	// configuration update merely because a previous_response_id was supplied.
	_, visible := openAIReasoningConfigurationEffort([]byte(`{"previous_response_id":"resp_prior","reasoning":{"effort":"low"},"input":"continue"}`))
	require.False(t, visible)
}
