package openai

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultModelsIncludeBareGPT56Alias(t *testing.T) {
	require.Contains(t, DefaultModelIDs(), "gpt-5.6")
}

func TestDefaultModelsIncludeGPT6Astra(t *testing.T) {
	require.Contains(t, DefaultModelIDs(), "gpt-6-astra")
	require.Contains(t, DefaultModelIDs(), "gpt-6")
}

func TestDefaultModelsContainConcreteGPT56SolForAccountTests(t *testing.T) {
	require.Contains(t, DefaultModelIDs(), "gpt-5.6-sol")
}

func TestDefaultModelIDsAreUnique(t *testing.T) {
	seen := make(map[string]struct{})
	for _, id := range DefaultModelIDs() {
		require.NotContains(t, seen, id, "model catalog must not expose duplicate IDs")
		seen[id] = struct{}{}
	}
}

func TestDefaultModelsIncludeGPTImage25(t *testing.T) {
	require.Contains(t, DefaultModelIDs(), "gpt-image-2.5-flare")
	require.Contains(t, DefaultModelIDs(), "gpt-image-2.5-sunburst")
}

func TestGPT6SolLunaModelIdentity(t *testing.T) {
	for _, model := range []string{"gpt-6-sol", "gpt-6-luna"} {
		require.Contains(t, DefaultModelIDs(), model)
		require.True(t, IsGPT6SolOrLunaModelSpelling(model))
	}
	require.False(t, IsGPT6SolOrLunaModelSpelling("gpt-6-astra"))
	require.False(t, IsGPT6SolOrLunaModelSpelling("gpt-6-solitude"))
	require.False(t, IsGPT6SolOrLunaModelSpelling("gpt-6-luna-preview"))
}

func TestGPT61SolIdentityAndEffort(t *testing.T) {
	require.Contains(t, DefaultModelIDs(), "gpt-6.1-sol")
	for _, id := range []string{"gpt-6.1-sol", "openai/gpt-6.1-sol-max", "GPT_6.1_SOL", "gpt-6.1-sol-openai-compact"} {
		require.True(t, IsGPT61SolModelSpelling(id), id)
		require.False(t, IsGPT6SolOrLunaModelSpelling(id), id)
	}
	for _, id := range []string{"gpt-6.1", "gpt-6.1-solitude", "gpt-6.1-sol-preview", "gpt-6-sol"} {
		require.False(t, IsGPT61SolModelSpelling(id), id)
	}
	for _, effort := range []string{"", "low", "medium", "high", "xhigh", "max"} {
		require.NoError(t, ValidateGPT61SolReasoningEffort("gpt-6.1-sol", effort))
	}
	for _, effort := range []string{"none", "minimal"} {
		require.Error(t, ValidateGPT61SolReasoningEffort("gpt-6.1-sol", effort))
		require.NoError(t, ValidateGPT61SolReasoningEffort("gpt-6-sol", effort))
	}
	require.Contains(t, CodexBaseInstructionsForModel("gpt-6.1-sol"), "based on GPT-6")
}
