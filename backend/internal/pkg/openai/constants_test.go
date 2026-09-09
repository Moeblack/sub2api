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
