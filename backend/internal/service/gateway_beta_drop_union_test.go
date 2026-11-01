//go:build unit

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeBetaPolicyDropSetsDoesNotPolluteDefaults(t *testing.T) {
	defaultsBefore := make(map[string]struct{}, len(defaultDroppedBetasSet))
	for token := range defaultDroppedBetasSet {
		defaultsBefore[token] = struct{}{}
	}
	for _, sets := range [][]map[string]struct{}{
		nil,
		{nil, nil},
		{{}, {}},
		{nil, {"request-only-beta": {}}},
		{{"evaluated-beta": {}}, {"cached-beta": {}}},
	} {
		merged := mergeBetaPolicyDropSets(sets...)
		for token := range defaultsBefore {
			require.Contains(t, merged, token)
		}
		for _, set := range sets {
			for token := range set {
				require.Contains(t, merged, token)
			}
		}
		// Mutating the returned set must never affect process-wide defaults,
		// including when both dynamic policy sets are absent or empty.
		for token := range merged {
			delete(merged, token)
		}
		merged["mutation-must-stay-local"] = struct{}{}
		require.Len(t, defaultDroppedBetasSet, len(defaultsBefore))
		for token := range defaultsBefore {
			require.Contains(t, defaultDroppedBetasSet, token)
		}
		require.Equal(t, defaultsBefore, mergeBetaPolicyDropSets())
	}
}

func TestMergeBetaPolicyDropSetsDoesNotAliasInputs(t *testing.T) {
	evaluated := map[string]struct{}{"evaluated-beta": {}, "shared-beta": {}}
	cached := map[string]struct{}{"cached-beta": {}, "shared-beta": {}}
	merged := mergeBetaPolicyDropSets(evaluated, cached)
	require.Contains(t, merged, "evaluated-beta")
	require.Contains(t, merged, "cached-beta")
	require.Contains(t, merged, "shared-beta")
	delete(merged, "shared-beta")
	merged["request-local-beta"] = struct{}{}
	require.Equal(t, map[string]struct{}{"evaluated-beta": {}, "shared-beta": {}}, evaluated)
	require.Equal(t, map[string]struct{}{"cached-beta": {}, "shared-beta": {}}, cached)
	evaluated["later-evaluated-beta"] = struct{}{}
	cached["later-cached-beta"] = struct{}{}
	require.NotContains(t, merged, "later-evaluated-beta")
	require.NotContains(t, merged, "later-cached-beta")
}
