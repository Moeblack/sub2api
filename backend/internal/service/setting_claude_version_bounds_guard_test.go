//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type claudeVersionBoundsGuardRepo struct {
	SettingRepository
	contextSeen context.Context
}

func (r *claudeVersionBoundsGuardRepo) GetMultiple(ctx context.Context, _ []string) (map[string]string, error) {
	r.contextSeen = ctx
	return map[string]string{
		SettingKeyMinClaudeCodeVersion: "2.1.1",
		SettingKeyMaxClaudeCodeVersion: "2.9.9",
	}, nil
}

func TestClaudeCodeVersionBounds_MissingServiceOrRepository(t *testing.T) {
	for _, svc := range []*SettingService{nil, {}} {
		min, max := svc.GetClaudeCodeVersionBounds(nil)
		require.Empty(t, min)
		require.Empty(t, max)
	}
}

func TestClaudeCodeVersionBounds_NilContextAndEmptyCache(t *testing.T) {
	previous, _ := versionBoundsCache.Load().(*cachedVersionBounds)
	versionBoundsSF.Forget("version_bounds")
	versionBoundsCache.Store((*cachedVersionBounds)(nil))
	t.Cleanup(func() {
		versionBoundsSF.Forget("version_bounds")
		versionBoundsCache.Store(previous)
	})

	repo := &claudeVersionBoundsGuardRepo{}
	svc := &SettingService{settingRepo: repo}
	min, max := svc.GetClaudeCodeVersionBounds(nil)
	require.Equal(t, "2.1.1", min)
	require.Equal(t, "2.9.9", max)
	require.NotNil(t, repo.contextSeen)
}
