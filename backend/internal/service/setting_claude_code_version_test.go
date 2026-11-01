//go:build unit

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/stretchr/testify/require"
)

func TestGetClaudeCodeClientVersionPriority(t *testing.T) {
	for _, tt := range []struct {
		name   string
		manual string
		synced string
		want   string
	}{
		{name: "手动值优先", manual: "2.1.280", synced: "2.1.281", want: "2.1.280"},
		{name: "归一化手动值", manual: " v2.1.280 ", synced: "2.1.281", want: "2.1.280"},
		{name: "跟随同步值", synced: "2.1.281", want: "2.1.281"},
		{name: "无效手动值回退", manual: "invalid", synced: "2.1.281", want: "2.1.281"},
		{name: "无效同步值回退", synced: "2.1.9", want: claude.CLIVersion()},
		{name: "未配置时回退", want: claude.CLIVersion()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := &authSourceDefaultsRepoStub{values: map[string]string{
				SettingKeyClaudeCodeClientVersion:       tt.manual,
				SettingKeyClaudeCodeClientVersionSynced: tt.synced,
			}}
			svc := NewSettingService(repo, &config.Config{})
			require.Equal(t, tt.want, svc.GetClaudeCodeClientVersion(context.Background()))
		})
	}
}

func TestUpdateSettingsClaudeCodeVersionTakesEffectImmediately(t *testing.T) {
	ctx := context.Background()
	repo := &authSourceDefaultsRepoStub{values: map[string]string{
		SettingKeyClaudeCodeClientVersionSynced: "2.1.281",
	}}
	svc := NewSettingService(repo, &config.Config{})
	resetGatewayForwardingSettingsCacheForTest(t)
	defer svc.refreshCachedSettings(&SystemSettings{})
	require.Equal(t, "2.1.281", svc.GetClaudeCodeClientVersion(ctx))

	settings := &SystemSettings{
		ClaudeCodeClientVersion:          "2.1.280",
		ClaudeCodeVersionAutoSyncEnabled: true,
	}
	require.NoError(t, svc.UpdateSettings(ctx, settings))
	require.Equal(t, "2.1.280", svc.GetClaudeCodeClientVersion(ctx), "保存手动版本后应立即生效")
	require.NotContains(t, repo.updates, SettingKeyClaudeCodeClientVersionSynced)
	require.Equal(t, "2.1.281", repo.values[SettingKeyClaudeCodeClientVersionSynced])

	// 清空手动值并关闭后续同步时，仍保留并使用已有同步值。
	settings.ClaudeCodeClientVersion = ""
	settings.ClaudeCodeVersionAutoSyncEnabled = false
	require.NoError(t, svc.UpdateSettings(ctx, settings))
	require.Equal(t, "2.1.281", svc.GetClaudeCodeClientVersion(ctx))
	require.Equal(t, "false", repo.values[SettingKeyClaudeCodeVersionAutoSyncEnabled])
}

// The repository captures its snapshot before blocking, like an in-flight DB
// read concurrent with an administrator save or the version sync worker.
type blockedClaudeVersionRepo struct {
	SettingRepository
	mu      sync.Mutex
	version string
	calls   int
	err     error
	started chan struct{}
	release chan struct{}
}

func (r *blockedClaudeVersionRepo) GetMultiple(ctx context.Context, _ []string) (map[string]string, error) {
	r.mu.Lock()
	r.calls++
	first := r.calls == 1
	version, err := r.version, r.err
	r.mu.Unlock()
	if first {
		close(r.started)
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return map[string]string{SettingKeyClaudeCodeClientVersionSynced: version}, err
}

func TestClaudeCodeVersionInvalidationRejectsInflightSnapshot(t *testing.T) {
	for _, oldError := range []bool{false, true} {
		for _, refreshFirst := range []bool{false, true} {
			name := "success"
			if oldError {
				name = "error"
			}
			if refreshFirst {
				name += "/new_cache_before_old_result"
			} else {
				name += "/old_result_before_refresh"
			}
			t.Run(name, func(t *testing.T) {
				repo := &blockedClaudeVersionRepo{
					version: "2.1.280",
					started: make(chan struct{}),
					release: make(chan struct{}),
				}
				if oldError {
					repo.err = errors.New("old read failed")
				}
				var release sync.Once
				t.Cleanup(func() { release.Do(func() { close(repo.release) }) })
				svc := NewSettingService(repo, &config.Config{})
				oldResult := make(chan string, 1)
				go func() { oldResult <- svc.GetClaudeCodeClientVersion(context.Background()) }()
				select {
				case <-repo.started:
				case <-time.After(time.Second):
					t.Fatal("repository read did not start")
				}
				repo.mu.Lock()
				repo.version, repo.err = "2.1.281", nil
				repo.mu.Unlock()
				svc.InvalidateClaudeCodeClientVersionCache()
				if refreshFirst {
					// A new generation must not join the blocked old singleflight.
					freshResult := make(chan string, 1)
					go func() { freshResult <- svc.GetClaudeCodeClientVersion(context.Background()) }()
					select {
					case version := <-freshResult:
						require.Equal(t, "2.1.281", version)
					case <-time.After(time.Second):
						t.Fatal("new generation joined the old flight")
					}
				}
				release.Do(func() { close(repo.release) })
				select {
				case version := <-oldResult:
					want := "2.1.280"
					if oldError {
						want = claude.CLIVersion()
					}
					require.Equal(t, want, version, "old caller may receive its own snapshot")
				case <-time.After(time.Second):
					t.Fatal("old caller did not finish")
				}
				if !refreshFirst {
					require.Empty(t, svc.claudeCodeVersionCache.Load().version, "invalidated cache must remain empty")
				}
				require.Equal(t, "2.1.281", svc.GetClaudeCodeClientVersion(context.Background()))
				require.Equal(t, "2.1.281", svc.GetClaudeCodeClientVersion(context.Background()))
				repo.mu.Lock()
				calls := repo.calls
				repo.mu.Unlock()
				require.Equal(t, 2, calls, "fresh generation should remain cached")
			})
		}
	}
}

func TestClaudeCodeVersionCacheTTLs(t *testing.T) {
	for _, fail := range []bool{false, true} {
		repo := &blockedClaudeVersionRepo{version: "2.1.281", calls: 1}
		ttl := claudeCodeClientVersionCacheTTL
		if fail {
			repo.err = errors.New("DB unavailable")
			ttl = claudeCodeClientVersionErrorTTL
		}
		svc := NewSettingService(repo, &config.Config{})
		before := time.Now()
		svc.GetClaudeCodeClientVersion(context.Background())
		expires := time.Unix(0, svc.claudeCodeVersionCache.Load().expiresAt)
		require.False(t, expires.Before(before.Add(ttl)))
		require.False(t, expires.After(time.Now().Add(ttl)))
	}
}
