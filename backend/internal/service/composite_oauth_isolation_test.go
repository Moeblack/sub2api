package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompositeOAuthOwnershipDoesNotFollowTransientHealth(t *testing.T) {
	for _, typ := range []string{AccountTypeOAuth, AccountTypeSetupToken, AccountTypeAPIKey} {
		t.Run(typ, func(t *testing.T) {
			repo := &compositeOwnershipAccountRepo{accounts: []Account{
				{ID: 1, Platform: PlatformOpenAI, Type: typ, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"model_mapping": map[string]any{"gpt-custom-private": "gpt-custom-private"}}},
				{ID: 2, Platform: PlatformDeepseek, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"model_mapping": map[string]any{"gpt-custom-private": "deepseek-chat"}}},
			}}
			svc := &GatewayService{accountRepo: repo}
			resolver := NewCompositeRouteResolver(nil)
			resolver.SetModelOwnershipResolver(svc.resolveCompositeModelOwnership)
			before, err := resolver.Resolve(context.Background(), 7, "gpt-custom-private", CompositeRouteEndpointMessages)
			require.NoError(t, err)
			require.False(t, before.Matched)
			require.Equal(t, typ != AccountTypeAPIKey, before.OAuthAmbiguous())

			until := time.Now().Add(time.Hour)
			repo.accounts[0].RateLimitResetAt = &until
			repo.accounts[0].TempUnschedulableUntil = &until
			after, err := resolver.Resolve(context.Background(), 7, "gpt-custom-private", CompositeRouteEndpointMessages)
			require.NoError(t, err)
			require.Equal(t, before, after, "ownership must use configuration candidates, not schedulable accounts")

			resolver.repo = compositeRouteRepoStub{routes: []CompositeModelRoute{{
				ID: 1, GroupID: 7, PublicModel: "gpt-custom-private", MatchType: CompositeRouteMatchExact,
				TargetPlatform: PlatformOpenAI, UpstreamModel: "gpt-custom-private", Endpoint: CompositeRouteEndpointAny, Enabled: true,
			}}}
			explicit, err := resolver.Resolve(context.Background(), 7, "gpt-custom-private", CompositeRouteEndpointResponses)
			require.NoError(t, err)
			require.True(t, explicit.Matched)
			require.False(t, explicit.OAuthAmbiguous())
			require.Equal(t, PlatformOpenAI, explicit.TargetPlatform)
			require.Equal(t, "gpt-custom-private", explicit.UpstreamModel)
		})
	}
}

func TestCompositeOAuthAmbiguityOnlyMarksOpenAIGatewayFallback(t *testing.T) {
	for _, tc := range []struct {
		model   string
		claimed bool
		reject  bool
	}{
		{"gpt-custom-private", true, true},
		{"grok-4", true, true},
		{"gpt-custom-private", false, false},
		{"claude-sonnet-4-5", true, false},
		{"private-alias", true, false},
	} {
		resolver := NewCompositeRouteResolver(nil)
		resolver.SetModelOwnershipResolver(func(context.Context, int64, string) (CompositeModelOwnership, error) {
			return CompositeModelOwnership{Ambiguous: true, OpenAIGatewayOAuthClaimed: tc.claimed}, nil
		})
		decision, err := resolver.Resolve(context.Background(), 7, tc.model, CompositeRouteEndpointMessages)
		require.NoError(t, err)
		require.False(t, decision.Matched)
		require.Equal(t, tc.reject, decision.OAuthAmbiguous(), "model=%s claimed=%v", tc.model, tc.claimed)
	}
}
