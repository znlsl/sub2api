//go:build integration

package repository

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// The new upstream group-name sync may complete with a snapshot loaded before
// an operator disabled scheduling. That metadata write must not reopen admission.
func TestAccountSchedulingV025ProbeGroupSyncPreservesDisable(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, nil)
	account := &service.Account{
		Name: "local", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true,
		Credentials: map[string]any{"api_key": "test-key"},
		Extra:       map[string]any{service.UpstreamBillingProbeEnabledExtraKey: true, service.UpstreamBillingGroupSyncEnabledExtraKey: true},
	}
	require.NoError(t, repo.Create(ctx, account))
	stale, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.True(t, stale.Schedulable)
	require.NoError(t, repo.SetSchedulable(ctx, account.ID, false))
	snapshot := &service.UpstreamBillingProbeSnapshot{Status: service.UpstreamBillingProbeStatusOK}
	group := service.BillingGroupInfo{Name: "Synced group", Description: "Synced description"}
	require.NoError(t, repo.UpdateUpstreamBillingProbeSnapshot(ctx, stale, snapshot, nil, group))
	actual, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, group.Name, actual.Name)
	require.NotNil(t, actual.Notes)
	require.Equal(t, group.Description, *actual.Notes)
	require.False(t, actual.Schedulable)
	enabled, err := repo.IsAccountSchedulingEnabled(ctx, account.ID)
	require.NoError(t, err)
	require.False(t, enabled)
}
