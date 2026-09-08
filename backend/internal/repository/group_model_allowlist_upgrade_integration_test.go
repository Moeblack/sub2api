//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"

	dbmigrations "github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

// Exercise the actual 235 -> 236 upgrade from the pre-0.2.3 column shape.
// Keeping the old allowlist values is necessary both for enforcing enabled
// policies and for avoiding accidentally enabling an administrator's draft.
func TestMigrations235236PreserveLegacyAllowlistAndChangeQueryContract(t *testing.T) {
	tx := testTx(t)
	ctx := context.Background()

	_, err := tx.ExecContext(ctx, "ALTER TABLE groups RENAME COLUMN model_allowlist TO models_list_config")
	require.NoError(t, err)

	configs := []string{
		`{"enabled":true,"models":["gpt-5.6","gpt-6"]}`,
		`{"enabled":false,"models":["draft-model"]}`,
		`{}`,
	}
	ids := make([]int64, len(configs))
	for i, config := range configs {
		require.NoError(t, tx.QueryRowContext(ctx, `
INSERT INTO groups (name, platform, rate_multiplier, status, models_list_config)
VALUES ($2, 'openai', 1, 'active', $1::jsonb)
RETURNING id`, config, fmt.Sprintf("migration-235-236-upgrade-%d", i)).Scan(&ids[i]))
	}

	for _, name := range []string{"235_group_model_allowlist.sql", groupModelAllowlistRepairMigration} {
		migrationSQL, err := dbmigrations.FS.ReadFile(name)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, string(migrationSQL))
		require.NoError(t, err, name)
	}

	for i, id := range ids {
		var config string
		require.NoError(t, tx.QueryRowContext(ctx,
			"SELECT model_allowlist::text FROM groups WHERE id = $1", id).Scan(&config))
		require.JSONEq(t, configs[i], config)
	}
	requireModelAllowlistColumnShape(ctx, t, tx)

	// An old binary still queries models_list_config, so an image-only rollback
	// cannot work after the rename. A savepoint keeps this probe from aborting
	// the surrounding transaction; its final rollback restores the test schema.
	_, err = tx.ExecContext(ctx, "SAVEPOINT legacy_query_probe")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "SELECT models_list_config FROM groups LIMIT 0")
	require.ErrorContains(t, err, `column "models_list_config" does not exist`)
	_, err = tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT legacy_query_probe")
	require.NoError(t, err)
}
