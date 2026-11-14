package repository

import (
	"context"
	"database/sql"
	"testing"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestAccountSchedulingAdmissionReadsDatabaseGate(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY, status TEXT, schedulable BOOLEAN, deleted_at DATETIME)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO accounts VALUES (5579,'active',TRUE,NULL)")
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.SQLite, db)))
	repo := newAccountRepositoryWithSQL(client, db, nil)
	check := func(want bool) {
		t.Helper()
		enabled, readErr := repo.IsAccountSchedulingEnabled(context.Background(), 5579)
		require.NoError(t, readErr)
		require.Equal(t, want, enabled)
	}
	check(true)
	_, err = db.Exec("UPDATE accounts SET schedulable=FALSE WHERE id=5579")
	require.NoError(t, err)
	check(false)
	_, err = db.Exec("UPDATE accounts SET schedulable=TRUE,status='disabled' WHERE id=5579")
	require.NoError(t, err)
	check(false)
	_, err = db.Exec("UPDATE accounts SET status='active',deleted_at=CURRENT_TIMESTAMP WHERE id=5579")
	require.NoError(t, err)
	check(false)
	_, err = db.Exec("UPDATE accounts SET deleted_at=NULL WHERE id=5579")
	require.NoError(t, err)
	check(true)
	enabled, err := repo.IsAccountSchedulingEnabled(context.Background(), 9999)
	require.NoError(t, err)
	require.False(t, enabled)
	require.NoError(t, db.Close())
	_, err = repo.IsAccountSchedulingEnabled(context.Background(), 5579)
	require.Error(t, err, "database failure must not authorize admission")
}
