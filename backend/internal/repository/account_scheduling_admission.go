package repository

import (
	"context"

	dbaccount "github.com/Wei-Shaw/sub2api/ent/account"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.AccountSchedulingStateReader = (*accountRepository)(nil)

// IsAccountSchedulingEnabled is the terminal admission check. Query only the
// manual gate, not credentials/groups or a potentially stale scheduler cache.
func (r *accountRepository) IsAccountSchedulingEnabled(ctx context.Context, id int64) (bool, error) {
	if scoped := r.accountTransactionScope(ctx); scoped != r {
		return scoped.IsAccountSchedulingEnabled(ctx, id)
	}
	return r.client.Account.Query().Where(
		dbaccount.IDEQ(id), dbaccount.DeletedAtIsNil(),
		dbaccount.StatusEQ(service.StatusActive), dbaccount.SchedulableEQ(true),
	).Exist(ctx)
}
