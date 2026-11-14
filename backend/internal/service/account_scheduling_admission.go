package service

import (
	"context"
	"errors"
	"fmt"
)

// ErrAccountSchedulingDisabled requests reselection without failing an account
// or interrupting a request that has already been admitted.
var ErrAccountSchedulingDisabled = errors.New("account scheduling is disabled")

// ErrAccountSchedulingCheckFailed is a control-plane failure, not an upstream
// account health failure. It must refuse new work without penalizing the account.
var ErrAccountSchedulingCheckFailed = errors.New("account scheduling check failed")

// AccountSchedulingStateReader reads the operator-controlled admission gate
// from the database, independently of scheduler snapshots and profit gates.
// It is optional so other AccountRepository implementations can use GetByID.
type AccountSchedulingStateReader interface {
	IsAccountSchedulingEnabled(context.Context, int64) (bool, error)
}

// CheckAccountScheduling must run AFTER acquiring the request's concurrency
// slot and BEFORE forwarding a new request/WS turn. A request admitted before
// disable remains counted until it finishes; disable never cancels that work.
// Read failures fail closed, since a stale snapshot cannot authorize new work.
func (s *OpenAIGatewayService) CheckAccountScheduling(ctx context.Context, accountID int64) error {
	if s == nil || s.accountRepo == nil {
		return fmt.Errorf("%w: repository is unavailable", ErrAccountSchedulingCheckFailed)
	}
	if accountID <= 0 {
		return ErrAccountSchedulingDisabled
	}
	if reader, ok := s.accountRepo.(AccountSchedulingStateReader); ok {
		enabled, err := reader.IsAccountSchedulingEnabled(ctx, accountID)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrAccountSchedulingCheckFailed, err)
		}
		if !enabled {
			return ErrAccountSchedulingDisabled
		}
		return nil
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if errors.Is(err, ErrAccountNotFound) {
		return ErrAccountSchedulingDisabled
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrAccountSchedulingCheckFailed, err)
	}
	if account == nil || account.ID != accountID || !account.Schedulable || !account.IsActive() {
		return ErrAccountSchedulingDisabled
	}
	return nil
}
