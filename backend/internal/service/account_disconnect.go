package service

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrAccountDisconnectUnsupported = errors.New("force disconnect supports OpenAI gateway accounts only")
	ErrAccountDisconnectUnverified  = errors.New("unable to verify account disconnect completion")
)

// AccountDisconnectResult never equates issuing cancellation with being drained.
// Only the current process's OpenAI gateway requests/sessions are controllable.
// Redis concurrency is also checked so work elsewhere cannot silently count as zero.
type AccountDisconnectResult struct {
	AccountID                    int64                `json:"account_id"`
	Schedulable                  bool                 `json:"schedulable"`
	Scope                        string               `json:"scope"`
	CancellationRequested        AccountRequestCounts `json:"cancellation_requested"`
	Remaining                    AccountRequestCounts `json:"remaining"`
	ClosedUpstreamConnections    int                  `json:"closed_upstream_connections"`
	RemainingUpstreamConnections int                  `json:"remaining_upstream_connections"`
	CurrentConcurrency           *int                 `json:"current_concurrency"`
	Drained                      bool                 `json:"drained"`
	TimedOut                     bool                 `json:"timed_out"`
}

// DisconnectAccount disables new admission, cancels this instance's requests,
// aborts its upstream WS pool, then waits for forwarding cleanup and real slots.
// An already-disabled account is valid; repeating the operation is safe.
func (s *OpenAIGatewayService) DisconnectAccount(ctx context.Context, accountID int64, wait time.Duration) (*AccountDisconnectResult, error) {
	if wait < 0 || wait > 30*time.Second {
		return nil, errors.New("disconnect wait must be between 0 and 30 seconds")
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, ErrAccountNotFound
	}
	if account.Platform != PlatformOpenAI {
		return nil, ErrAccountDisconnectUnsupported
	}
	endDrain := s.accountRequests.startDrain(accountID)
	defer endDrain()
	// Once accepted, a management client disconnect must not leave a half-
	// applied operation (disabled but never signalled). Persistence is bounded.
	persistCtx, cancelPersist := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	err = s.accountRepo.SetSchedulable(persistCtx, accountID, false)
	cancelPersist()
	if err != nil {
		return nil, err
	}
	result := &AccountDisconnectResult{AccountID: accountID, Scope: "instance_openai_gateway"}
	result.CancellationRequested = s.accountRequests.cancelAccount(accountID)
	result.ClosedUpstreamConnections = s.getOpenAIWSConnPool().AbortAccount(accountID)

	deadline := time.Now().Add(wait)
	for {
		result.Remaining = s.accountRequests.counts(accountID)
		result.RemainingUpstreamConnections = s.getOpenAIWSConnPool().AccountConnectionCount(accountID)
		result.CurrentConcurrency = nil
		if s.concurrencyService == nil || s.concurrencyService.cache == nil {
			return result, ErrAccountDisconnectUnverified
		}
		counts, countErr := s.concurrencyService.GetAccountConcurrencyBatch(ctx, []int64{accountID})
		if countErr != nil {
			return result, fmt.Errorf("%w: %w", ErrAccountDisconnectUnverified, countErr)
		}
		count, ok := counts[accountID]
		if !ok || count < 0 {
			return result, ErrAccountDisconnectUnverified
		}
		result.CurrentConcurrency = &count
		if result.Remaining.empty() && result.RemainingUpstreamConnections == 0 && count == 0 {
			// Concurrent explicit re-enablement is not a successful drain receipt.
			verifyCtx, cancelVerify := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			live, verifyErr := s.accountRepo.GetByID(verifyCtx, accountID)
			cancelVerify()
			if verifyErr != nil || live == nil {
				return result, ErrAccountDisconnectUnverified
			}
			result.Schedulable = live.Schedulable
			if live.Schedulable {
				return result, ErrAccountDisconnectUnverified
			}
			result.Drained = true
			return result, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			result.TimedOut = true
			return result, nil
		}
		timer := time.NewTimer(min(remaining, 25*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, nil
		case <-timer.C:
		}
	}
}
