package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type schedulingAdmissionRepo struct {
	AccountRepository
	account *Account
	err     error
}

func (r *schedulingAdmissionRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, r.err
}

type schedulingAdmissionReader struct {
	AccountRepository
	enabled bool
	err     error
}

func (r *schedulingAdmissionReader) IsAccountSchedulingEnabled(context.Context, int64) (bool, error) {
	return r.enabled, r.err
}

func TestCheckAccountScheduling(t *testing.T) {
	dbErr := errors.New("database unavailable")
	for _, tc := range []struct {
		name         string
		repo         AccountRepository
		wantErr      error
		wantAnyError bool
	}{
		{name: "enabled", repo: &schedulingAdmissionRepo{account: &Account{ID: 5579, Status: StatusActive, Schedulable: true}}},
		{name: "disabled", repo: &schedulingAdmissionRepo{account: &Account{ID: 5579, Status: StatusActive, Schedulable: false}}, wantErr: ErrAccountSchedulingDisabled},
		{name: "paused", repo: &schedulingAdmissionRepo{account: &Account{ID: 5579, Status: StatusDisabled, Schedulable: true}}, wantErr: ErrAccountSchedulingDisabled},
		{name: "missing", repo: &schedulingAdmissionRepo{}, wantErr: ErrAccountSchedulingDisabled},
		{name: "deleted", repo: &schedulingAdmissionRepo{err: ErrAccountNotFound}, wantErr: ErrAccountSchedulingDisabled},
		{name: "wrong_identity", repo: &schedulingAdmissionRepo{account: &Account{ID: 1, Status: StatusActive, Schedulable: true}}, wantErr: ErrAccountSchedulingDisabled},
		{name: "read_failure", repo: &schedulingAdmissionRepo{err: dbErr}, wantErr: dbErr},
		{name: "missing_repository", wantAnyError: true},
		{name: "lightweight_enabled", repo: &schedulingAdmissionReader{enabled: true}},
		{name: "lightweight_disabled", repo: &schedulingAdmissionReader{}, wantErr: ErrAccountSchedulingDisabled},
		{name: "lightweight_read_failure", repo: &schedulingAdmissionReader{enabled: true, err: dbErr}, wantErr: dbErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &OpenAIGatewayService{accountRepo: tc.repo}
			err := svc.CheckAccountScheduling(context.Background(), 5579)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else if tc.wantAnyError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
