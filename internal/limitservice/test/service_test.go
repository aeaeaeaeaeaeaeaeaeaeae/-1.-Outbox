package test

import (
	"awesomeProject7/internal/limitservice"
	"context"
	"errors"
	"testing"
)

func TestBlockLenderLimit_Stage1(t *testing.T) {
	const id = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	errDB := errors.New("db: connection refused")

	app := func(status limitservice.ApplicationStatus, blockID string) *limitservice.Application {
		return &limitservice.Application{
			ID:                   id,
			ClientID:             "11111111-1111-1111-1111-111111111111",
			LenderID:             "22222222-2222-2222-2222-222222222222",
			Status:               status,
			RequestedAmountMinor: 150_000_00,
			Currency:             "RUB",
			LenderBlockID:        blockID,
			Version:              1,
		}
	}

	tests := []struct {
		name     string
		inputID  string
		stored   *limitservice.Application
		findErr  error
		wantErr  error
		wantApp  *limitservice.Application
		wantFind int
	}{
		{name: "пустой id", inputID: "", wantErr: limitservice.ErrInvalidInput, wantFind: 0},
		{name: "id не uuid", inputID: "abc", wantErr: limitservice.ErrInvalidInput, wantFind: 0},
		{name: "id обрезан", inputID: id[:35], wantErr: limitservice.ErrInvalidInput, wantFind: 0},
		{name: "заявки нет", inputID: id, wantErr: limitservice.ErrNotFound, wantFind: 1},
		{name: "ошибка БД пробрасывается как есть", inputID: id, findErr: errDB, wantErr: errDB, wantFind: 1},
		{name: "INITIAL нельзя блокировать", inputID: id, stored: app(limitservice.Initial, ""), wantErr: limitservice.ErrInvalidStatus, wantApp: app(limitservice.Initial, ""), wantFind: 1},
		{name: "REJECTED нельзя блокировать", inputID: id, stored: app(limitservice.Rejected, ""), wantErr: limitservice.ErrInvalidStatus, wantApp: app(limitservice.Rejected, ""), wantFind: 1},
		{name: "повтор после LIMIT_BLOCKED", inputID: id, stored: app(limitservice.LimitBlocked, "blk-1"), wantApp: app(limitservice.LimitBlocked, "blk-1"), wantFind: 1},
		{name: "повтор после CONTRACT_SIGNED", inputID: id, stored: app(limitservice.ContractSigned, "blk-1"), wantApp: app(limitservice.ContractSigned, "blk-1"), wantFind: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newFakeRepo()
			if tt.stored != nil {
				repo.apps[tt.stored.ID] = *tt.stored
			}
			repo.findErr = tt.findErr
			lender := &fakeLender{}
			svc := limitservice.NewService(0, repo, lender)

			got, err := svc.BlockLenderLimit(context.Background(), tt.inputID)

			if tt.wantErr == nil && err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("ошибка %v, ожидалась %v", err, tt.wantErr)
			}

			if tt.findErr != nil && errors.Is(err, limitservice.ErrInvalidStatus) {
				t.Fatalf("ошибка БД превратилась в ErrInvalidStatus: %v", err)
			}

			want := limitservice.Application{}
			if tt.wantApp != nil {
				want = *tt.wantApp
			}
			if got != want {
				t.Errorf("заявка %+v, ожидалась %+v", got, want)
			}

			if repo.findCalls != tt.wantFind {
				t.Errorf("FindByID вызван %d раз, ожидалось %d", repo.findCalls, tt.wantFind)
			}

			if lender.blockCalls != 0 || lender.lookupCalls != 0 {
				t.Errorf("кредитор вызван: BlockLimit=%d, LookupBlock=%d", lender.blockCalls, lender.lookupCalls)
			}

			if repo.insertCalls != 0 || repo.outboxCalls != 0 || repo.declinedCalls != 0 {
				t.Errorf("репозиторий менял данные: InsertBlockOperation=%d, Outbox=%d, MarkDeclined=%d",
					repo.insertCalls, repo.outboxCalls, repo.declinedCalls)
			}

			if tt.stored != nil && repo.apps[id] != *tt.stored {
				t.Errorf("заявка в репозитории изменилась: %+v", repo.apps[id])
			}
		})
	}
}
