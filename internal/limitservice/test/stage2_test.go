package test

import (
	"awesomeProject7/internal/limitservice"
	"context"
	"encoding/json"
	"errors"
	"testing"
)

const (
	appID    = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	clientID = "11111111-1111-1111-1111-111111111111"
	lenderID = "22222222-2222-2222-2222-222222222222"
)

func approvedApp() limitservice.Application {
	return limitservice.Application{
		ID:                   appID,
		ClientID:             clientID,
		LenderID:             lenderID,
		Status:               limitservice.ScoringApproved,
		RequestedAmountMinor: 150_000_00,
		Currency:             "RUB",
		Version:              1,
	}
}

// expectNothingCommitted — заявка не изменилась, событий нет, операция PENDING.
// Общая проверка для всех исходов, кроме успеха и отказа.
func expectNothingCommitted(t *testing.T, repo *fakeRepo, initial limitservice.Application) {
	t.Helper()
	if got := repo.apps[appID]; got != initial {
		t.Errorf("заявка изменилась: %+v", got)
	}
	if len(repo.events) != 0 {
		t.Errorf("событий в outbox: %d, ожидалось 0", len(repo.events))
	}
	if op := repo.ops[appID]; op.State != limitservice.OperationPending || op.BlockId != "" {
		t.Errorf("операция: state=%s block_id=%q, ожидалось PENDING без block_id", op.State, op.BlockId)
	}
}

// Кредитор подтвердил: заявка, операция и событие сохранены вместе.
func TestStage2_Confirmed(t *testing.T) {
	initial := approvedApp()
	repo := newFakeRepo(initial)
	lender := &fakeLender{}
	svc := limitservice.NewService(repo, lender)

	got, err := svc.BlockLenderLimit(context.Background(), appID)
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}

	op := repo.ops[appID]

	// Кредитор вызван один раз, с параметрами из сохранённой операции.
	if len(lender.requests) != 1 {
		t.Fatalf("вызовов BlockLimit: %d, ожидался 1", len(lender.requests))
	}
	wantReq := limitservice.BlockRequest{
		OperationID:   op.OperationID,
		ApplicationID: appID,
		LenderID:      lenderID,
		AmountMinor:   150_000_00,
		Currency:      "RUB",
	}
	if lender.requests[0] != wantReq {
		t.Errorf("параметры BlockLimit %+v, ожидались %+v", lender.requests[0], wantReq)
	}
	if op.OperationID == "" {
		t.Error("OperationID не сохранён")
	}
	blockID := lender.results[op.OperationID].BlockID

	// Заявка.
	stored := repo.apps[appID]
	if stored.Status != limitservice.LimitBlocked || stored.LenderBlockID != blockID || stored.Version != initial.Version+1 {
		t.Errorf("заявка в БД: status=%s block=%q version=%d", stored.Status, stored.LenderBlockID, stored.Version)
	}
	if got != stored {
		t.Errorf("метод вернул %+v, в БД %+v", got, stored)
	}

	// Операция.
	if op.State != limitservice.OperationConfirmed || op.BlockId != blockID {
		t.Errorf("операция: state=%s block_id=%q, ожидалось CONFIRMED %q", op.State, op.BlockId, blockID)
	}

	// Событие.
	if len(repo.events) != 1 {
		t.Fatalf("событий в outbox: %d, ожидалось 1", len(repo.events))
	}
	ev := repo.events[0]
	if ev.EventID == "" || ev.Key != appID || ev.Topic == "" {
		t.Errorf("сообщение: %+v", ev)
	}
	var p map[string]string
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("payload не JSON: %v", err)
	}
	want := map[string]string{
		"event_id":        ev.EventID,
		"event_type":      "ApplicationLimitBlocked",
		"application_id":  appID,
		"client_id":       clientID,
		"lender_id":       lenderID,
		"lender_block_id": blockID,
		"amount_minor":    "15000000",
		"currency":        "RUB",
	}
	for k, v := range want {
		if p[k] != v {
			t.Errorf("payload[%q] = %q, ожидалось %q", k, p[k], v)
		}
	}

	// В БД ничего не отмечено как отказ.
	if repo.declinedCalls != 0 {
		t.Errorf("MarkDeclined вызван %d раз", repo.declinedCalls)
	}
}

// Кредитор отказал: заявка не становится LIMIT_BLOCKED, события нет.
func TestStage2_Declined(t *testing.T) {
	initial := approvedApp()
	repo := newFakeRepo(initial)
	lender := &fakeLender{decide: func(limitservice.BlockRequest) limitservice.BlockResult {
		return limitservice.BlockResult{Outcome: limitservice.BlockDeclined}
	}}
	svc := limitservice.NewService(repo, lender)

	_, err := svc.BlockLenderLimit(context.Background(), appID)
	if !errors.Is(err, limitservice.ErrLimitDeclined) {
		t.Fatalf("ошибка %v, ожидалась ErrLimitDeclined", err)
	}

	if got := repo.apps[appID]; got != initial {
		t.Errorf("заявка изменилась: %+v", got)
	}
	if op := repo.ops[appID]; op.State != limitservice.OperationDeclined || op.BlockId != "" {
		t.Errorf("операция: state=%s block_id=%q, ожидалось DECLINED", op.State, op.BlockId)
	}
	if len(repo.events) != 0 {
		t.Errorf("при отказе создано событий: %d", len(repo.events))
	}
	if repo.outboxCalls != 0 {
		t.Errorf("Outbox вызван %d раз", repo.outboxCalls)
	}
	if len(lender.requests) != 1 {
		t.Errorf("вызовов BlockLimit: %d, ожидался 1", len(lender.requests))
	}
}

// Ответ не даёт установить результат. Это НЕ отказ: заявка и операция не меняются,
// событие не создаётся, повтор безопасен.
func TestStage2_ResultNotEstablished(t *testing.T) {
	errTimeout := context.DeadlineExceeded

	tests := []struct {
		name    string
		respond func(int, limitservice.BlockResult) (limitservice.BlockResult, error)
	}{
		{"unknown", func(int, limitservice.BlockResult) (limitservice.BlockResult, error) {
			return limitservice.BlockResult{Outcome: limitservice.BlockUnknown}, nil
		}},
		{"confirmed без BlockID", func(int, limitservice.BlockResult) (limitservice.BlockResult, error) {
			return limitservice.BlockResult{Outcome: limitservice.BlockConfirmed}, nil
		}},
		{"неизвестный outcome", func(int, limitservice.BlockResult) (limitservice.BlockResult, error) {
			return limitservice.BlockResult{Outcome: "approved", BlockID: "blk-x"}, nil
		}},
		{"таймаут", func(int, limitservice.BlockResult) (limitservice.BlockResult, error) {
			return limitservice.BlockResult{}, errTimeout
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initial := approvedApp()
			repo := newFakeRepo(initial)
			lender := &fakeLender{respond: tt.respond}
			svc := limitservice.NewService(repo, lender)

			_, err := svc.BlockLenderLimit(context.Background(), appID)

			if err == nil {
				t.Fatal("ожидалась ошибка")
			}
			if errors.Is(err, limitservice.ErrLimitDeclined) {
				t.Fatalf("неустановленный результат выдан за отказ: %v", err)
			}
			if !errors.Is(err, limitservice.ErrBlockPending) {
				t.Errorf("ошибка %v, ожидалась ErrBlockPending", err)
			}
			expectNothingCommitted(t, repo, initial)
			if repo.outboxCalls != 0 || repo.declinedCalls != 0 {
				t.Errorf("Outbox=%d, MarkDeclined=%d, ожидалось 0", repo.outboxCalls, repo.declinedCalls)
			}
		})
	}
}

// Кредитор выполнил блокировку, но ответ потерялся. Повтор идёт с ТЕМ ЖЕ
// OperationID, вторая блокировка не создаётся, событие одно.
func TestStage2_RetryAfterLostResponse(t *testing.T) {
	initial := approvedApp()
	repo := newFakeRepo(initial)
	lender := &fakeLender{respond: func(call int, res limitservice.BlockResult) (limitservice.BlockResult, error) {
		if call == 1 {
			return limitservice.BlockResult{}, errors.New("read tcp: connection reset by peer")
		}
		return res, nil
	}}
	svc := limitservice.NewService(repo, lender)

	if _, err := svc.BlockLenderLimit(context.Background(), appID); err == nil {
		t.Fatal("первый вызов: ожидалась ошибка")
	}
	expectNothingCommitted(t, repo, initial)

	got, err := svc.BlockLenderLimit(context.Background(), appID)
	if err != nil {
		t.Fatalf("повтор: %v", err)
	}

	if len(lender.requests) != 2 {
		t.Fatalf("вызовов BlockLimit: %d, ожидалось 2", len(lender.requests))
	}
	if lender.requests[0] != lender.requests[1] {
		t.Errorf("повтор с другими параметрами:\n1: %+v\n2: %+v", lender.requests[0], lender.requests[1])
	}
	if lender.confirmedBlocks() != 1 {
		t.Errorf("блокировок у кредитора: %d, ожидалась 1", lender.confirmedBlocks())
	}
	if got.Status != limitservice.LimitBlocked || len(repo.events) != 1 {
		t.Errorf("итог: status=%s, событий %d", got.Status, len(repo.events))
	}
}

// Пока ждали кредитора, заявку отклонили. Статус не возвращается назад,
// событие об успехе не создаётся.
func TestStage2_ApplicationRejectedWhileWaiting(t *testing.T) {
	repo := newFakeRepo(approvedApp())
	lender := &fakeLender{respond: func(_ int, res limitservice.BlockResult) (limitservice.BlockResult, error) {
		a := repo.apps[appID]
		a.Status = limitservice.Rejected
		a.Version++
		repo.apps[appID] = a
		return res, nil
	}}
	svc := limitservice.NewService(repo, lender)

	_, err := svc.BlockLenderLimit(context.Background(), appID)
	if !errors.Is(err, limitservice.ErrConflict) {
		t.Fatalf("ошибка %v, ожидалась ErrConflict", err)
	}
	if st := repo.apps[appID].Status; st != limitservice.Rejected {
		t.Errorf("статус %s, ожидался REJECTED", st)
	}
	if len(repo.events) != 0 {
		t.Errorf("событий: %d, ожидалось 0", len(repo.events))
	}
}

// Транзакция Outbox упала: ни статус, ни событие не сохранены по отдельности.
func TestStage2_OutboxFailure(t *testing.T) {
	initial := approvedApp()
	repo := newFakeRepo(initial)
	errDB := errors.New("db: connection lost")
	repo.outboxErr = errDB
	lender := &fakeLender{}
	svc := limitservice.NewService(repo, lender)

	_, err := svc.BlockLenderLimit(context.Background(), appID)
	if !errors.Is(err, errDB) {
		t.Fatalf("ошибка %v, ожидалась %v", err, errDB)
	}
	if errors.Is(err, limitservice.ErrLimitDeclined) {
		t.Fatalf("сбой БД выдан за отказ: %v", err)
	}
	expectNothingCommitted(t, repo, initial)
	if lender.confirmedBlocks() != 1 {
		t.Errorf("у кредитора блокировка должна существовать, есть %d", lender.confirmedBlocks())
	}
}
