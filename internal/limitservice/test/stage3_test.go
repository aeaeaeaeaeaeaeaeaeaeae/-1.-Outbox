package test

import (
	"awesomeProject7/internal/limitservice"
	"context"
	"errors"
	"testing"
)

// Этап 3: повторы и конкурентные вызовы не создают вторую блокировку и второе
// событие, а более поздний статус заявки не откатывается назад.

// Повтор после отказа: в банк второй раз не ходим, отказ окончательный.
func TestStage3_RetryAfterDeclined(t *testing.T) {
	initial := approvedApp()
	repo := newFakeRepo(initial)
	lender := &fakeLender{decide: func(limitservice.BlockRequest) limitservice.BlockResult {
		return limitservice.BlockResult{Outcome: limitservice.BlockDeclined}
	}}
	svc := limitservice.NewService(0, repo, lender)

	for i := 1; i <= 2; i++ {
		_, err := svc.BlockLenderLimit(context.Background(), appID)
		if !errors.Is(err, limitservice.ErrLimitDeclined) {
			t.Fatalf("вызов %d: ошибка %v, ожидалась ErrLimitDeclined", i, err)
		}
	}

	if lender.blockCalls != 1 {
		t.Errorf("вызовов BlockLimit: %d, ожидался 1 — повтор после отказа не должен идти в банк", lender.blockCalls)
	}
	if op := repo.ops[appID]; op.State != limitservice.OperationDeclined {
		t.Errorf("операция %s, ожидалась DECLINED", op.State)
	}
	if got := repo.apps[appID]; got != initial {
		t.Errorf("заявка изменилась: %+v", got)
	}
	if len(repo.events) != 0 {
		t.Errorf("событий: %d, ожидалось 0", len(repo.events))
	}
}

// Повтор после успеха: заявка возвращается как есть, банк и outbox не трогаются.
func TestStage3_RetryAfterSuccess(t *testing.T) {
	repo := newFakeRepo(approvedApp())
	lender := &fakeLender{}
	svc := limitservice.NewService(0, repo, lender)

	first, err := svc.BlockLenderLimit(context.Background(), appID)
	if err != nil {
		t.Fatalf("первый вызов: %v", err)
	}
	second, err := svc.BlockLenderLimit(context.Background(), appID)
	if err != nil {
		t.Fatalf("повтор: %v", err)
	}

	if second != first {
		t.Errorf("повтор вернул %+v, первый вызов %+v", second, first)
	}
	if lender.blockCalls != 1 {
		t.Errorf("вызовов BlockLimit: %d, ожидался 1", lender.blockCalls)
	}
	if repo.outboxCalls != 1 || len(repo.events) != 1 {
		t.Errorf("Outbox вызван %d раз, событий %d — ожидалось по 1", repo.outboxCalls, len(repo.events))
	}
}

// Повтор после перехода на следующий шаг: CONTRACT_SIGNED не откатывается.
func TestStage3_RetryAfterContractSigned(t *testing.T) {
	repo := newFakeRepo(approvedApp())
	lender := &fakeLender{}
	svc := limitservice.NewService(0, repo, lender)

	if _, err := svc.BlockLenderLimit(context.Background(), appID); err != nil {
		t.Fatalf("первый вызов: %v", err)
	}

	// Другой сервис перевёл заявку дальше.
	signed := repo.apps[appID]
	signed.Status = limitservice.ContractSigned
	signed.Version++
	repo.apps[appID] = signed

	got, err := svc.BlockLenderLimit(context.Background(), appID)
	if err != nil {
		t.Fatalf("повтор: %v", err)
	}
	if got != signed || repo.apps[appID] != signed {
		t.Errorf("заявка %+v, ожидалась без изменений %+v", repo.apps[appID], signed)
	}
	if lender.blockCalls != 1 || len(repo.events) != 1 {
		t.Errorf("BlockLimit=%d, событий=%d — ожидалось по 1", lender.blockCalls, len(repo.events))
	}
}

// Операция уже CONFIRMED, а заявка всё ещё SCORING_APPROVED. В текущей схеме
// это состояние недостижимо: Outbox меняет их одной транзакцией. Если оно
// появилось — это ошибка, а не повод идти в банк или сообщать об успехе.
func TestStage3_ConfirmedOperationButApplicationNotBlocked(t *testing.T) {
	initial := approvedApp()
	repo := newFakeRepo(initial)
	repo.ops[appID] = limitservice.LimitOperation{
		BlockRequest: limitservice.BlockRequest{
			OperationID:   "33333333-3333-3333-3333-333333333333",
			ApplicationID: appID,
			LenderID:      lenderID,
			AmountMinor:   initial.RequestedAmountMinor,
			Currency:      initial.Currency,
		},
		State:   limitservice.OperationConfirmed,
		BlockId: "blk-old",
	}
	lender := &fakeLender{}
	svc := limitservice.NewService(0, repo, lender)

	got, err := svc.BlockLenderLimit(context.Background(), appID)

	if err == nil {
		t.Fatalf("ожидалась ошибка, получен успех: %+v", got)
	}
	if errors.Is(err, limitservice.ErrLimitDeclined) || errors.Is(err, limitservice.ErrBlockPending) {
		t.Errorf("противоречие в данных выдано за отказ или неизвестный результат: %v", err)
	}
	if lender.blockCalls != 0 {
		t.Errorf("вызовов BlockLimit: %d, ожидалось 0", lender.blockCalls)
	}
	if got := repo.apps[appID]; got != initial {
		t.Errorf("заявка изменилась: %+v", got)
	}
	if len(repo.events) != 0 || repo.outboxCalls != 0 {
		t.Errorf("Outbox вызван %d раз, событий %d — ожидалось 0", repo.outboxCalls, len(repo.events))
	}
}

// Два вызова по одной заявке пересеклись: пока первый ждал ответа банка, второй
// прошёл весь путь и перевёл заявку в LIMIT_BLOCKED. Первый получает конфликт
// в Outbox, перечитывает заявку и возвращает её как успех.
func TestStage3_ConcurrentCallCompletedFirst(t *testing.T) {
	repo := newFakeRepo(approvedApp())
	lender := &fakeLender{}
	svc := limitservice.NewService(0, repo, lender)

	var second limitservice.Application
	var secondErr error
	lender.respond = func(call int, res limitservice.BlockResult) (limitservice.BlockResult, error) {
		if call == 1 {
			// Первый вызов ещё ждёт ответа — в это время приходит второй.
			second, secondErr = svc.BlockLenderLimit(context.Background(), appID)
		}
		return res, nil
	}

	first, err := svc.BlockLenderLimit(context.Background(), appID)

	if secondErr != nil {
		t.Fatalf("второй вызов: %v", secondErr)
	}
	if err != nil {
		t.Fatalf("первый вызов: %v (должен вернуть успех — блокировку уже завершил второй)", err)
	}

	stored := repo.apps[appID]
	if stored.Status != limitservice.LimitBlocked {
		t.Fatalf("статус в БД %s, ожидался LIMIT_BLOCKED", stored.Status)
	}
	// Оба вызова возвращают актуальную заявку из БД, а не прочитанную в начале.
	if first != stored {
		t.Errorf("первый вызов вернул %+v, в БД %+v", first, stored)
	}
	if second != stored {
		t.Errorf("второй вызов вернул %+v, в БД %+v", second, stored)
	}

	// Одна блокировка, один номер запроса, одно событие.
	if len(lender.requests) != 2 || lender.requests[0].OperationID != lender.requests[1].OperationID {
		t.Errorf("запросы к банку: %+v — ожидалось 2 с одним OperationID", lender.requests)
	}
	if lender.confirmedBlocks() != 1 {
		t.Errorf("блокировок у банка: %d, ожидалась 1", lender.confirmedBlocks())
	}
	if len(repo.events) != 1 {
		t.Errorf("событий: %d, ожидалось 1", len(repo.events))
	}
}
