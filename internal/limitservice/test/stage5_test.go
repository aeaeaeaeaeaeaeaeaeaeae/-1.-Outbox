package test

import (
	"awesomeProject7/internal/limitservice"
	"context"
	"errors"
	"testing"
	"time"
)

// Этап 5: вызовы зависимостей имеют ограниченное время ожидания.

// Кредитор завис. Вызов не висит бесконечно: возвращается за время таймаута
// с ErrBlockPending (это не отказ), операция остаётся PENDING.
func TestStage5_LenderHangs_BoundedByTimeout(t *testing.T) {
	initial := approvedApp()
	repo := newFakeRepo(initial)
	lender := &fakeLender{hang: true}
	const timeout = 50 * time.Millisecond
	svc := limitservice.NewService(timeout, repo, lender)

	// Входящий контекст без дедлайна — ограничивать должен сам сервис.
	start := time.Now()
	_, err := svc.BlockLenderLimit(context.Background(), appID)
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("вызов длился %v — ожидание не ограничено таймаутом %v", elapsed, timeout)
	}
	if !errors.Is(err, limitservice.ErrBlockPending) {
		t.Errorf("ошибка %v, ожидалась ErrBlockPending", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка %v, ожидалась причина context.DeadlineExceeded", err)
	}
	if errors.Is(err, limitservice.ErrLimitDeclined) {
		t.Errorf("таймаут выдан за отказ: %v", err)
	}
	expectNothingCommitted(t, repo, initial)
}

// Таймаут кредитора не ограничивает сохранение результата: если кредитор
// ответил почти в самый последний момент, транзакция Outbox всё равно
// получает исходный контекст, а не остаток времени кредитора.
func TestStage5_SlowLenderDoesNotStarveOutbox(t *testing.T) {
	repo := newFakeRepo(approvedApp())
	const timeout = 100 * time.Millisecond
	var outboxCtxErr error
	repo.onOutbox = func(ctx context.Context) {
		time.Sleep(timeout) // к этому моменту таймаут кредитора уже истёк бы
		outboxCtxErr = ctx.Err()
	}
	lender := &fakeLender{respond: func(_ int, res limitservice.BlockResult) (limitservice.BlockResult, error) {
		time.Sleep(timeout * 8 / 10) // ответ пришёл на 80% таймаута
		return res, nil
	}}
	svc := limitservice.NewService(timeout, repo, lender)

	got, err := svc.BlockLenderLimit(context.Background(), appID)
	if err != nil {
		t.Fatalf("неожиданная ошибка: %v", err)
	}
	if outboxCtxErr != nil {
		t.Errorf("контекст Outbox истёк (%v) — сохранение делит время с кредитором", outboxCtxErr)
	}
	if got.Status != limitservice.LimitBlocked {
		t.Errorf("статус %s, ожидался LIMIT_BLOCKED", got.Status)
	}
}
