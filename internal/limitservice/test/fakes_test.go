package test

import (
	"awesomeProject7/internal/limitservice"
	"context"
	"fmt"

	"github.com/google/uuid"
)

// fakeRepo — in-memory подмена ApplicationRepository. Повторяет поведение
// SQL-запросов из postgres.Repository, чтобы тесты сервиса проверяли ту же логику:
//   - InsertBlockOperation создаёт операцию один раз и всегда возвращает сохранённую;
//   - Outbox меняет заявку, операцию и добавляет событие только вместе
//     (как транзакция): при любой ошибке ничего не меняется.
type fakeRepo struct {
	apps   map[string]limitservice.Application
	ops    map[string]limitservice.LimitOperation // ключ — ID заявки
	events []limitservice.KafkaMessage

	findErr   error // если задано, FindByID возвращает эту ошибку (сбой БД)
	outboxErr error // если задано, Outbox возвращает эту ошибку и ничего не меняет
	// если задано, FindPendingOperations возвращает эту ошибку
	findPendingErr error

	findCalls        int
	insertCalls      int
	outboxCalls      int
	declinedCalls    int
	findPendingCalls int
}

// FindPendingOperations — как SELECT ... WHERE state = 'PENDING'. Возраст операции
// фейк не учитывает: в тестах все операции считаются «зависшими».
func (r *fakeRepo) FindPendingOperations(context.Context) ([]limitservice.LimitOperation, error) {
	r.findPendingCalls++
	if r.findPendingErr != nil {
		return nil, r.findPendingErr
	}
	var ops []limitservice.LimitOperation
	for _, op := range r.ops {
		if op.State == limitservice.OperationPending {
			ops = append(ops, op)
		}
	}
	return ops, nil
}

func newFakeRepo(apps ...limitservice.Application) *fakeRepo {
	r := &fakeRepo{
		apps: map[string]limitservice.Application{},
		ops:  map[string]limitservice.LimitOperation{},
	}
	for _, a := range apps {
		r.apps[a.ID] = a
	}
	return r
}

func (r *fakeRepo) FindByID(_ context.Context, id string) (limitservice.Application, error) {
	r.findCalls++
	if r.findErr != nil {
		return limitservice.Application{}, r.findErr
	}
	a, ok := r.apps[id]
	if !ok {
		return limitservice.Application{}, limitservice.ErrNotFound
	}
	return a, nil
}

// InsertBlockOperation — как INSERT ... ON CONFLICT DO NOTHING + SELECT.
func (r *fakeRepo) InsertBlockOperation(_ context.Context, app limitservice.Application) (limitservice.LimitOperation, error) {
	r.insertCalls++
	if _, ok := r.ops[app.ID]; !ok {
		r.ops[app.ID] = limitservice.LimitOperation{
			BlockRequest: limitservice.BlockRequest{
				OperationID:   uuid.NewString(),
				ApplicationID: app.ID,
				LenderID:      app.LenderID,
				AmountMinor:   app.RequestedAmountMinor,
				Currency:      app.Currency,
			},
			State: limitservice.OperationPending,
		}
	}
	return r.ops[app.ID], nil
}

// Outbox — как транзакция из трёх запросов: всё или ничего.
func (r *fakeRepo) Outbox(_ context.Context, op limitservice.LimitOperation, msg limitservice.KafkaMessage) (limitservice.Application, error) {
	r.outboxCalls++
	if r.outboxErr != nil {
		return limitservice.Application{}, r.outboxErr
	}

	app, ok := r.apps[op.ApplicationID]
	if !ok || app.Status != limitservice.ScoringApproved {
		return limitservice.Application{}, limitservice.ErrConflict
	}
	stored, ok := r.ops[op.ApplicationID]
	if !ok || stored.State != limitservice.OperationPending {
		return limitservice.Application{}, fmt.Errorf("block operation not pending: %w", limitservice.ErrConflict)
	}

	// «COMMIT»: все три изменения сразу.
	app.Status = limitservice.LimitBlocked
	app.LenderBlockID = op.BlockId
	app.Version++
	r.apps[app.ID] = app

	stored.State = limitservice.OperationConfirmed
	stored.BlockId = op.BlockId
	r.ops[app.ID] = stored

	r.events = append(r.events, msg)
	return app, nil
}

// MarkDeclined — как UPDATE ... WHERE state = 'PENDING'.
func (r *fakeRepo) MarkDeclined(_ context.Context, applicationID, operationID string) error {
	r.declinedCalls++
	op, ok := r.ops[applicationID]
	if ok && op.OperationID == operationID && op.State == limitservice.OperationPending {
		op.State = limitservice.OperationDeclined
		r.ops[applicationID] = op
	}
	return nil
}

// fakeLender — подмена API кредитора. Как и настоящий, он идемпотентен по
// OperationID: тот же номер запроса возвращает тот же результат и не создаёт
// вторую блокировку.
type fakeLender struct {
	results  map[string]limitservice.BlockResult // ключ — OperationID
	requests []limitservice.BlockRequest         // параметры каждого вызова BlockLimit

	// decide задаёт исход для нового OperationID. По умолчанию — confirmed.
	decide func(req limitservice.BlockRequest) limitservice.BlockResult
	// respond подменяет ответ ПОСЛЕ того, как кредитор выполнил действие:
	// так моделируются потерянный ответ, таймаут, нарушение контракта.
	respond func(call int, res limitservice.BlockResult) (limitservice.BlockResult, error)

	// unavailable — кредитор недоступен: BlockLimit возвращает эту ошибку,
	// НЕ выполнив действие (запрос до кредитора не дошёл).
	unavailable error
	lookupErr   error        // если задано, LookupBlock возвращает эту ошибку
	lookups     []lookupCall // параметры каждого вызова LookupBlock

	blockCalls  int
	lookupCalls int
}

func (l *fakeLender) BlockLimit(_ context.Context, req limitservice.BlockRequest) (limitservice.BlockResult, error) {
	l.blockCalls++
	l.requests = append(l.requests, req)
	if l.unavailable != nil {
		return limitservice.BlockResult{}, l.unavailable
	}
	if l.results == nil {
		l.results = map[string]limitservice.BlockResult{}
	}

	res, ok := l.results[req.OperationID]
	if !ok {
		if l.decide != nil {
			res = l.decide(req)
		} else {
			res = limitservice.BlockResult{Outcome: limitservice.BlockConfirmed, BlockID: fmt.Sprintf("blk-%d", len(l.results)+1)}
		}
		l.results[req.OperationID] = res
	}

	if l.respond != nil {
		return l.respond(l.blockCalls, res)
	}
	return res, nil
}

// LookupBlock отвечает по тому, что кредитор уже сделал: если блокировка с таким
// OperationID есть — её результат, если нет — unknown (запрос до кредитора не дошёл).
func (l *fakeLender) LookupBlock(_ context.Context, lenderID, operationID string) (limitservice.BlockResult, error) {
	l.lookupCalls++
	l.lookups = append(l.lookups, lookupCall{LenderID: lenderID, OperationID: operationID})
	if l.lookupErr != nil {
		return limitservice.BlockResult{}, l.lookupErr
	}
	if res, ok := l.results[operationID]; ok {
		return res, nil
	}
	return limitservice.BlockResult{Outcome: limitservice.BlockUnknown}, nil
}

type lookupCall struct{ LenderID, OperationID string }

// confirmedBlocks — сколько блокировок реально существует у кредитора.
func (l *fakeLender) confirmedBlocks() int {
	n := 0
	for _, r := range l.results {
		if r.Outcome == limitservice.BlockConfirmed && r.BlockID != "" {
			n++
		}
	}
	return n
}
