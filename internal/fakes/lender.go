// Package fakes содержит учебного кредитора с предсказуемым поведением,
// чтобы локально воспроизвести сценарии блокировки лимита.
package fakes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"awesomeProject7/internal/limitservice"
)

// Lender — учебный API кредитора. Поведение выбирается по последней цифре LenderID:
//
//	...1 -> блокирует сразу (confirmed)
//	...2 -> отказ (declined)
//	...3 -> блокирует, но ответ теряется (ошибка связи); повтор с тем же OperationID
//	        возвращает ту же блокировку, LookupBlock её находит
//	...4 -> первые 2 вызова недоступен (запрос до кредитора не доходит), потом блокирует
//	...5 -> зависает, пока вызывающий не перестанет ждать
//	...6 -> всегда отвечает unknown, ничего не делая
//	...7 -> отвечает confirmed без номера блокировки (нарушение контракта)
//	...8 -> блокирует, но отвечает через 2 секунды (для одновременных вызовов)
//
// Как и договорено, кредитор идемпотентен по OperationID: повтор не создаёт вторую блокировку.
// Состояние живёт в памяти процесса.
type Lender struct {
	mu      sync.Mutex
	calls   map[string]int                      // OperationID -> число вызовов BlockLimit
	results map[string]limitservice.BlockResult // OperationID -> выполненная блокировка
	seq     int
}

func NewLender() *Lender {
	return &Lender{calls: map[string]int{}, results: map[string]limitservice.BlockResult{}}
}

func (l *Lender) BlockLimit(ctx context.Context, req limitservice.BlockRequest) (limitservice.BlockResult, error) {
	l.mu.Lock()
	l.calls[req.OperationID]++
	call := l.calls[req.OperationID]
	l.mu.Unlock()

	log := slog.With("operation_id", req.OperationID, "application_id", req.ApplicationID, "call", call)

	switch lastChar(req.LenderID) {
	case '2':
		log.Info("fake lender: declined")
		return l.remember(req.OperationID, limitservice.BlockResult{Outcome: limitservice.BlockDeclined}), nil

	case '3':
		res, existed := l.confirm(req.OperationID)
		if existed {
			log.Info("fake lender: duplicate request, same block returned", "block_id", res.BlockID)
			return res, nil
		}
		log.Info("fake lender: blocked, but response lost", "block_id", res.BlockID)
		return limitservice.BlockResult{}, errors.New("fake lender: connection reset after request was processed")

	case '4':
		if call <= 2 {
			log.Info("fake lender: unavailable, request not processed")
			return limitservice.BlockResult{}, errors.New("fake lender: service unavailable")
		}

	case '5':
		log.Info("fake lender: hanging")
		<-ctx.Done()
		return limitservice.BlockResult{}, ctx.Err()

	case '6':
		log.Info("fake lender: unknown")
		return limitservice.BlockResult{Outcome: limitservice.BlockUnknown}, nil

	case '7':
		log.Info("fake lender: confirmed without block id")
		return limitservice.BlockResult{Outcome: limitservice.BlockConfirmed}, nil

	case '8':
		res, _ := l.confirm(req.OperationID)
		log.Info("fake lender: blocked, answering slowly", "block_id", res.BlockID)
		select {
		case <-time.After(2 * time.Second):
			return res, nil
		case <-ctx.Done():
			return limitservice.BlockResult{}, ctx.Err()
		}
	}

	res, existed := l.confirm(req.OperationID)
	if existed {
		log.Info("fake lender: duplicate request, same block returned", "block_id", res.BlockID)
	} else {
		log.Info("fake lender: blocked", "block_id", res.BlockID)
	}
	return res, nil
}

// LookupBlock отвечает по тому, что кредитор уже сделал: есть блокировка или отказ
// по этому OperationID — их результат, иначе unknown.
func (l *Lender) LookupBlock(_ context.Context, lenderID, operationID string) (limitservice.BlockResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	res, ok := l.results[operationID]
	if !ok {
		res = limitservice.BlockResult{Outcome: limitservice.BlockUnknown}
	}
	slog.Info("fake lender: lookup", "operation_id", operationID, "outcome", res.Outcome, "block_id", res.BlockID)
	return res, nil
}

func (l *Lender) confirm(operationID string) (limitservice.BlockResult, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if res, ok := l.results[operationID]; ok {
		return res, true
	}
	l.seq++
	res := limitservice.BlockResult{Outcome: limitservice.BlockConfirmed, BlockID: fmt.Sprintf("blk-%04d", l.seq)}
	l.results[operationID] = res
	return res, false
}

func (l *Lender) remember(operationID string, res limitservice.BlockResult) limitservice.BlockResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prev, ok := l.results[operationID]; ok {
		return prev
	}
	l.results[operationID] = res
	return res
}

func lastChar(s string) byte {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	return s[len(s)-1]
}
