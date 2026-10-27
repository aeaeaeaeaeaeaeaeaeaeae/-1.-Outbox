package test

import (
	"awesomeProject7/internal/limitservice"
	"context"
	"errors"
)

type fakeRepo struct {
	apps      map[string]limitservice.Application
	findErr   error
	findCalls int
}

func newFakeRepo(apps ...limitservice.Application) *fakeRepo {
	r := &fakeRepo{apps: map[string]limitservice.Application{}}
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

type fakeLender struct {
	blockCalls  int
	lookupCalls int
}

func (l *fakeLender) BlockLimit(context.Context, limitservice.BlockRequest) (limitservice.BlockResult, error) {
	l.blockCalls++
	return limitservice.BlockResult{}, errors.New("fakeLender: BlockLimit не настроен")
}

func (l *fakeLender) LookupBlock(context.Context, string, string) (limitservice.BlockResult, error) {
	l.lookupCalls++
	return limitservice.BlockResult{}, errors.New("fakeLender: LookupBlock не настроен")
}
