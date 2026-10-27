package limitservice

import (
	"context"
	"errors"
)

type fakeRepo struct {
	apps      map[string]Application
	findErr   error
	findCalls int
}

func newFakeRepo(apps ...Application) *fakeRepo {
	r := &fakeRepo{apps: map[string]Application{}}
	for _, a := range apps {
		r.apps[a.ID] = a
	}
	return r
}

func (r *fakeRepo) FindByID(_ context.Context, id string) (Application, error) {
	r.findCalls++
	if r.findErr != nil {
		return Application{}, r.findErr
	}
	a, ok := r.apps[id]
	if !ok {
		return Application{}, ErrNotFound
	}
	return a, nil
}

type fakeLender struct {
	blockCalls  int
	lookupCalls int
}

func (l *fakeLender) BlockLimit(context.Context, BlockRequest) (BlockResult, error) {
	l.blockCalls++
	return BlockResult{}, errors.New("fakeLender: BlockLimit не настроен")
}

func (l *fakeLender) LookupBlock(context.Context, string, string) (BlockResult, error) {
	l.lookupCalls++
	return BlockResult{}, errors.New("fakeLender: LookupBlock не настроен")
}
