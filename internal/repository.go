package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var _ ApplicationRepository = (*Repository)(nil)

type Repository struct {
	db        *sql.DB
	dbTimeout time.Duration
}

func NewRepository(db *sql.DB, dbTimeout time.Duration) (*Repository, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	if dbTimeout <= 0 {
		dbTimeout = time.Second * 3
	}

	return &Repository{
		db:        db,
		dbTimeout: dbTimeout,
	}, nil
}

func (r *Repository) FindByID(ctx context.Context, id string) (Application, error) {
	const query = `
	SELECT id, client_id, lender_id, status, requested_amount_minor, currency, lender_block_id, version 
	FROM applications WHERE id = $1`

	ctx, cancel := context.WithTimeout(ctx, r.dbTimeout)
	defer cancel()

	var a Application
	var status string
	err := r.db.QueryRowContext(ctx, query, id).Scan(&a.ID, &a.ClientID, &a.LenderID, &status, &a.RequestedAmountMinor, &a.Currency, &a.LenderBlockID, &a.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return Application{}, ErrNotFound
	}

	if err != nil {
		return Application{}, fmt.Errorf("%s: %w", id, err)
	}
	a.Status = ApplicationStatus(status)
	return a, nil
}
