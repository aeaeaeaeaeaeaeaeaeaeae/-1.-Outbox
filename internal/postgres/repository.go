package postgres

import (
	"awesomeProject7/internal/limitservice"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var _ limitservice.ApplicationRepository = (*Repository)(nil)

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

func (r *Repository) FindByID(ctx context.Context, id string) (limitservice.Application, error) {
	const query = `SELECT id, client_id, lender_id, status, requested_amount_minor, currency, lender_block_id, version FROM applications WHERE id = $1`

	ctx, cancel := context.WithTimeout(ctx, r.dbTimeout)
	defer cancel()

	var a limitservice.Application
	err := r.db.QueryRowContext(ctx, query, id).Scan(&a.ID, &a.ClientID, &a.LenderID, &a.Status, &a.RequestedAmountMinor, &a.Currency, &a.LenderBlockID, &a.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return limitservice.Application{}, limitservice.ErrNotFound
	}

	if err != nil {
		return limitservice.Application{}, fmt.Errorf("%s: %w", id, err)
	}
	return a, nil
}

var InsertBlockError = errors.New("insert block operation error")

func (r *Repository) InsertBlockOperation(ctx context.Context, application limitservice.Application) (limitservice.LimitOperation, error) {
	const query = `INSERT INTO limit_block_operation (application_id, operation_id, lender_id, amount_minor, currency, state)
	VALUES ($1, $2, $3, $4, $5, 'PENDING') ON CONFLICT (application_id) DO NOTHING`

	const id_query = `SELECT operation_id, application_id, lender_id, amount_minor, currency, state, block_id FROM limit_block_operation WHERE application_id = $1`

	ctx, cancel := context.WithTimeout(ctx, r.dbTimeout)
	defer cancel()

	var id string
	id = uuid.NewString()
	_, err := r.db.ExecContext(ctx, query, application.ID, id, application.LenderID, application.RequestedAmountMinor, application.Currency)
	if err != nil {
		return limitservice.LimitOperation{}, fmt.Errorf("%w: %w", InsertBlockError, err)
	}

	var limitOperation limitservice.LimitOperation
	err = r.db.QueryRowContext(ctx, id_query, application.ID).Scan(&limitOperation.OperationID,
		&limitOperation.ApplicationID,
		&limitOperation.LenderID,
		&limitOperation.AmountMinor,
		&limitOperation.Currency,
		&limitOperation.State,
		&limitOperation.BlockId,
	)
	if err != nil {
		return limitservice.LimitOperation{}, fmt.Errorf("error read an application: %w", err)
	}

	return limitOperation, nil
}

func (r *Repository) Outbox(ctx context.Context, limitOperation limitservice.LimitOperation, msg limitservice.KafkaMessage) (limitservice.Application, error) {
	const update_application_query = `UPDATE applications SET status = 'LIMIT_BLOCKED', lender_block_id = $2, version = version + 1 WHERE id = $1 AND status = 'SCORING_APPROVED'
									  RETURNING id, client_id, lender_id, status, requested_amount_minor, currency, lender_block_id, version`
	const update_limit_block_operation = `UPDATE limit_block_operation SET state = 'CONFIRMED', block_id = $2 WHERE application_id = $1 AND state = 'PENDING'`
	const outbox_events = `INSERT INTO outbox_events (event_id, topic, msg_key, payload) VALUES ($1, $2, $3, $4)`

	ctx, cancel := context.WithTimeout(ctx, r.dbTimeout)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return limitservice.Application{}, fmt.Errorf("%w : %w", ErrorBeginTx, err)
	}
	defer tx.Rollback()

	var app limitservice.Application
	if err = tx.QueryRowContext(ctx, update_application_query, limitOperation.ApplicationID, limitOperation.BlockId).
		Scan(&app.ID, &app.ClientID, &app.LenderID, &app.Status, &app.RequestedAmountMinor, &app.Currency, &app.LenderBlockID, &app.Version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return limitservice.Application{}, limitservice.ErrConflict
		}
		return limitservice.Application{}, fmt.Errorf("%w: %w", ErrorWhileUpdateApplication, err)
	}

	res, err := tx.ExecContext(ctx, update_limit_block_operation, limitOperation.ApplicationID, limitOperation.BlockId)
	if err != nil {
		return limitservice.Application{}, fmt.Errorf("%w: %w", ErrorWhileUpdateBlockOperation, err)
	}

	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return limitservice.Application{}, fmt.Errorf("%w: %w", BlockOperationNotPending, limitservice.ErrConflict)
	}

	if _, err := tx.ExecContext(ctx, outbox_events, msg.EventID, msg.Topic, msg.Key, msg.Payload); err != nil {
		return limitservice.Application{}, fmt.Errorf("%w: %w", ErrorWhileInsertInOutbox, err)
	}

	err = tx.Commit()
	if err != nil {
		return limitservice.Application{}, fmt.Errorf("%w: %w", ErrorCommitTx, err)
	}

	return app, nil
}

func (r *Repository) MarkDeclined(ctx context.Context, applicationId, operationId string) error {
	const query = `UPDATE limit_block_operation SET state = 'DECLINED' WHERE application_id = $1 AND operation_id = $2 AND state = 'PENDING'`

	ctx, cancel := context.WithTimeout(ctx, r.dbTimeout)
	defer cancel()

	if _, err := r.db.ExecContext(ctx, query, applicationId, operationId); err != nil {
		return fmt.Errorf("error while update block operation: %w", err)
	}
	return nil
}

func (r *Repository) FindPendingOperations(ctx context.Context) ([]limitservice.LimitOperation, error) {
	const query = `
	SELECT operation_id, application_id, lender_id, amount_minor, currency, state, block_id
	FROM limit_block_operation 
	WHERE state = 'PENDING' AND updated_at < now() - make_interval(secs => 120)
	ORDER BY updated_at
	LIMIT 100
	`

	ctx, cancel := context.WithTimeout(ctx, r.dbTimeout)
	defer cancel()

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	operations := make([]limitservice.LimitOperation, 0)
	for rows.Next() {
		var op limitservice.LimitOperation
		if err := rows.Scan(&op.OperationID, &op.ApplicationID, &op.LenderID, &op.AmountMinor, &op.Currency, &op.State, &op.BlockId); err != nil {
			return nil, err
		}
		operations = append(operations, op)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error while iterating: %w", err)
	}

	return operations, nil
}
