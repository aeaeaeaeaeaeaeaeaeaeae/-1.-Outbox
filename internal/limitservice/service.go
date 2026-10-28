package limitservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type LenderClient interface {
	BlockLimit(context.Context, BlockRequest) (BlockResult, error)
	LookupBlock(ctx context.Context, lenderID, operationID string) (BlockResult, error)
}

type ApplicationRepository interface {
	FindByID(context.Context, string) (Application, error)
	InsertBlockOperation(ctx context.Context, application Application) (LimitOperation, error)
	Outbox(ctx context.Context, limitOperation LimitOperation, msg KafkaMessage) (Application, error)
	MarkDeclined(ctx context.Context, applicationID, operationID string) error
}

type Service struct {
	Applications ApplicationRepository
	Lender       LenderClient
}

func NewService(application ApplicationRepository, lender LenderClient) *Service {
	return &Service{
		Applications: application,
		Lender:       lender,
	}
}

func kafkaEvent(app Application, op LimitOperation) (KafkaMessage, error) {
	eventID := uuid.NewString()

	payload, err := json.Marshal(limitBlockedPayload{
		EventID:       eventID,
		EventType:     "ApplicationLimitBlocked",
		OccurredAt:    time.Now().UTC().Format(time.RFC3339),
		ApplicationID: op.ApplicationID,
		ClientID:      app.ClientID,
		LenderID:      op.LenderID,
		LenderBlockID: op.BlockId,
		AmountMinor:   strconv.FormatInt(op.AmountMinor, 10),
		Currency:      op.Currency,
	})
	if err != nil {
		return KafkaMessage{}, fmt.Errorf("marshal event: %w", err)
	}

	return KafkaMessage{
		EventID: eventID,
		Topic:   "topic.name",
		Key:     op.ApplicationID,
		Payload: payload,
	}, nil
}

func (s *Service) BlockLenderLimit(ctx context.Context, applicationID string) (Application, error) {
	if _, err := uuid.Parse(applicationID); err != nil {
		return Application{}, fmt.Errorf("%w: application id %q: %v", ErrInvalidInput, applicationID, err)
	}

	application, err := s.Applications.FindByID(ctx, applicationID)
	if err != nil {
		return Application{}, err
	}

	status := application.Status
	if status == ScoringApproved {
		limitOperation, err := s.Applications.InsertBlockOperation(ctx, application)
		if err != nil {
			return Application{}, fmt.Errorf("error while insert block operation: %w", err)
		}

		if limitOperation.State == OperationDeclined {
			return Application{}, ErrLimitDeclined
		}

		if limitOperation.State == OperationConfirmed {
			return Application{}, fmt.Errorf("operation confirmed %s, but application still %s", limitOperation.BlockId, application.Status)
		}

		blockRes, err := s.Lender.BlockLimit(ctx, limitOperation.BlockRequest)
		if err != nil {
			return Application{}, fmt.Errorf("%w: %w", ErrBlockPending, err)
		}

		if blockRes.Outcome == BlockConfirmed && blockRes.BlockID != "" {
			limitOperation.BlockId = blockRes.BlockID

			kafkaMessage, err := kafkaEvent(application, limitOperation)
			if err != nil {
				return Application{}, fmt.Errorf("error while getting kafka message: %w", err)
			}

			app, err := s.Applications.Outbox(ctx, limitOperation, kafkaMessage)
			if err != nil {
				if errors.Is(err, ErrConflict) {
					currentApplication, currentErr := s.Applications.FindByID(ctx, applicationID)
					if currentErr != nil {
						return Application{}, currentErr
					}
					if currentApplication.Status == LimitBlocked || currentApplication.Status == ContractSigned {
						return currentApplication, nil
					}
					return Application{}, fmt.Errorf("%w: %s", ErrConflict, currentApplication.Status)
				}
				return Application{}, fmt.Errorf("error while outbox: %w", err)
			}

			return app, nil
		} else if blockRes.Outcome == BlockDeclined {
			if err := s.Applications.MarkDeclined(ctx, limitOperation.ApplicationID, limitOperation.OperationID); err != nil {
				return Application{}, fmt.Errorf("error while making declined: %w", err)
			}
			return application, ErrLimitDeclined
		}
		return application, fmt.Errorf("%w: outcome %q, block id %q", ErrBlockPending, blockRes.Outcome, blockRes.BlockID)
	} else if status == LimitBlocked || status == ContractSigned {
		return application, nil
	}
	return application, fmt.Errorf("%w : status %s", ErrInvalidStatus, application.Status)
}
