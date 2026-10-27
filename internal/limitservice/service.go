package limitservice

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type LenderClient interface {
	BlockLimit(context.Context, BlockRequest) (BlockResult, error)
	LookupBlock(ctx context.Context, lenderID, operationID string) (BlockResult, error)
}

type ApplicationRepository interface {
	FindByID(context.Context, string) (Application, error)
	// TODO: добавь операции, необходимые для выбранного протокола хранения.
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

var ErrInvalidStatus = errors.New("status is incorrect")
var ErrInvalidInput = errors.New("invalid input")

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
		// тут блокировка
		return Application{}, ErrNotImplemented
	} else if status == LimitBlocked || status == ContractSigned {
		return application, nil
	} else {
		return application, fmt.Errorf("%w : status %s", ErrInvalidStatus, application.Status)
	}
}
