package internal

import "errors"

var (
	ErrNotFound       = errors.New("заявка не найдена")
	ErrConflict       = errors.New("состояние заявки изменилось")
	ErrNotImplemented = errors.New("реализуй метод")
)

type ApplicationStatus string

const (
	Initial         ApplicationStatus = "INITIAL"
	ScoringApproved ApplicationStatus = "SCORING_APPROVED"
	LimitBlocked    ApplicationStatus = "LIMIT_BLOCKED"
	ContractSigned  ApplicationStatus = "CONTRACT_SIGNED"
	Rejected        ApplicationStatus = "REJECTED"
)

type Application struct {
	ID                   string
	ClientID             string
	LenderID             string
	Status               ApplicationStatus
	RequestedAmountMinor int64
	Currency             string
	LenderBlockID        string
	Version              int64
}

type BlockOutcome string

const (
	BlockConfirmed BlockOutcome = "confirmed"
	BlockDeclined  BlockOutcome = "declined"
	BlockUnknown   BlockOutcome = "unknown"
)

type BlockRequest struct {
	OperationID   string
	ApplicationID string
	LenderID      string
	AmountMinor   int64
	Currency      string
}

type BlockResult struct {
	Outcome BlockOutcome
	BlockID string
}

type KafkaMessage struct {
	EventID string
	Topic   string
	Key     string
	Payload []byte
}
