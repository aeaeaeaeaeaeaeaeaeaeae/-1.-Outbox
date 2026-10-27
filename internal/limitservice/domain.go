package limitservice

import (
	"errors"
)

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

type LimitOperation struct {
	BlockRequest
	State   OperationState
	BlockId string
}

type OperationState string

const (
	OperationPending   OperationState = "PENDING"
	OperationConfirmed OperationState = "CONFIRMED"
	OperationDeclined  OperationState = "DECLINED"
)

var ErrInvalidStatus = errors.New("status is incorrect")
var ErrInvalidInput = errors.New("invalid input")
var ErrLimitDeclined = errors.New("lender declined block")
var ErrBlockPending = errors.New("block result not established")

type BlockResult struct {
	Outcome BlockOutcome
	BlockID string
}

type limitBlockedPayload struct {
	EventID       string `json:"event_id"`
	EventType     string `json:"event_type"`
	OccurredAt    string `json:"occurred_at"`
	ApplicationID string `json:"application_id"`
	ClientID      string `json:"client_id"`
	LenderID      string `json:"lender_id"`
	LenderBlockID string `json:"lender_block_id"`
	AmountMinor   string `json:"amount_minor"`
	Currency      string `json:"currency"`
}

type KafkaMessage struct {
	EventID string
	Topic   string
	Key     string
	Payload []byte
}
