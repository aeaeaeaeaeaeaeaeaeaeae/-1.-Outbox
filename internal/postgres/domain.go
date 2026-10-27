package postgres

import "errors"

var ErrorBeginTx = errors.New("error begin transaction")
var ErrorCommitTx = errors.New("error commit transaction")
var ErrorWhileUpdateApplication = errors.New("error while update application")
var ErrorWhileUpdateBlockOperation = errors.New("error while update block operation")
var ErrorWhileInsertInOutbox = errors.New("error while insert in outbox")
var BlockOperationNotPending = errors.New("block operation not pending")
