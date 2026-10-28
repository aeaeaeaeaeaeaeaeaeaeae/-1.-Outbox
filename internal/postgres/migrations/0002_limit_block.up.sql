-- Операция блокировки лимита: одна на заявку (PRIMARY KEY application_id).
-- operation_id — ключ идемпотентности у кредитора. Сохраняется ДО вызова
-- кредитора и переиспользуется при повторах и восстановлении.
-- lender_id, amount_minor, currency фиксируются при создании: при повторе
-- кредитору уходят ровно те же параметры.
CREATE TABLE limit_block_operation (
    application_id uuid        PRIMARY KEY REFERENCES applications (id),
    operation_id   uuid        NOT NULL UNIQUE,
    lender_id      uuid        NOT NULL,
    amount_minor   bigint      NOT NULL CHECK (amount_minor > 0),
    currency       text        NOT NULL CHECK (currency = 'RUB'),
    state          text        NOT NULL CHECK (state IN ('PENDING', 'CONFIRMED', 'DECLINED')),
    -- Номер блокировки от кредитора. Пустая строка — блокировки нет.
    block_id       text        NOT NULL DEFAULT '',
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    -- block_id заполнен тогда и только тогда, когда операция CONFIRMED.
    CONSTRAINT limit_block_operation_block_id_iff_confirmed
        CHECK ((state = 'CONFIRMED') = (block_id <> ''))
);

-- Поиск зависших операций для восстановления (FindPendingOperations).
CREATE INDEX limit_block_operation_pending_idx
    ON limit_block_operation (updated_at)
    WHERE state = 'PENDING';

-- Transactional outbox: событие вставляется в той же транзакции, что и
-- переход заявки в LIMIT_BLOCKED. Публикатор читает строки с published_at IS NULL.
-- event_id — постоянный ID события, не меняется при повторной публикации.
CREATE TABLE outbox_events (
    event_id     uuid        PRIMARY KEY,
    topic        text        NOT NULL,
    msg_key      text        NOT NULL,
    payload      json        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_events_unpublished_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;
