-- Таблица заявок. В реальном сервисе она уже существует; миграция нужна,
-- чтобы поднимать схему с нуля (локально и в тестах).
CREATE TABLE applications (
    id                     uuid   PRIMARY KEY,
    client_id              uuid   NOT NULL,
    lender_id              uuid   NOT NULL,
    status                 text   NOT NULL
        CHECK (status IN ('INITIAL', 'SCORING_APPROVED', 'LIMIT_BLOCKED', 'CONTRACT_SIGNED', 'REJECTED')),
    requested_amount_minor bigint NOT NULL CHECK (requested_amount_minor > 0),
    currency               text   NOT NULL CHECK (currency = 'RUB'),
    -- Номер блокировки у кредитора. Пустая строка — блокировки нет.
    -- NOT NULL DEFAULT '', чтобы Scan в Go-string не падал на NULL.
    lender_block_id        text   NOT NULL DEFAULT '',
    version                bigint NOT NULL CHECK (version > 0),
    -- В LIMIT_BLOCKED и дальше номер блокировки обязан быть.
    CONSTRAINT applications_blocked_has_block_id
        CHECK (status NOT IN ('LIMIT_BLOCKED', 'CONTRACT_SIGNED') OR lender_block_id <> '')
);
