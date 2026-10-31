CREATE SCHEMA IF NOT EXISTS notifier;

-- Inbox table for the idempotent consumer: an event is processed only if
-- its id can be inserted here in the same transaction as its side effects.
CREATE TABLE notifier.processed_events (
    event_id     uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifier.notifications (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id   uuid        NOT NULL,
    owner_id   text        NOT NULL,
    account_id uuid        NOT NULL,
    message    text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX notifications_owner_idx ON notifier.notifications (owner_id, id DESC);
