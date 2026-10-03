-- +goose Up

-- Dispatch lifecycle for event alarms. A state row is claimed before the
-- notification is dispatched. It reaches "delivered" only after a backend
-- completes. A failed dispatch keeps "retry" with retry_at. A process that
-- exits between claim and completion leaves "dispatching" with a stale
-- claimed_at. A later checker takes that row over when the lease expires.
ALTER TABLE alarm_state ADD COLUMN dispatch_status TEXT NOT NULL DEFAULT 'delivered'
    CHECK (dispatch_status IN ('dispatching','retry','delivered'));
ALTER TABLE alarm_state ADD COLUMN claimed_at   TEXT;
ALTER TABLE alarm_state ADD COLUMN claim_token  TEXT;
ALTER TABLE alarm_state ADD COLUMN attempts     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE alarm_state ADD COLUMN retry_at     TEXT;
ALTER TABLE alarm_state ADD COLUMN last_error   TEXT;
ALTER TABLE alarm_state ADD COLUMN delivered_at TEXT;

-- Rows written before the lifecycle existed already completed their
-- dispatch under the old mark-then-fire contract. Mark them delivered so
-- an upgrade does not re-notify old reminders.
UPDATE alarm_state SET delivered_at = fired_at WHERE fired_at IS NOT NULL;

CREATE INDEX idx_alarm_state_dispatch_retry ON alarm_state(dispatch_status, retry_at);

-- Same lifecycle for todo alarms.
ALTER TABLE todo_alarm_state ADD COLUMN dispatch_status TEXT NOT NULL DEFAULT 'delivered'
    CHECK (dispatch_status IN ('dispatching','retry','delivered'));
ALTER TABLE todo_alarm_state ADD COLUMN claimed_at   TEXT;
ALTER TABLE todo_alarm_state ADD COLUMN claim_token  TEXT;
ALTER TABLE todo_alarm_state ADD COLUMN attempts     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE todo_alarm_state ADD COLUMN retry_at     TEXT;
ALTER TABLE todo_alarm_state ADD COLUMN last_error   TEXT;
ALTER TABLE todo_alarm_state ADD COLUMN delivered_at TEXT;

UPDATE todo_alarm_state SET delivered_at = fired_at WHERE fired_at IS NOT NULL;

CREATE INDEX idx_todo_alarm_state_dispatch_retry ON todo_alarm_state(dispatch_status, retry_at);

-- +goose Down

DROP INDEX IF EXISTS idx_alarm_state_dispatch_retry;
ALTER TABLE alarm_state DROP COLUMN delivered_at;
ALTER TABLE alarm_state DROP COLUMN last_error;
ALTER TABLE alarm_state DROP COLUMN retry_at;
ALTER TABLE alarm_state DROP COLUMN attempts;
ALTER TABLE alarm_state DROP COLUMN claim_token;
ALTER TABLE alarm_state DROP COLUMN claimed_at;
ALTER TABLE alarm_state DROP COLUMN dispatch_status;

DROP INDEX IF EXISTS idx_todo_alarm_state_dispatch_retry;
ALTER TABLE todo_alarm_state DROP COLUMN delivered_at;
ALTER TABLE todo_alarm_state DROP COLUMN last_error;
ALTER TABLE todo_alarm_state DROP COLUMN retry_at;
ALTER TABLE todo_alarm_state DROP COLUMN attempts;
ALTER TABLE todo_alarm_state DROP COLUMN claim_token;
ALTER TABLE todo_alarm_state DROP COLUMN claimed_at;
ALTER TABLE todo_alarm_state DROP COLUMN dispatch_status;
