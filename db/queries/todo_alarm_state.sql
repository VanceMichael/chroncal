-- name: GetTodoAlarmState :one
SELECT * FROM todo_alarm_state 
WHERE alarm_id = ? AND trigger_at = ?;

-- Claim a fire slot for one todo alarm. The EXISTS arm reads the action in
-- the same statement as the insert, so a sync pull that rewrites the alarm
-- to a sync-only action between the check and the claim cannot leave a
-- fired state for an alarm the user disabled (issue #579). Zero rows means
-- the claim failed, and the caller reports sql.ErrNoRows.
-- The row enters in the "dispatching" state. It reaches "delivered" only
-- through CompleteTodoAlarmDispatch. A failed dispatch moves to "retry",
-- and a process death leaves a stale "dispatching" row the next checker
-- takes over via TakeoverTodoAlarmDispatch.
-- Keep the action list in lockstep with model.FireableAlarmAction.
-- name: InsertTodoAlarmState :one
INSERT INTO todo_alarm_state (
    alarm_id, todo_id, trigger_at, fired_at, acked_at, snoozed_to,
    dispatch_status, claimed_at, claim_token, attempts
)
SELECT sqlc.arg(alarm_id), sqlc.arg(todo_id), sqlc.arg(trigger_at),
       sqlc.arg(fired_at), sqlc.arg(acked_at), sqlc.arg(snoozed_to),
       'dispatching', sqlc.arg(claimed_at), sqlc.arg(claim_token), 1
WHERE EXISTS (
    SELECT 1 FROM todo_alarms
    WHERE id = sqlc.arg(alarm_id) AND action IN ('AUDIO','DISPLAY','EMAIL')
)
RETURNING *;

-- name: AcknowledgeTodoAlarmState :exec
UPDATE todo_alarm_state SET acked_at = ? WHERE id = ?;

-- name: SnoozeTodoAlarmState :exec
UPDATE todo_alarm_state SET snoozed_to = ? WHERE id = ?;

-- name: ListTodoAlarmStates :many
SELECT * FROM todo_alarm_state 
WHERE todo_id = ? 
ORDER BY trigger_at DESC;

-- name: ListFiredTodoAlarmStates :many
SELECT * FROM todo_alarm_state 
WHERE todo_id = ? AND fired_at IS NOT NULL AND acked_at IS NULL AND snoozed_to IS NULL
ORDER BY fired_at DESC;

-- The same sync-only filter as ListPendingTodoAlarmStates, for the re-fire
-- path. Keep the action list in lockstep with model.FireableAlarmAction.
-- name: ListExpiredTodoSnoozed :many
SELECT s.* FROM todo_alarm_state s
JOIN todo_alarms a ON a.id = s.alarm_id
WHERE s.fired_at IS NOT NULL
  AND s.acked_at IS NULL
  AND s.snoozed_to IS NOT NULL
  AND s.snoozed_to <= ?
  AND a.action IN ('AUDIO','DISPLAY','EMAIL')
ORDER BY s.snoozed_to;

-- Re-fire an expired snooze and start a new dispatch cycle. The UPDATE is
-- gated on snoozed_to IS NOT NULL, so it acts as an atomic claim between
-- overlapping checkers. The reset fields match a fresh claim.
-- name: RefireTodoAlarmState :execrows
UPDATE todo_alarm_state
SET fired_at = sqlc.arg(fired_at),
    snoozed_to = NULL,
    dispatch_status = 'dispatching',
    claimed_at = sqlc.arg(claimed_at),
    claim_token = sqlc.arg(claim_token),
    attempts = 1,
    retry_at = NULL,
    last_error = NULL,
    delivered_at = NULL
WHERE id = sqlc.arg(id) AND snoozed_to IS NOT NULL;

-- Take over a todo-alarm dispatch that another checker did not finish.
-- The eligibility rules match TakeoverAlarmDispatch.
-- name: TakeoverTodoAlarmDispatch :execrows
UPDATE todo_alarm_state
SET dispatch_status = 'dispatching',
    claimed_at = sqlc.arg(claimed_at),
    claim_token = sqlc.arg(claim_token),
    attempts = attempts + 1,
    fired_at = sqlc.arg(fired_at),
    retry_at = NULL
WHERE id = sqlc.arg(id)
  AND acked_at IS NULL
  AND snoozed_to IS NULL
  AND (claim_token IS NULL OR claim_token <> sqlc.arg(claim_token))
  AND (
        (dispatch_status = 'retry' AND retry_at IS NOT NULL AND retry_at <= sqlc.arg(now))
     OR (dispatch_status = 'dispatching' AND (claimed_at IS NULL OR claimed_at <= sqlc.arg(lease_boundary)))
  );

-- Finish a todo-alarm dispatch. Only the claim token owner may complete it.
-- name: CompleteTodoAlarmDispatch :execrows
UPDATE todo_alarm_state
SET dispatch_status = 'delivered',
    claim_token = NULL,
    delivered_at = sqlc.arg(delivered_at),
    retry_at = NULL,
    last_error = NULL
WHERE id = sqlc.arg(id)
  AND claim_token = sqlc.arg(claim_token)
  AND dispatch_status = 'dispatching';

-- Release a failed todo-alarm dispatch for a later retry.
-- name: RetryTodoAlarmDispatch :execrows
UPDATE todo_alarm_state
SET dispatch_status = 'retry',
    claim_token = NULL,
    retry_at = sqlc.arg(retry_at),
    last_error = sqlc.arg(last_error)
WHERE id = sqlc.arg(id)
  AND claim_token = sqlc.arg(claim_token)
  AND dispatch_status = 'dispatching';

-- name: DeleteTodoAlarmState :exec
DELETE FROM todo_alarm_state WHERE id = ?;

-- name: CountTodoAlarmStates :one
SELECT COUNT(*) FROM todo_alarm_state WHERE todo_id = ?;

-- Hide the state of an alarm a sync pull rewrote to a sync-only action.
-- The filter reads the current action, so the row comes back when a later
-- pull restores a fireable action (issue #579).
-- Keep the action list in lockstep with model.FireableAlarmAction.
-- name: ListPendingTodoAlarmStates :many
SELECT s.* FROM todo_alarm_state s
JOIN todo_alarms a ON a.id = s.alarm_id
WHERE s.acked_at IS NULL AND (s.fired_at IS NOT NULL OR s.snoozed_to IS NOT NULL)
  AND a.action IN ('AUDIO','DISPLAY','EMAIL')
ORDER BY s.trigger_at;

-- name: GetTodoAlarmStateByID :one
SELECT * FROM todo_alarm_state WHERE id = ?;

-- name: PurgeAcknowledgedTodoAlarmStates :execrows
DELETE FROM todo_alarm_state WHERE acked_at IS NOT NULL AND trigger_at < ?;

-- name: PurgeStaleUnacknowledgedTodoAlarmStates :execrows
DELETE FROM todo_alarm_state
WHERE acked_at IS NULL
  AND trigger_at < ?
  AND (snoozed_to IS NULL OR snoozed_to < trigger_at);
