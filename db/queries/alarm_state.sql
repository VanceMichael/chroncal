-- name: GetAlarmState :one
SELECT * FROM alarm_state WHERE alarm_id = ? AND trigger_at = ?;

-- Claim a fire slot for one alarm. The EXISTS arm reads the action in the
-- same statement as the insert, so a sync pull that rewrites the alarm to
-- a sync-only action between the check and the claim cannot leave a fired
-- state for an alarm the user disabled (issue #579). Zero rows means the
-- claim failed, and the caller reports sql.ErrNoRows.
-- The row enters in the "dispatching" state. It reaches "delivered" only
-- through CompleteAlarmDispatch. A failed dispatch moves to "retry", and a
-- process death leaves a stale "dispatching" row the next checker takes
-- over via TakeoverAlarmDispatch.
-- Keep the action list in lockstep with model.FireableAlarmAction.
-- name: CreateAlarmState :one
INSERT INTO alarm_state (
    alarm_id, event_id, trigger_at, fired_at,
    dispatch_status, claimed_at, claim_token, attempts
)
SELECT sqlc.arg(alarm_id), sqlc.arg(event_id), sqlc.arg(trigger_at), sqlc.arg(fired_at),
       'dispatching', sqlc.arg(claimed_at), sqlc.arg(claim_token), 1
WHERE EXISTS (
    SELECT 1 FROM event_alarms
    WHERE id = sqlc.arg(alarm_id) AND action IN ('AUDIO','DISPLAY','EMAIL')
)
RETURNING *;

-- name: AcknowledgeAlarmState :exec
UPDATE alarm_state SET acked_at = ? WHERE id = ?;

-- name: SnoozeAlarmState :exec
UPDATE alarm_state SET snoozed_to = ? WHERE id = ?;

-- Hide the state of an alarm a sync pull rewrote to a sync-only action.
-- The filter reads the current action, so the row comes back when a later
-- pull restores a fireable action (issue #579). A retirement that wrote
-- acked_at instead would consume the snooze of the user for good.
-- Keep the action list in lockstep with model.FireableAlarmAction.
-- name: ListPendingAlarmStates :many
SELECT s.* FROM alarm_state s
JOIN event_alarms a ON a.id = s.alarm_id
WHERE s.acked_at IS NULL AND s.fired_at IS NOT NULL
  AND a.action IN ('AUDIO','DISPLAY','EMAIL')
ORDER BY s.trigger_at;

-- name: GetAlarmStateByID :one
SELECT * FROM alarm_state WHERE id = ?;

-- The same sync-only filter as ListPendingAlarmStates, for the re-fire path.
-- Keep the action list in lockstep with model.FireableAlarmAction.
-- name: ListExpiredSnoozedAlarmStates :many
SELECT s.* FROM alarm_state s
JOIN event_alarms a ON a.id = s.alarm_id
WHERE s.fired_at IS NOT NULL
  AND s.acked_at IS NULL
  AND s.snoozed_to IS NOT NULL
  AND s.snoozed_to <= ?
  AND a.action IN ('AUDIO','DISPLAY','EMAIL')
ORDER BY s.snoozed_to;

-- Re-fire an expired snooze and start a new dispatch cycle. The UPDATE is
-- gated on snoozed_to IS NOT NULL, so it acts as an atomic claim between
-- overlapping checkers. The reset fields match a fresh claim: the cycle
-- starts at attempt 1 in "dispatching", with no retry or delivery record.
-- name: RefireAlarmState :execrows
UPDATE alarm_state
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

-- Take over a dispatch that another checker did not finish. The row is
-- eligible when it waits for a retry that is due, or when its dispatch
-- lease expired (the owner process died or hung). A live claim that is
-- inside its lease is not touched. Neither is a snoozed or an acknowledged
-- row. The predicate on claim_token keeps a caller from taking over its
-- own claim. RowsAffected == 0 means this caller lost the race or the row
-- is not due. The caller then dispatches nothing.
-- name: TakeoverAlarmDispatch :execrows
UPDATE alarm_state
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

-- Finish a dispatch. Only the claim token owner may complete the row, so a
-- stale process that lost its lease cannot overwrite a newer attempt.
-- RowsAffected == 0 means the ownership was lost. The notification may
-- already be sent. The caller must not write the row again.
-- name: CompleteAlarmDispatch :execrows
UPDATE alarm_state
SET dispatch_status = 'delivered',
    claim_token = NULL,
    delivered_at = sqlc.arg(delivered_at),
    retry_at = NULL,
    last_error = NULL
WHERE id = sqlc.arg(id)
  AND claim_token = sqlc.arg(claim_token)
  AND dispatch_status = 'dispatching';

-- Release a failed dispatch for a later retry. The same token gate as
-- CompleteAlarmDispatch applies. retry_at carries the deterministic
-- backoff; last_error records the cause for "alarm list".
-- name: RetryAlarmDispatch :execrows
UPDATE alarm_state
SET dispatch_status = 'retry',
    claim_token = NULL,
    retry_at = sqlc.arg(retry_at),
    last_error = sqlc.arg(last_error)
WHERE id = sqlc.arg(id)
  AND claim_token = sqlc.arg(claim_token)
  AND dispatch_status = 'dispatching';

-- name: ListAlarmStatesByEventID :many
SELECT * FROM alarm_state WHERE event_id = ? ORDER BY trigger_at;

-- name: DeleteAlarmStatesByEventID :exec
DELETE FROM alarm_state WHERE event_id = ?;

-- name: PurgeAcknowledgedAlarmStates :execrows
DELETE FROM alarm_state WHERE acked_at IS NOT NULL AND trigger_at < ?;

-- name: PurgeStaleUnacknowledgedAlarmStates :execrows
DELETE FROM alarm_state
WHERE acked_at IS NULL
  AND trigger_at < ?
  AND (snoozed_to IS NULL OR snoozed_to < trigger_at);
