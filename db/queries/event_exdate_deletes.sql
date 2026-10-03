-- name: RecordEventExdateDelete :exec
-- Idempotent: deleting the same instance twice leaves exactly one log row.
INSERT INTO event_exdate_deletes (calendar_id, uid, recurrence_id)
VALUES (?, ?, ?)
ON CONFLICT(uid, recurrence_id) DO UPDATE SET
    deleted_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now');

-- name: ListEventExdateDeletesByCalendar :many
SELECT * FROM event_exdate_deletes
WHERE calendar_id = ?
ORDER BY deleted_at DESC;

-- name: GetEventExdateDelete :one
SELECT * FROM event_exdate_deletes WHERE id = ?;

-- name: GetEventExdateDeleteByUIDRecurrence :one
SELECT * FROM event_exdate_deletes WHERE uid = ? AND recurrence_id = ?;

-- name: DeleteEventExdateDelete :exec
DELETE FROM event_exdate_deletes WHERE id = ?;

-- name: DeleteEventExdateDeleteChecked :execrows
-- Same delete as DeleteEventExdateDelete, but it reports the number of rows
-- removed. A batch apply then fails when another operation consumed the log
-- row between the batch check and the batch apply.
DELETE FROM event_exdate_deletes WHERE id = ?;

-- name: PurgeOldEventExdateDeletes :execrows
DELETE FROM event_exdate_deletes WHERE deleted_at < ?;
