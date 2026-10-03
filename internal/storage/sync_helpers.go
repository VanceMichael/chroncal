package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// MarkResourceDirty marks a resource as dirty for sync. If the calendar is
// linked to an account (synced), this upserts a sync_resources row with
// dirty=1. For local-only calendars this is a no-op.
// Called by service-layer mutations (Create, Update, ReplaceAlarms, etc.).
func MarkResourceDirty(ctx context.Context, db DBTX, calendarID int64, uid, ownerType string) error {
	if calendarID == 0 || uid == "" {
		return nil
	}
	// Only act if the calendar is linked to an account.
	var accountID *int64
	err := db.QueryRowContext(ctx,
		`SELECT account_id FROM calendars WHERE id = ?`, calendarID,
	).Scan(&accountID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if accountID == nil || *accountID == 0 {
		return nil
	}
	// Bump rev on every edit so a concurrent push (which captured the prior
	// rev before exporting the body) refuses to clear dirty and silently drop
	// this edit. See FinalizePushedResource and issue #92.
	_, err = db.ExecContext(ctx,
		`INSERT INTO sync_resources (calendar_id, uid, owner_type, dirty, sync_strategy)
		 VALUES (?, ?, ?, 1, 'sync-token')
		 ON CONFLICT(calendar_id, uid) DO UPDATE SET dirty = 1, rev = rev + 1`,
		calendarID, uid, ownerType,
	)
	return err
}

// MoveResourceOwnership transfers the sync intents for one UID across a
// cross-calendar move. The caller moves the local rows in the same
// transaction. db is the transaction handle: a failure here rolls the row
// move back, and a failed row write rolls these intent writes back. The
// move then never commits with only half of its sync intent.
//
// Source calendar with a pushed remote identity (non-empty remote_url):
// record a tombstone for the source DELETE and clear the source dirty flag.
// The move is a delete intent, not an edit to push. The source
// sync_resources row stays in place with its remote_url and etag until the
// DELETE confirms: processTombstones sends the conditional DELETE, keeps the
// 412 conflict protection, and converges on 404/410.
//
// Source calendar with no pushed identity (empty remote_url): drop the
// source row. A never-pushed placeholder must not create the resource on
// the source server after the item leaves.
//
// No source row: nothing to delete remotely.
//
// The destination calendar always gets a dirty resource (a create when it
// has no remote identity, an update when it has one). MarkResourceDirty is a
// no-op for a local-only destination calendar.
//
// The source tombstone and the destination dirty row are independent. They
// live on different calendars. One side succeeding never clears the other
// side.
func MoveResourceOwnership(ctx context.Context, db DBTX, srcCalendarID, dstCalendarID int64, uid, ownerType string) error {
	if uid == "" || srcCalendarID == dstCalendarID {
		// No move: keep the plain dirty-mark contract.
		return MarkResourceDirty(ctx, db, dstCalendarID, uid, ownerType)
	}

	var remoteURL string
	err := db.QueryRowContext(ctx,
		`SELECT remote_url FROM sync_resources WHERE calendar_id = ? AND uid = ?`,
		srcCalendarID, uid,
	).Scan(&remoteURL)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Nothing tracked on the source. Only the destination intent remains.
		return MarkResourceDirty(ctx, db, dstCalendarID, uid, ownerType)
	case err != nil:
		return fmt.Errorf("read source sync resource: %w", err)
	}

	if remoteURL != "" {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO tombstones (calendar_id, uid, remote_url) VALUES (?, ?, ?)
			 ON CONFLICT(calendar_id, uid) DO UPDATE SET
			     remote_url = excluded.remote_url,
			     deleted_at = excluded.deleted_at`,
			srcCalendarID, uid, remoteURL,
		); err != nil {
			return fmt.Errorf("record source delete tombstone: %w", err)
		}
		// Clear the source dirty flag. Keep remote_url and etag. The push
		// phase must not re-PUT the moved body to the source href before the
		// tombstone phase DELETEs it. The etag still makes that DELETE
		// conditional.
		if _, err := db.ExecContext(ctx,
			`UPDATE sync_resources SET dirty = 0 WHERE calendar_id = ? AND uid = ?`,
			srcCalendarID, uid,
		); err != nil {
			return fmt.Errorf("clear source dirty flag: %w", err)
		}
	} else {
		// Never-pushed placeholder. No remote resource exists to delete.
		// Remove the row so a later source-calendar push cannot create the
		// moved item at a fresh href.
		if _, err := db.ExecContext(ctx,
			`DELETE FROM sync_resources WHERE calendar_id = ? AND uid = ?`,
			srcCalendarID, uid,
		); err != nil {
			return fmt.Errorf("drop unpushed source resource: %w", err)
		}
	}

	// Establish the destination push intent. A linked destination calendar
	// gets a dirty row (empty remote_url becomes a first-time create). A
	// local-only destination calendar is a no-op.
	if err := MarkResourceDirty(ctx, db, dstCalendarID, uid, ownerType); err != nil {
		return fmt.Errorf("mark destination resource dirty: %w", err)
	}
	return nil
}

// CreateTombstoneIfSynced inserts a tombstone row if the resource was
// previously synced (has a sync_resources row with a non-empty remote_url).
// Returns true if a tombstone was created.
//
// db is a DBTX so callers can pass their own *sql.Tx. The tombstone
// write then commits (or rolls back) atomically with the soft-delete it
// accompanies. Pass *sql.DB only when there is no transaction around it.
func CreateTombstoneIfSynced(ctx context.Context, db DBTX, calendarID int64, uid string) (bool, error) {
	if calendarID == 0 || uid == "" {
		return false, nil
	}
	var remoteURL string
	err := db.QueryRowContext(ctx,
		`SELECT remote_url FROM sync_resources WHERE calendar_id = ? AND uid = ?`,
		calendarID, uid,
	).Scan(&remoteURL)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if remoteURL == "" {
		return false, nil
	}
	_, err = db.ExecContext(ctx,
		`INSERT INTO tombstones (calendar_id, uid, remote_url) VALUES (?, ?, ?)
		 ON CONFLICT(calendar_id, uid) DO UPDATE SET
		     remote_url = excluded.remote_url,
		     deleted_at = excluded.deleted_at`,
		calendarID, uid, remoteURL,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}
