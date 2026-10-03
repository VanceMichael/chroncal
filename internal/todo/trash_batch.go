package todo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/douglasdemoura/chroncal/internal/calendaraccess"
	"github.com/douglasdemoura/chroncal/internal/storage"
)

// This file holds the transaction-scoped cores the trash aggregator uses
// for atomic multi-entry batches. The aggregator owns one transaction for
// the whole batch. It calls Check* for every entry first, then Apply*.
// None of these methods begins or commits a transaction. A failed check
// or apply rolls the whole batch back. The single-entry methods in
// soft_delete.go keep their own transactions and behavior.

// CheckRestoreTrashID gates one soft-deleted todo row for restore. It
// mirrors RestoreByID gates: the row must exist and be soft-deleted, and
// the calendar must accept VTODO writes.
func (s *Service) CheckRestoreTrashID(ctx context.Context, tx *sql.Tx, id int64) error {
	qtx := s.q.WithTx(tx)
	r, err := qtx.GetTodoIncludingDeleted(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotDeleted
		}
		return fmt.Errorf("get todo: %w", err)
	}
	if r.DeletedAt == nil || *r.DeletedAt == "" {
		return ErrNotDeleted
	}
	return calendaraccess.EnsureWritable(ctx, qtx, r.CalendarID, todoComponent)
}

// ApplyRestoreTrashID un-hides the row inside the batch transaction. For
// an override it also strips the delete-recorded EXDATE from the master.
// Tombstone clearing and the dirty mark join the same transaction. Every
// state-changing write is row-count guarded so a concurrent restore or
// purge fails the batch and rolls it back.
func (s *Service) ApplyRestoreTrashID(ctx context.Context, tx *sql.Tx, id int64) error {
	qtx := s.q.WithTx(tx)
	r, err := qtx.GetTodoIncludingDeleted(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotDeleted
		}
		return fmt.Errorf("get todo: %w", err)
	}
	if r.DeletedAt == nil || *r.DeletedAt == "" {
		return ErrNotDeleted
	}
	n, err := qtx.RestoreTodoRow(ctx, r.ID)
	if err != nil {
		return fmt.Errorf("restore todo: %w", err)
	}
	if n != 1 {
		return ErrNotDeleted
	}
	if r.RecurrenceID != "" {
		if err := clearMasterEXDATE(ctx, qtx, r.Uid, r.RecurrenceID); err != nil {
			return err
		}
	}
	return reconcileSyncRestoreTx(ctx, tx, qtx, r.CalendarID, r.Uid, "todo")
}

// CheckPurgeTrashID gates one permanent todo removal. Purge has no
// calendar write guard; PurgeByID checks only the deleted state.
func (s *Service) CheckPurgeTrashID(ctx context.Context, tx *sql.Tx, id int64) error {
	qtx := s.q.WithTx(tx)
	r, err := qtx.GetTodoIncludingDeleted(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotDeleted
		}
		return fmt.Errorf("get todo: %w", err)
	}
	if r.DeletedAt == nil || *r.DeletedAt == "" {
		return ErrNotDeleted
	}
	return nil
}

// ApplyPurgeTrashID hard-deletes the row inside the batch transaction.
// Zero affected rows means another operation changed the row after the
// check; the apply fails and the whole batch rolls back.
func (s *Service) ApplyPurgeTrashID(ctx context.Context, tx *sql.Tx, id int64) error {
	qtx := s.q.WithTx(tx)
	n, err := qtx.PurgeTodoByID(ctx, id)
	if err != nil {
		return fmt.Errorf("purge todo: %w", err)
	}
	if n != 1 {
		return ErrNotDeleted
	}
	return nil
}

// reconcileSyncRestoreTx runs the restore sync reconciliation on the
// batch transaction: clear the pending tombstone and mark the resource
// dirty. A failure rolls the row restore back too.
func reconcileSyncRestoreTx(ctx context.Context, exec storage.DBTX, qtx *storage.Queries, calendarID int64, uid, ownerType string) error {
	if err := qtx.DeleteTombstonesByCalendarAndUID(ctx, storage.DeleteTombstonesByCalendarAndUIDParams{
		CalendarID: calendarID,
		Uid:        uid,
	}); err != nil {
		return fmt.Errorf("clear tombstone after restore: %w", err)
	}
	if err := storage.MarkResourceDirty(ctx, exec, calendarID, uid, ownerType); err != nil {
		return fmt.Errorf("mark resource dirty after restore: %w", err)
	}
	return nil
}
