package event

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/douglasdemoura/chroncal/internal/calendaraccess"
	"github.com/douglasdemoura/chroncal/internal/storage"
	"github.com/douglasdemoura/chroncal/internal/timeutil"
)

// This file holds the transaction-scoped cores the trash aggregator uses
// for atomic multi-entry batches. The aggregator owns one transaction for
// the whole batch. It calls every Check* method first (read-only gates),
// then every Apply* method (writes). None of these methods begins or
// commits a transaction. A failed check or apply rolls the whole batch
// back. The single-entry paths in soft_delete.go / trash.go own their
// own transactions and keep their existing behavior.
//
// CheckRestoreTrashEntry gates one trash entry for restore. It verifies
// the target exists in the expected deleted state and that the calendar
// accepts VEVENT writes where the single-entry restore path checks that
// too. It performs no writes.
func (s *Service) CheckRestoreTrashEntry(ctx context.Context, tx *sql.Tx, entry TrashEntry) error {
	qtx := s.q.WithTx(tx)
	switch entry.Kind {
	case TrashKindEvent:
		r, err := qtx.GetEventIncludingDeleted(ctx, entry.ID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotDeleted
			}
			return fmt.Errorf("get event: %w", err)
		}
		if r.DeletedAt == nil || *r.DeletedAt == "" {
			return ErrNotDeleted
		}
		return calendaraccess.EnsureWritable(ctx, qtx, r.CalendarID, "VEVENT")
	case TrashKindInstance:
		log, err := qtx.GetEventExdateDelete(ctx, entry.ID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotDeleted
			}
			return fmt.Errorf("get exdate log: %w", err)
		}
		if _, err := qtx.GetEventByUID(ctx, log.Uid); err != nil {
			return fmt.Errorf("get master: %w", err)
		}
		return nil
	case TrashKindTruncation:
		log, err := qtx.GetEventTruncateDelete(ctx, entry.ID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotDeleted
			}
			return fmt.Errorf("get truncate log: %w", err)
		}
		if _, err := qtx.GetEventByUID(ctx, log.Uid); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("get master: %w", ErrNotDeleted)
			}
			return fmt.Errorf("get master: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown trash kind %d", entry.Kind)
	}
}

// ApplyRestoreTrashEntry performs the restore writes for one entry inside
// the batch transaction. It re-reads its target on the transaction
// snapshot and uses row-count-guarded writes. A change from another
// operation between the check and the apply then fails the apply and the
// aggregator rolls every earlier write back.
func (s *Service) ApplyRestoreTrashEntry(ctx context.Context, tx *sql.Tx, entry TrashEntry) error {
	qtx := s.q.WithTx(tx)
	switch entry.Kind {
	case TrashKindEvent:
		r, err := qtx.GetEventIncludingDeleted(ctx, entry.ID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotDeleted
			}
			return fmt.Errorf("get event: %w", err)
		}
		n, err := qtx.RestoreEventRow(ctx, r.ID)
		if err != nil {
			return fmt.Errorf("restore event: %w", err)
		}
		if n != 1 {
			// Zero rows with the row still deleted in this transaction's
			// snapshot means a writer outside this batch changed the row;
			// the commit then also fails. A live row can only appear when
			// an earlier apply in this same batch un-hid it (for example
			// the provenance restore of a truncation entry). That is a
			// benign overlap; the EXDATE and sync steps below are
			// idempotent.
			fresh, getErr := qtx.GetEventIncludingDeleted(ctx, r.ID)
			if getErr != nil {
				return fmt.Errorf("get event: %w", getErr)
			}
			if fresh.DeletedAt != nil && *fresh.DeletedAt != "" {
				return ErrNotDeleted
			}
		}
		if r.RecurrenceID != "" {
			if err := clearMasterEXDATE(ctx, qtx, r.Uid, r.RecurrenceID); err != nil {
				return err
			}
		}
		return reconcileSyncRestoreTx(ctx, tx, qtx, r.CalendarID, r.Uid, "event")
	case TrashKindInstance:
		return applyInstanceRestoreTx(ctx, tx, qtx, entry.UID, entry.InstanceTime, entry.ID)
	case TrashKindTruncation:
		return applyTruncationRestoreTx(ctx, tx, qtx, entry.ID)
	default:
		return fmt.Errorf("unknown trash kind %d", entry.Kind)
	}
}

// CheckPurgeTrashEntry gates one trash entry for permanent removal. Purge
// never calls the calendar write guard; the single-entry PurgeByID and
// log-delete paths do not call it either. The check mirrors that
// contract so batch and single purge stay equivalent.
func (s *Service) CheckPurgeTrashEntry(ctx context.Context, tx *sql.Tx, entry TrashEntry) error {
	qtx := s.q.WithTx(tx)
	switch entry.Kind {
	case TrashKindEvent:
		r, err := qtx.GetEventIncludingDeleted(ctx, entry.ID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotDeleted
			}
			return fmt.Errorf("get event: %w", err)
		}
		if r.DeletedAt == nil || *r.DeletedAt == "" {
			return ErrNotDeleted
		}
		return nil
	case TrashKindInstance:
		if _, err := qtx.GetEventExdateDelete(ctx, entry.ID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotDeleted
			}
			return fmt.Errorf("get exdate log: %w", err)
		}
		return nil
	case TrashKindTruncation:
		if _, err := qtx.GetEventTruncateDelete(ctx, entry.ID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotDeleted
			}
			return fmt.Errorf("get truncate log: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown trash kind %d", entry.Kind)
	}
}

// ApplyPurgeTrashEntry hard-removes one entry inside the batch
// transaction. Every delete is row-count guarded. Zero rows means
// another operation removed the target after the check; the apply fails
// and the whole batch rolls back. No partial purge can commit.
func (s *Service) ApplyPurgeTrashEntry(ctx context.Context, tx *sql.Tx, entry TrashEntry) error {
	qtx := s.q.WithTx(tx)
	switch entry.Kind {
	case TrashKindEvent:
		n, err := qtx.PurgeEventByID(ctx, entry.ID)
		if err != nil {
			return fmt.Errorf("purge event: %w", err)
		}
		if n != 1 {
			return ErrNotDeleted
		}
		return nil
	case TrashKindInstance:
		n, err := qtx.DeleteEventExdateDeleteChecked(ctx, entry.ID)
		if err != nil {
			return fmt.Errorf("delete exdate log: %w", err)
		}
		if n != 1 {
			return ErrNotDeleted
		}
		return nil
	case TrashKindTruncation:
		n, err := qtx.DeleteEventTruncateDeleteChecked(ctx, entry.ID)
		if err != nil {
			return fmt.Errorf("delete truncate log: %w", err)
		}
		if n != 1 {
			return ErrNotDeleted
		}
		return nil
	default:
		return fmt.Errorf("unknown trash kind %d", entry.Kind)
	}
}

// applyInstanceRestoreTx is the batch-tx core for an EXDATE-based
// instance restore. It mirrors restoreInstanceByLogID, but every write
// joins the caller transaction and the provenance-log delete is row-count
// guarded so a consumed log fails the batch.
func applyInstanceRestoreTx(ctx context.Context, tx *sql.Tx, qtx *storage.Queries, uid string, instance time.Time, logID int64) error {
	log, err := qtx.GetEventExdateDelete(ctx, logID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("get exdate log: %w", err)
		}
		// The log row is gone. It was either consumed by an earlier
		// entry in this same batch (an override-row restore strips the
		// EXDATE and deletes the provenance row together) or removed by
		// an outside writer. Own writes are visible on the transaction
		// connection; an outside delete is not. When the master and the
		// EXDATE are already gone too, the batch has fully reversed this
		// instance delete. Treat it as done.
		return instanceRestoreAlreadyAppliedTx(ctx, qtx, uid, instance)
	}
	master, err := qtx.GetEventByUID(ctx, log.Uid)
	if err != nil {
		return fmt.Errorf("get master: %w", err)
	}
	existing := ParseTimeList(storage.NullableToString(master.Exdates))
	target, err := time.Parse(time.RFC3339, log.RecurrenceID)
	if err != nil {
		return fmt.Errorf("parse recurrence_id %q: %w", log.RecurrenceID, err)
	}
	filtered := timeutil.RemoveTimeFromList(existing, target)
	if err := qtx.UpdateEventExdates(ctx, storage.UpdateEventExdatesParams{
		Exdates: storage.StringToNullable(SerializeTimeList(filtered)),
		ID:      master.ID,
	}); err != nil {
		return fmt.Errorf("update exdates: %w", err)
	}
	n, err := qtx.DeleteEventExdateDeleteChecked(ctx, logID)
	if err != nil {
		return fmt.Errorf("delete log row: %w", err)
	}
	if n != 1 {
		return ErrNotDeleted
	}
	if err := storage.MarkResourceDirty(ctx, tx, master.CalendarID, log.Uid, "event"); err != nil {
		return fmt.Errorf("mark resource dirty: %w", err)
	}
	return nil
}

// instanceRestoreAlreadyAppliedTx is the overlap fallback for an
// instance-log entry whose provenance row an earlier apply in the same
// batch already consumed (an override-row restore strips the EXDATE and
// deletes the log together). It confirms the live master no longer
// carries that EXDATE. When the EXDATE is still present the state did
// not come from this batch, so it returns ErrNotDeleted and the batch
// rolls back.
func instanceRestoreAlreadyAppliedTx(ctx context.Context, qtx *storage.Queries, uid string, instance time.Time) error {
	if uid == "" || instance.IsZero() {
		return ErrNotDeleted
	}
	master, err := qtx.GetEventByUID(ctx, uid)
	if err != nil {
		return ErrNotDeleted
	}
	existing := ParseTimeList(storage.NullableToString(master.Exdates))
	filtered := timeutil.RemoveTimeFromList(existing, instance)
	if len(filtered) != len(existing) {
		return ErrNotDeleted
	}
	return nil
}

// applyTruncationRestoreTx is the batch-tx core for an RRULE truncation
// restore. It mirrors restoreTruncationByLogID: master RRULE, trimmed
// RDATEs, and the exact hidden overrides come back, then the log row is
// consumed. The log delete is row-count guarded so a concurrent change
// fails the batch instead of silently committing half of the reversal.
func applyTruncationRestoreTx(ctx context.Context, tx *sql.Tx, qtx *storage.Queries, logID int64) error {
	log, err := qtx.GetEventTruncateDelete(ctx, logID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotDeleted
		}
		return fmt.Errorf("get truncate log: %w", err)
	}
	master, err := qtx.GetEventByUID(ctx, log.Uid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("get master: %w", ErrNotDeleted)
		}
		return fmt.Errorf("get master: %w", err)
	}
	if err := qtx.UpdateEventRecurrenceRule(ctx, storage.UpdateEventRecurrenceRuleParams{
		RecurrenceRule: storage.StringToNullable(log.PreviousRrule),
		ID:             master.ID,
	}); err != nil {
		return fmt.Errorf("restore rrule: %w", err)
	}
	if err := restoreTruncatedRDates(ctx, qtx, master, log); err != nil {
		return fmt.Errorf("restore rdates: %w", err)
	}
	if err := restoreTruncatedOverrides(ctx, qtx, log); err != nil {
		return fmt.Errorf("restore overrides: %w", err)
	}
	n, err := qtx.DeleteEventTruncateDeleteChecked(ctx, logID)
	if err != nil {
		return fmt.Errorf("delete log row: %w", err)
	}
	if n != 1 {
		return ErrNotDeleted
	}
	if err := storage.MarkResourceDirty(ctx, tx, master.CalendarID, log.Uid, "event"); err != nil {
		return fmt.Errorf("mark resource dirty: %w", err)
	}
	return nil
}

// reconcileSyncRestoreTx applies the restore sync reconciliation inside
// the batch transaction: the pending tombstone is cleared and the
// resource is marked dirty. The writes use the same DBTX as the rest of
// the batch so a failure rolls the row restore back too.
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
