// Package trash joins soft-deleted rows from the event, todo, and journal
// services into one "Recently deleted" view. The TUI calls Service.List
// to show a mixed list newest-first. It then calls RestoreBatch or
// PurgeBatch with the marked entries. Service.Restore and Service.Purge
// are the one-entry forms of the same batch path.
//
// A batch runs in one transaction. Every entry passes its existence,
// permission, and kind check before any entry changes a row. The domain
// rows, EXDATE and RRULE changes, provenance log rows, and sync
// tombstone and dirty side effects then commit together. One failed
// entry rolls the whole batch back.
//
// Each service still owns its own soft-delete, restore, and purge paths.
// This package is an aggregator. It holds no storage state of its own
// apart from the shared database handle.
package trash

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/douglasdemoura/chroncal/internal/event"
	"github.com/douglasdemoura/chroncal/internal/journal"
	"github.com/douglasdemoura/chroncal/internal/todo"
)

// Kind discriminates the source domain of a trash entry. The numeric
// values intentionally do not overlap with event.TrashKind. Callers
// should use the Kind methods (IsEvent, IsTodo, IsJournal). Do not
// compare raw values.
type Kind int

const (
	// KindEvent is a soft-deleted events row.
	KindEvent Kind = iota
	// KindEventInstance is an EXDATE-based instance delete captured in the
	// event_exdate_deletes log.
	KindEventInstance
	// KindEventSeriesTail is an RRULE truncation captured in
	// event_truncate_deletes ("This and following").
	KindEventSeriesTail
	// KindTodo is a soft-deleted todos row.
	KindTodo
	// KindJournal is a soft-deleted journals row.
	KindJournal
)

// Entry is a unified row for the mixed trash dialog. Domain-specific
// fields have values only for the Kind that matches. Readers should branch
// on Kind before they read those fields. Title and DeletedAt are always valid.
type Entry struct {
	Kind       Kind
	ID         int64
	CalendarID int64
	UID        string
	Title      string
	DeletedAt  time.Time

	// Event-specific (KindEvent, KindEventInstance, KindEventSeriesTail).
	InstanceTime  time.Time
	CutoffTime    time.Time
	PreviousRRule string
	StartTime     time.Time
	EndTime       time.Time
	AllDay        bool

	// Todo-specific (KindTodo).
	DueDate         time.Time
	PercentComplete int64

	// Common.
	Location    string
	Description string
	Status      string
	Categories  string
}

// Service joins soft-delete state across event, todo, and journal
// into one List/Restore/Purge surface.
type Service struct {
	db       *sql.DB
	events   *event.Service
	todos    *todo.Service
	journals *journal.Service
}

// NewService wires the aggregator. db is the shared database handle the
// batch paths use for one cross-domain transaction. Any of the service
// arguments may be nil to opt a domain out of trash aggregation (tests,
// partial features).
func NewService(db *sql.DB, e *event.Service, t *todo.Service, j *journal.Service) *Service {
	return &Service{db: db, events: e, todos: t, journals: j}
}

// PurgeCounts reports how many rows each domain dropped from a PurgeOld
// call so callers (maintenance, CLI) can log per-domain numbers.
type PurgeCounts struct {
	Events              int
	EventInstanceLogs   int
	EventTruncateLogs   int
	Todos               int
	TodoInstanceLogs    int
	Journals            int
	JournalInstanceLogs int
}

// List returns every trash entry for calendarID across all domains,
// sorted newest-first by DeletedAt.
func (s *Service) List(ctx context.Context, calendarID int64) ([]Entry, error) {
	var entries []Entry

	if s.events != nil {
		evTrash, err := s.events.ListTrash(ctx, calendarID)
		if err != nil {
			return nil, fmt.Errorf("list event trash: %w", err)
		}
		for _, e := range evTrash {
			entries = append(entries, fromEventTrash(e))
		}
	}

	if s.todos != nil {
		tdDeleted, err := s.todos.ListDeleted(ctx, calendarID)
		if err != nil {
			return nil, fmt.Errorf("list deleted todos: %w", err)
		}
		for _, td := range tdDeleted {
			entries = append(entries, fromTodo(td))
		}
	}

	if s.journals != nil {
		jDeleted, err := s.journals.ListDeleted(ctx, calendarID)
		if err != nil {
			return nil, fmt.Errorf("list deleted journals: %w", err)
		}
		for _, j := range jDeleted {
			entries = append(entries, fromJournal(j))
		}
	}

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].DeletedAt.After(entries[j].DeletedAt)
	})
	return entries, nil
}

// batchOp selects the restore or purge path inside one batch
// transaction.
type batchOp int

const (
	batchRestore batchOp = iota
	batchPurge
)

func (op batchOp) name() string {
	switch op {
	case batchRestore:
		return "restore"
	case batchPurge:
		return "purge"
	default:
		return "batch"
	}
}

// BatchError reports which entry of a batch failed and why. Index is the
// 1-based position in the deduplicated batch. The wrapped error stays
// available via errors.Is / errors.As so callers can still detect
// domain errors such as event.ErrNotDeleted or calendaraccess.ErrReadOnly.
type BatchError struct {
	Op    string
	Index int
	Entry Entry
	Err   error
}

func (e *BatchError) Error() string {
	title := e.Entry.Title
	if title == "" {
		title = e.Entry.Kind.Label()
	}
	return fmt.Sprintf("%s item %d of batch (%q): %v", e.Op, e.Index, title, e.Err)
}

func (e *BatchError) Unwrap() error { return e.Err }

// Restore reverses the delete recorded in e. It runs through the same
// atomic batch path a multi-entry restore uses, so single and batch
// semantics stay identical.
func (s *Service) Restore(ctx context.Context, e Entry) error {
	return s.RestoreBatch(ctx, []Entry{e})
}

// Purge hard-removes e from the trash. It runs through the same atomic
// batch path a multi-entry purge uses, so single and batch semantics stay
// identical.
func (s *Service) Purge(ctx context.Context, e Entry) error {
	return s.PurgeBatch(ctx, []Entry{e})
}

// RestoreBatch restores every entry in one atomic transaction. The
// method first runs the existence, permission, and kind checks for every
// entry. It then applies the domain writes. The domain rows, EXDATE and
// RRULE changes, the delete-source log rows, and the sync tombstone and
// dirty side effects all commit together. Any failure rolls the whole
// batch back. Callers never observe a partially restored selection.
func (s *Service) RestoreBatch(ctx context.Context, entries []Entry) error {
	return s.runBatch(ctx, entries, batchRestore)
}

// PurgeBatch permanently removes every entry in one atomic transaction.
// The check phase runs first for every entry. The hard deletes then
// commit together. Any failure rolls the whole batch back, so a purge
// can never leave a subset of the selection permanently removed.
func (s *Service) PurgeBatch(ctx context.Context, entries []Entry) error {
	return s.runBatch(ctx, entries, batchPurge)
}

// runBatch is the shared two-phase batch driver. It deduplicates entries
// by kind and ID in first-seen order so a repeated mark cannot act twice
// on one row. Static problems (unknown kind, domain not configured) fail
// before a transaction opens.
func (s *Service) runBatch(ctx context.Context, entries []Entry, op batchOp) error {
	if len(entries) == 0 {
		return nil
	}
	if s.db == nil {
		return fmt.Errorf("trash: database not configured")
	}

	unique := make([]Entry, 0, len(entries))
	seen := make(map[string]bool, len(entries))
	for i, e := range entries {
		if err := s.validateEntry(e); err != nil {
			return &BatchError{Op: op.name(), Index: i + 1, Entry: e, Err: err}
		}
		key := fmt.Sprintf("%d:%d", e.Kind, e.ID)
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, e)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin batch tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Phase 1: verify every entry against current state. This phase
	// performs no writes.
	for i, e := range unique {
		if err := s.checkEntry(ctx, tx, e, op); err != nil {
			return &BatchError{Op: op.name(), Index: i + 1, Entry: e, Err: err}
		}
	}

	// Phase 2: apply every mutation on the same transaction.
	for i, e := range unique {
		if err := s.applyEntry(ctx, tx, e, op); err != nil {
			return &BatchError{Op: op.name(), Index: i + 1, Entry: e, Err: err}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}
	return nil
}

// validateEntry verifies the Kind is known and the domain service that
// owns it is configured. It runs before the batch transaction opens.
func (s *Service) validateEntry(e Entry) error {
	switch {
	case e.Kind.IsEvent():
		if s.events == nil {
			return fmt.Errorf("events service not configured")
		}
	case e.Kind.IsTodo():
		if s.todos == nil {
			return fmt.Errorf("todos service not configured")
		}
	case e.Kind.IsJournal():
		if s.journals == nil {
			return fmt.Errorf("journals service not configured")
		}
	default:
		return fmt.Errorf("unknown trash kind %d", e.Kind)
	}
	return nil
}

// checkEntry dispatches the read-only feasibility check for one entry.
func (s *Service) checkEntry(ctx context.Context, tx *sql.Tx, e Entry, op batchOp) error {
	switch op {
	case batchRestore:
		switch {
		case e.Kind.IsEvent():
			return s.events.CheckRestoreTrashEntry(ctx, tx, toEventTrash(e))
		case e.Kind.IsTodo():
			return s.todos.CheckRestoreTrashID(ctx, tx, e.ID)
		case e.Kind.IsJournal():
			return s.journals.CheckRestoreTrashID(ctx, tx, e.ID)
		}
	case batchPurge:
		switch {
		case e.Kind.IsEvent():
			return s.events.CheckPurgeTrashEntry(ctx, tx, toEventTrash(e))
		case e.Kind.IsTodo():
			return s.todos.CheckPurgeTrashID(ctx, tx, e.ID)
		case e.Kind.IsJournal():
			return s.journals.CheckPurgeTrashID(ctx, tx, e.ID)
		}
	}
	return fmt.Errorf("unknown trash kind %d", e.Kind)
}

// applyEntry dispatches the write for one entry on the batch transaction.
func (s *Service) applyEntry(ctx context.Context, tx *sql.Tx, e Entry, op batchOp) error {
	switch op {
	case batchRestore:
		switch {
		case e.Kind.IsEvent():
			return s.events.ApplyRestoreTrashEntry(ctx, tx, toEventTrash(e))
		case e.Kind.IsTodo():
			return s.todos.ApplyRestoreTrashID(ctx, tx, e.ID)
		case e.Kind.IsJournal():
			return s.journals.ApplyRestoreTrashID(ctx, tx, e.ID)
		}
	case batchPurge:
		switch {
		case e.Kind.IsEvent():
			return s.events.ApplyPurgeTrashEntry(ctx, tx, toEventTrash(e))
		case e.Kind.IsTodo():
			return s.todos.ApplyPurgeTrashID(ctx, tx, e.ID)
		case e.Kind.IsJournal():
			return s.journals.ApplyPurgeTrashID(ctx, tx, e.ID)
		}
	}
	return fmt.Errorf("unknown trash kind %d", e.Kind)
}

// PurgeOld walks each domain's retention purge and returns per-domain
// counts. The log-row purges (event_exdate_deletes, event_truncate_deletes)
// also run so the "This event" / "This and following" history does not
// pile up without bound.
func (s *Service) PurgeOld(ctx context.Context, olderThan time.Time) (PurgeCounts, error) {
	var counts PurgeCounts
	if s.events != nil {
		n, err := s.events.PurgeDeleted(ctx, olderThan)
		if err != nil {
			return counts, fmt.Errorf("purge events: %w", err)
		}
		counts.Events = n
		n, err = s.events.PurgeOldInstanceDeletes(ctx, olderThan)
		if err != nil {
			return counts, fmt.Errorf("purge event instance logs: %w", err)
		}
		counts.EventInstanceLogs = n
		n, err = s.events.PurgeOldTruncationDeletes(ctx, olderThan)
		if err != nil {
			return counts, fmt.Errorf("purge event truncation logs: %w", err)
		}
		counts.EventTruncateLogs = n
	}
	if s.todos != nil {
		n, err := s.todos.PurgeDeleted(ctx, olderThan)
		if err != nil {
			return counts, fmt.Errorf("purge todos: %w", err)
		}
		counts.Todos = n
		n, err = s.todos.PurgeOldInstanceDeletes(ctx, olderThan)
		if err != nil {
			return counts, fmt.Errorf("purge todo instance logs: %w", err)
		}
		counts.TodoInstanceLogs = n
	}
	if s.journals != nil {
		n, err := s.journals.PurgeDeleted(ctx, olderThan)
		if err != nil {
			return counts, fmt.Errorf("purge journals: %w", err)
		}
		counts.Journals = n
		n, err = s.journals.PurgeOldInstanceDeletes(ctx, olderThan)
		if err != nil {
			return counts, fmt.Errorf("purge journal instance logs: %w", err)
		}
		counts.JournalInstanceLogs = n
	}
	return counts, nil
}

// fromEventTrash converts an event.TrashEntry into the unified Entry
// shape. It keeps every field so the trash dialog can show the same
// detail content it did before the aggregator.
func fromEventTrash(e event.TrashEntry) Entry {
	return Entry{
		Kind:          mapEventKind(e.Kind),
		ID:            e.ID,
		CalendarID:    e.CalendarID,
		UID:           e.UID,
		Title:         e.Title,
		DeletedAt:     e.DeletedAt,
		InstanceTime:  e.InstanceTime,
		CutoffTime:    e.CutoffTime,
		PreviousRRule: e.PreviousRRule,
		StartTime:     e.StartTime,
		EndTime:       e.EndTime,
		AllDay:        e.AllDay,
		Location:      e.Location,
		Description:   e.Description,
		Status:        e.Status,
		Categories:    e.Categories,
	}
}

// toEventTrash is the inverse of fromEventTrash, used to hand an Entry
// back to event.Service.RestoreTrash / PurgeTrashEntry.
func toEventTrash(e Entry) event.TrashEntry {
	return event.TrashEntry{
		Kind:          unmapEventKind(e.Kind),
		ID:            e.ID,
		CalendarID:    e.CalendarID,
		UID:           e.UID,
		Title:         e.Title,
		DeletedAt:     e.DeletedAt,
		InstanceTime:  e.InstanceTime,
		CutoffTime:    e.CutoffTime,
		PreviousRRule: e.PreviousRRule,
		StartTime:     e.StartTime,
		EndTime:       e.EndTime,
		AllDay:        e.AllDay,
		Location:      e.Location,
		Description:   e.Description,
		Status:        e.Status,
		Categories:    e.Categories,
	}
}

func mapEventKind(k event.TrashKind) Kind {
	switch k {
	case event.TrashKindEvent:
		return KindEvent
	case event.TrashKindInstance:
		return KindEventInstance
	case event.TrashKindTruncation:
		return KindEventSeriesTail
	default:
		return KindEvent
	}
}

func unmapEventKind(k Kind) event.TrashKind {
	switch k {
	case KindEvent:
		return event.TrashKindEvent
	case KindEventInstance:
		return event.TrashKindInstance
	case KindEventSeriesTail:
		return event.TrashKindTruncation
	default:
		return event.TrashKindEvent
	}
}

func fromTodo(td todo.Todo) Entry {
	deletedAt := time.Time{}
	if td.DeletedAt != nil {
		deletedAt = *td.DeletedAt
	} else if !td.UpdatedAt.IsZero() {
		// Fallback for pre-v3 rows that were deleted before DeletedAt was
		// surfaced on the domain model; UpdatedAt is bumped alongside
		// deleted_at in SoftDeleteTodo, so it's a close stand-in.
		deletedAt = td.UpdatedAt
	}
	dueDate := time.Time{}
	if td.DueDate != "" {
		if t, err := time.Parse(time.RFC3339, td.DueDate); err == nil {
			dueDate = t
		} else if t, err := time.Parse("2006-01-02", td.DueDate); err == nil {
			dueDate = t
		}
	}
	return Entry{
		Kind:            KindTodo,
		ID:              td.ID,
		CalendarID:      td.CalendarID,
		UID:             td.UID,
		Title:           td.Summary,
		DeletedAt:       deletedAt,
		DueDate:         dueDate,
		PercentComplete: td.PercentComplete,
		Location:        td.Location,
		Description:     td.Description,
		Status:          td.Status,
		Categories:      td.Categories,
	}
}

func fromJournal(j journal.Journal) Entry {
	deletedAt := time.Time{}
	if j.DeletedAt != nil {
		deletedAt = *j.DeletedAt
	} else if !j.UpdatedAt.IsZero() {
		deletedAt = j.UpdatedAt
	}
	startDate := time.Time{}
	if j.StartDate != "" {
		if t, err := time.Parse(time.RFC3339, j.StartDate); err == nil {
			startDate = t
		} else if t, err := time.Parse("2006-01-02", j.StartDate); err == nil {
			startDate = t
		}
	}
	return Entry{
		Kind:        KindJournal,
		ID:          j.ID,
		CalendarID:  j.CalendarID,
		UID:         j.UID,
		Title:       j.Summary,
		DeletedAt:   deletedAt,
		StartTime:   startDate,
		Description: j.Description,
		Status:      j.Status,
		Categories:  j.Categories,
	}
}

// IsEvent reports whether the entry originates from the events table
// (event row, EXDATE log, or truncation log).
func (k Kind) IsEvent() bool {
	return k == KindEvent || k == KindEventInstance || k == KindEventSeriesTail
}

// IsTodo reports whether the entry is a soft-deleted todos row.
func (k Kind) IsTodo() bool { return k == KindTodo }

// IsJournal reports whether the entry is a soft-deleted journals row.
func (k Kind) IsJournal() bool { return k == KindJournal }

// Label is a short human-facing name for the kind, used in the trash
// dialog's "Kind" detail row.
func (k Kind) Label() string {
	switch k {
	case KindEvent:
		return "Event"
	case KindEventInstance:
		return "Event instance"
	case KindEventSeriesTail:
		return "Series tail"
	case KindTodo:
		return "Todo"
	case KindJournal:
		return "Journal"
	default:
		return "Unknown"
	}
}
