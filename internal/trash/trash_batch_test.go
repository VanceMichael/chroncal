package trash

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/douglasdemoura/chroncal/internal/calendaraccess"
	"github.com/douglasdemoura/chroncal/internal/event"
	"github.com/douglasdemoura/chroncal/internal/journal"
	"github.com/douglasdemoura/chroncal/internal/testutil"
	"github.com/douglasdemoura/chroncal/internal/todo"
)

// findEntry returns the entry with kind and id.
func findEntry(t *testing.T, entries []Entry, kind Kind, id int64) Entry {
	t.Helper()
	for _, e := range entries {
		if e.Kind == kind && e.ID == id {
			return e
		}
	}
	t.Fatalf("entry kind=%d id=%d not found in %d entries", kind, id, len(entries))
	return Entry{}
}

func requireStillDeleted(t *testing.T, svc *Service, calID int64, want int) {
	t.Helper()
	got, err := svc.List(context.Background(), calID)
	if err != nil {
		t.Fatalf("List after failed batch: %v", err)
	}
	if len(got) != want {
		t.Fatalf("trash entries after failed batch = %d, want %d (%+v)", len(got), want, got)
	}
}

// TestRestoreBatch_MixedKindsCommitsTogether restores an event, a todo,
// and a journal in one batch. Every domain row comes back and the trash
// list is empty.
func TestRestoreBatch_MixedKindsCommitsTogether(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	ev, err := events.Create(ctx, event.CreateParams{
		CalendarID: 1,
		Title:      "Batch Event",
		StartTime:  time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC),
		EndTime:    time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	td, err := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Batch Todo"})
	if err != nil {
		t.Fatalf("create todo: %v", err)
	}
	j, err := journals.Create(ctx, journal.CreateParams{CalendarID: 1, Summary: "Batch Journal", StartDate: "2026-05-03"})
	if err != nil {
		t.Fatalf("create journal: %v", err)
	}
	for _, del := range []func() error{
		func() error { return events.Delete(ctx, ev.ID) },
		func() error { return todos.Delete(ctx, td.ID) },
		func() error { return journals.Delete(ctx, j.ID) },
	} {
		if err := del(); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	entries, err := svc.List(ctx, 1)
	if err != nil || len(entries) != 3 {
		t.Fatalf("List = (%d, %v), want 3 entries", len(entries), err)
	}
	if err := svc.RestoreBatch(ctx, entries); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	}
	if left, _ := svc.List(ctx, 1); len(left) != 0 {
		t.Fatalf("trash after restore = %d, want 0", len(left))
	}
	if _, err := events.Get(ctx, ev.ID); err != nil {
		t.Fatalf("event not live: %v", err)
	}
	if _, err := todos.Get(ctx, td.ID); err != nil {
		t.Fatalf("todo not live: %v", err)
	}
	if got, err := journals.Get(ctx, j.ID); err != nil || got.DeletedAt != nil {
		t.Fatalf("journal not live: %v (deleted=%v)", err, got.DeletedAt)
	}
}

// TestRestoreBatch_RollsBackWhenOneEntryIsLive puts a live todo in the
// middle of two deleted todos. The check rejects it. Both deleted rows
// stay deleted. A partial restore must not be visible.
func TestRestoreBatch_RollsBackWhenOneEntryIsLive(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	td1, _ := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "First"})
	td2, _ := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Middle Live"})
	td3, _ := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Last"})
	if err := todos.Delete(ctx, td1.ID); err != nil {
		t.Fatalf("delete first: %v", err)
	}
	if err := todos.Delete(ctx, td3.ID); err != nil {
		t.Fatalf("delete last: %v", err)
	}

	deleted, err := svc.List(ctx, 1)
	if err != nil || len(deleted) != 2 {
		t.Fatalf("List = (%d, %v), want 2", len(deleted), err)
	}
	// Build a fixed order: deleted, live, deleted.
	first := findEntry(t, deleted, KindTodo, td1.ID)
	last := findEntry(t, deleted, KindTodo, td3.ID)
	live := Entry{Kind: KindTodo, ID: td2.ID, CalendarID: 1, Title: "Middle Live"}

	err = svc.RestoreBatch(ctx, []Entry{first, live, last})
	if err == nil {
		t.Fatalf("RestoreBatch with a live entry succeeded")
	}
	var be *BatchError
	if !errors.As(err, &be) {
		t.Fatalf("error %T, want *BatchError", err)
	}
	if be.Index != 2 {
		t.Fatalf("BatchError.Index = %d, want 2", be.Index)
	}
	if !errors.Is(err, todo.ErrNotDeleted) {
		t.Fatalf("error does not wrap todo.ErrNotDeleted: %v", err)
	}
	requireStillDeleted(t, svc, 1, 2)
	if _, err := todos.Get(ctx, td1.ID); err == nil {
		t.Fatalf("first todo restored despite batch rollback")
	}
	if _, err := todos.Get(ctx, td3.ID); err == nil {
		t.Fatalf("last todo restored despite batch rollback")
	}
}

// TestPurgeBatch_RollsBackWhenOneEntryIsLive verifies a purge batch can
// never hard-remove a subset. The two deleted todos stay in the trash
// after the middle live entry rejects the check.
func TestPurgeBatch_RollsBackWhenOneEntryIsLive(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	td1, _ := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "First"})
	td2, _ := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Middle Live"})
	td3, _ := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Last"})
	if err := todos.Delete(ctx, td1.ID); err != nil {
		t.Fatalf("delete first: %v", err)
	}
	if err := todos.Delete(ctx, td3.ID); err != nil {
		t.Fatalf("delete last: %v", err)
	}

	deleted, _ := svc.List(ctx, 1)
	first := findEntry(t, deleted, KindTodo, td1.ID)
	last := findEntry(t, deleted, KindTodo, td3.ID)
	live := Entry{Kind: KindTodo, ID: td2.ID, CalendarID: 1, Title: "Middle Live"}

	err := svc.PurgeBatch(ctx, []Entry{first, live, last})
	if err == nil {
		t.Fatalf("PurgeBatch with a live entry succeeded")
	}
	if !errors.Is(err, todo.ErrNotDeleted) {
		t.Fatalf("error does not wrap todo.ErrNotDeleted: %v", err)
	}
	requireStillDeleted(t, svc, 1, 2)
	// The deleted rows must still exist in the database, not hard-removed.
	if _, err := todos.GetIncludingDeleted(ctx, td1.ID); err != nil {
		t.Fatalf("first todo hard-deleted despite rollback: %v", err)
	}
	if _, err := todos.GetIncludingDeleted(ctx, td3.ID); err != nil {
		t.Fatalf("last todo hard-deleted despite rollback: %v", err)
	}
	if _, err := todos.Get(ctx, td2.ID); err != nil {
		t.Fatalf("live todo changed by failed purge: %v", err)
	}
}

// TestRestoreBatch_ReadOnlyCalendarRollsBackEverything verifies the
// permission check gates the whole batch. An event and a todo stay
// deleted when the calendar rejects writes.
func TestRestoreBatch_ReadOnlyCalendarRollsBackEverything(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	ev, err := events.Create(ctx, event.CreateParams{
		CalendarID: 1,
		Title:      "RO Event",
		StartTime:  time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC),
		EndTime:    time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	td, err := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "RO Todo"})
	if err != nil {
		t.Fatalf("create todo: %v", err)
	}
	if err := events.Delete(ctx, ev.ID); err != nil {
		t.Fatalf("delete event: %v", err)
	}
	if err := todos.Delete(ctx, td.ID); err != nil {
		t.Fatalf("delete todo: %v", err)
	}

	if _, err := db.ExecContext(ctx,
		"UPDATE calendars SET remote_access = 'read', remote_components = 'VEVENT,VTODO,VJOURNAL' WHERE id = 1"); err != nil {
		t.Fatalf("mark calendar read-only: %v", err)
	}

	entries, _ := svc.List(ctx, 1)
	err = svc.RestoreBatch(ctx, entries)
	if err == nil {
		t.Fatalf("RestoreBatch on read-only calendar succeeded")
	}
	if !errors.Is(err, calendaraccess.ErrReadOnly) {
		t.Fatalf("error does not wrap ErrReadOnly: %v", err)
	}
	requireStillDeleted(t, svc, 1, 2)
}

// TestRestoreBatch_SameSeriesMultipleDeleteForms selects an instance
// delete, a series-tail truncation, and the truncation-hidden override
// together. One batch restores the RRULE, strips the EXDATE, un-hides
// the override, and consumes both log rows.
func TestRestoreBatch_SameSeriesMultipleDeleteForms(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	start := time.Date(2026, 4, 1, 14, 0, 0, 0, time.UTC)
	master, err := events.UpsertByUID(ctx, event.UpsertParams{
		UID:            "sprint-review",
		CalendarID:     1,
		Title:          "Sprint Review",
		StartTime:      start,
		EndTime:        start.Add(time.Hour),
		RecurrenceRule: "FREQ=WEEKLY;COUNT=10",
	})
	if err != nil {
		t.Fatalf("create master: %v", err)
	}
	originalRRULE := master.RecurrenceRule

	overrideRec := time.Date(2026, 4, 22, 14, 0, 0, 0, time.UTC)
	override, err := events.UpsertByUID(ctx, event.UpsertParams{
		UID:          master.UID,
		CalendarID:   1,
		Title:        "Sprint Review (moved)",
		StartTime:    overrideRec.Add(time.Hour),
		EndTime:      overrideRec.Add(2 * time.Hour),
		RecurrenceID: overrideRec.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("create override: %v", err)
	}

	instance := time.Date(2026, 4, 8, 14, 0, 0, 0, time.UTC)
	if err := events.DeleteInstance(ctx, master.UID, instance); err != nil {
		t.Fatalf("DeleteInstance: %v", err)
	}
	cutoff := overrideRec
	if err := events.DeleteFromInstance(ctx, master.UID, cutoff); err != nil {
		t.Fatalf("DeleteFromInstance: %v", err)
	}

	entries, err := svc.List(ctx, 1)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (instance log + truncation log + hidden override): %+v", len(entries), entries)
	}
	var hasInst, hasTrunc, hasOverride bool
	for _, e := range entries {
		switch {
		case e.Kind == KindEventInstance:
			hasInst = true
		case e.Kind == KindEventSeriesTail:
			hasTrunc = true
		case e.Kind == KindEvent && e.ID == override.ID:
			hasOverride = true
		}
	}
	if !hasInst || !hasTrunc || !hasOverride {
		t.Fatalf("missing forms: inst=%v trunc=%v override=%v (%+v)", hasInst, hasTrunc, hasOverride, entries)
	}

	if err := svc.RestoreBatch(ctx, entries); err != nil {
		t.Fatalf("RestoreBatch: %v", err)
	}
	if left, _ := svc.List(ctx, 1); len(left) != 0 {
		t.Fatalf("trash after restore = %d, want 0 (%+v)", len(left), left)
	}
	got, err := events.Get(ctx, master.ID)
	if err != nil {
		t.Fatalf("get master: %v", err)
	}
	if got.RecurrenceRule != originalRRULE {
		t.Fatalf("RRULE = %q, want %q", got.RecurrenceRule, originalRRULE)
	}
	if containsRFC3339(got.ExDates, instance) {
		t.Fatalf("EXDATE for %s still on master: %q", instance, got.ExDates)
	}
	if _, err := events.Get(ctx, override.ID); err != nil {
		t.Fatalf("hidden override not live: %v", err)
	}
}

// TestRestoreBatch_InstanceLogAndOverrideRowBothOrders covers the two
// overlapping delete forms of one occurrence: the EXDATE log entry and
// the soft-deleted override row. Either order in the batch must restore
// the occurrence cleanly.
func TestRestoreBatch_InstanceLogAndOverrideRowBothOrders(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	start := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	master, err := events.UpsertByUID(ctx, event.UpsertParams{
		UID:            "standup",
		CalendarID:     1,
		Title:          "Standup",
		StartTime:      start,
		EndTime:        start.Add(15 * time.Minute),
		RecurrenceRule: "FREQ=DAILY;COUNT=10",
	})
	if err != nil {
		t.Fatalf("create master: %v", err)
	}
	rec := time.Date(2026, 4, 3, 9, 0, 0, 0, time.UTC)
	override, err := events.UpsertByUID(ctx, event.UpsertParams{
		UID:          master.UID,
		CalendarID:   1,
		Title:        "Standup (guest)",
		StartTime:    rec,
		EndTime:      rec.Add(15 * time.Minute),
		RecurrenceID: rec.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("create override: %v", err)
	}
	// Each sub-case deletes the occurrence, reads the current trash rows
	// (the provenance log gets a fresh ID on every delete), and restores
	// in one order. This keeps both runs independent.
	runOnce := func(t *testing.T, reverse bool) {
		t.Helper()
		if err := events.DeleteInstance(ctx, master.UID, rec); err != nil {
			t.Fatalf("DeleteInstance: %v", err)
		}
		list, err := svc.List(ctx, 1)
		if err != nil || len(list) != 2 {
			t.Fatalf("List = (%d, %v), want 2", len(list), err)
		}
		var inst Entry
		var foundInst bool
		for _, e := range list {
			if e.Kind == KindEventInstance {
				inst, foundInst = e, true
			}
		}
		if !foundInst {
			t.Fatalf("no instance entry in %+v", list)
		}
		row := findEntry(t, list, KindEvent, override.ID)
		order := []Entry{inst, row}
		if reverse {
			order = []Entry{row, inst}
		}
		if err := svc.RestoreBatch(ctx, order); err != nil {
			t.Fatalf("RestoreBatch: %v", err)
		}
		got, err := events.Get(ctx, master.ID)
		if err != nil {
			t.Fatalf("get master: %v", err)
		}
		if containsRFC3339(got.ExDates, rec) {
			t.Fatalf("EXDATE for %s still on master: %q", rec, got.ExDates)
		}
		if _, err := events.Get(ctx, override.ID); err != nil {
			t.Fatalf("override not live: %v", err)
		}
		left, err := svc.List(ctx, 1)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(left) != 0 {
			t.Fatalf("trash = %d entries, want 0 (%+v)", len(left), left)
		}
	}

	t.Run("log first", func(t *testing.T) { runOnce(t, false) })
	t.Run("row first", func(t *testing.T) { runOnce(t, true) })
}

// TestPurgeBatch_SameSeriesForms purges an instance log, a truncation
// log, and the hidden override in one batch. The master stays live with
// its truncated rule. Nothing remains in the trash.
func TestPurgeBatch_SameSeriesForms(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	start := time.Date(2026, 4, 1, 14, 0, 0, 0, time.UTC)
	master, err := events.UpsertByUID(ctx, event.UpsertParams{
		UID:            "review",
		CalendarID:     1,
		Title:          "Review",
		StartTime:      start,
		EndTime:        start.Add(time.Hour),
		RecurrenceRule: "FREQ=WEEKLY;COUNT=10",
	})
	if err != nil {
		t.Fatalf("create master: %v", err)
	}
	originalRRULE := master.RecurrenceRule

	cutoff := time.Date(2026, 4, 22, 14, 0, 0, 0, time.UTC)
	override, err := events.UpsertByUID(ctx, event.UpsertParams{
		UID:          master.UID,
		CalendarID:   1,
		Title:        "Review (moved)",
		StartTime:    cutoff.Add(time.Hour),
		EndTime:      cutoff.Add(2 * time.Hour),
		RecurrenceID: cutoff.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("create override: %v", err)
	}
	if err := events.DeleteInstance(ctx, master.UID, time.Date(2026, 4, 8, 14, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("DeleteInstance: %v", err)
	}
	if err := events.DeleteFromInstance(ctx, master.UID, cutoff); err != nil {
		t.Fatalf("DeleteFromInstance: %v", err)
	}

	entries, err := svc.List(ctx, 1)
	if err != nil || len(entries) != 3 {
		t.Fatalf("List = (%d, %v), want 3", len(entries), err)
	}
	if err := svc.PurgeBatch(ctx, entries); err != nil {
		t.Fatalf("PurgeBatch: %v", err)
	}
	if left, _ := svc.List(ctx, 1); len(left) != 0 {
		t.Fatalf("trash after purge = %d, want 0 (%+v)", len(left), left)
	}
	got, err := events.Get(ctx, master.ID)
	if err != nil {
		t.Fatalf("master gone: %v", err)
	}
	if got.RecurrenceRule == originalRRULE {
		t.Fatalf("master RRULE un-truncated after purge: %q", got.RecurrenceRule)
	}
	if _, err := events.GetIncludingDeleted(ctx, override.ID); err == nil {
		t.Fatalf("hidden override row survived purge")
	}
}

// TestRestoreBatch_DedupRepeatedEntries repeats the same entry. The
// batch acts once and succeeds.
func TestRestoreBatch_DedupRepeatedEntries(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	td, err := todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Once"})
	if err != nil {
		t.Fatalf("create todo: %v", err)
	}
	if err := todos.Delete(ctx, td.ID); err != nil {
		t.Fatalf("delete todo: %v", err)
	}
	entries, err := svc.List(ctx, 1)
	if err != nil || len(entries) != 1 {
		t.Fatalf("List = (%d, %v), want 1", len(entries), err)
	}
	e := entries[0]
	if err := svc.RestoreBatch(ctx, []Entry{e, e, e}); err != nil {
		t.Fatalf("RestoreBatch repeated: %v", err)
	}
	if left, _ := svc.List(ctx, 1); len(left) != 0 {
		t.Fatalf("trash = %d, want 0", len(left))
	}
	if _, err := todos.Get(ctx, td.ID); err != nil {
		t.Fatalf("todo not live: %v", err)
	}
}

// TestRestoreBatch_UnknownKindFailsBeforeCommit verifies a bad kind
// reports its position and never opens the domain dispatch.
func TestRestoreBatch_UnknownKindFailsBeforeCommit(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	events := event.NewService(db, q)
	todos := todo.NewService(db, q)
	journals := journal.NewService(db, q)
	svc := NewService(db, events, todos, journals)
	ctx := context.Background()

	err := svc.RestoreBatch(ctx, []Entry{{Kind: Kind(99), ID: 1}})
	if err == nil {
		t.Fatalf("RestoreBatch with unknown kind succeeded")
	}
	var be *BatchError
	if !errors.As(err, &be) || be.Index != 1 {
		t.Fatalf("error = %v, want BatchError index 1", err)
	}
}

// containsRFC3339 reports whether a comma-separated time list holds ts in
// RFC3339 UTC form.
func containsRFC3339(list string, ts time.Time) bool {
	want := ts.UTC().Format(time.RFC3339)
	for _, part := range strings.Split(list, ",") {
		if part == want {
			return true
		}
	}
	return false
}
