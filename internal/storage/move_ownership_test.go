package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// setupTwoLinkedCalendars opens a test database and links the seeded
// calendar and a new second calendar to distinct accounts, so
// MoveResourceOwnership acts on both sides.
func setupTwoLinkedCalendars(t *testing.T) (*sql.DB, *Queries, int64, int64) {
	t.Helper()
	db, q, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	cals, err := q.ListCalendars(ctx)
	if err != nil {
		t.Fatalf("list calendars: %v", err)
	}
	src := cals[0].ID
	dstCal, err := q.CreateCalendar(ctx, CreateCalendarParams{Name: "Private", Color: "#0284C7"})
	if err != nil {
		t.Fatalf("create destination calendar: %v", err)
	}
	dst := dstCal.ID

	link := func(calendarID int64, host string) {
		acct, err := q.CreateAccount(ctx, CreateAccountParams{
			Name: host, ServerUrl: "https://" + host + ".example.com",
			AuthType: "basic", Username: "user",
		})
		if err != nil {
			t.Fatalf("create account %s: %v", host, err)
		}
		remote := "https://" + host + ".example.com/cal/"
		if err := q.LinkCalendarToAccount(ctx, LinkCalendarToAccountParams{
			ID: calendarID, AccountID: &acct.ID, RemoteUrl: &remote,
		}); err != nil {
			t.Fatalf("link calendar %d: %v", calendarID, err)
		}
	}
	link(src, "work")
	link(dst, "personal")
	return db, q, src, dst
}

func syncRow(t *testing.T, q *Queries, calendarID int64, uid string) SyncResource {
	t.Helper()
	r, err := q.GetSyncResource(context.Background(), GetSyncResourceParams{
		CalendarID: calendarID, Uid: uid,
	})
	if err != nil {
		t.Fatalf("get sync resource cal=%d uid=%s: %v", calendarID, uid, err)
	}
	return r
}

// TestMoveResourceOwnership_PushedSource records a source tombstone, clears
// the source dirty flag while keeping href/ETag, and creates a dirty
// destination create.
func TestMoveResourceOwnership_PushedSource(t *testing.T) {
	db, q, src, dst := setupTwoLinkedCalendars(t)
	ctx := context.Background()
	const uid = "pushed-move"
	const href = "https://work.example.com/cal/pushed-move.ics"
	if err := q.UpsertSyncResource(ctx, UpsertSyncResourceParams{
		CalendarID: src, Uid: uid, OwnerType: "event",
		RemoteUrl: href, Etag: `"e1"`, Dirty: 1, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source: %v", err)
	}

	if err := MoveResourceOwnership(ctx, db, src, dst, uid, "event"); err != nil {
		t.Fatalf("MoveResourceOwnership: %v", err)
	}

	s := syncRow(t, q, src, uid)
	if s.RemoteUrl != href || s.Etag != `"e1"` || s.Dirty != 0 {
		t.Errorf("source = url %q etag %q dirty %d; want href/etag kept and dirty cleared",
			s.RemoteUrl, s.Etag, s.Dirty)
	}
	ts, err := q.ListTombstonesByCalendar(ctx, src)
	if err != nil {
		t.Fatalf("list tombstones: %v", err)
	}
	if len(ts) != 1 || ts[0].RemoteUrl != href {
		t.Fatalf("tombstones = %+v, want one row with the source href", ts)
	}
	d := syncRow(t, q, dst, uid)
	if d.Dirty != 1 || d.RemoteUrl != "" {
		t.Errorf("destination = dirty %d url %q, want dirty create", d.Dirty, d.RemoteUrl)
	}
}

// TestMoveResourceOwnership_NeverPushedSource drops the source placeholder
// when it never reached the server. A lingering dirty row would create the
// moved item on the source server at a fresh href.
func TestMoveResourceOwnership_NeverPushedSource(t *testing.T) {
	db, q, src, dst := setupTwoLinkedCalendars(t)
	ctx := context.Background()
	const uid = "unpushed-move"
	if err := q.UpsertSyncResource(ctx, UpsertSyncResourceParams{
		CalendarID: src, Uid: uid, OwnerType: "event",
		RemoteUrl: "", Etag: "", Dirty: 1, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source: %v", err)
	}

	if err := MoveResourceOwnership(ctx, db, src, dst, uid, "event"); err != nil {
		t.Fatalf("MoveResourceOwnership: %v", err)
	}
	if _, err := q.GetSyncResource(ctx, GetSyncResourceParams{
		CalendarID: src, Uid: uid,
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("source row = %v, want removed", err)
	}
	if ts, _ := q.ListTombstonesByCalendar(ctx, src); len(ts) != 0 {
		t.Errorf("tombstones = %d, want 0 (nothing to delete)", len(ts))
	}
	if d := syncRow(t, q, dst, uid); d.Dirty != 1 {
		t.Errorf("destination dirty = %d, want 1", d.Dirty)
	}
}

// TestMoveResourceOwnership_NoSourceRow only creates the destination intent.
func TestMoveResourceOwnership_NoSourceRow(t *testing.T) {
	db, q, src, dst := setupTwoLinkedCalendars(t)
	ctx := context.Background()
	const uid = "local-source-move"

	if err := MoveResourceOwnership(ctx, db, src, dst, uid, "todo"); err != nil {
		t.Fatalf("MoveResourceOwnership: %v", err)
	}
	if _, err := q.GetSyncResource(ctx, GetSyncResourceParams{
		CalendarID: src, Uid: uid,
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("source row = %v, want none", err)
	}
	if d := syncRow(t, q, dst, uid); d.Dirty != 1 || d.OwnerType != "todo" {
		t.Errorf("destination = dirty %d owner %q, want dirty todo", d.Dirty, d.OwnerType)
	}
}

// TestMoveResourceOwnership_SameCalendarDelegatesToDirtyMark proves the
// same-calendar path keeps the plain dirty-mark contract.
func TestMoveResourceOwnership_SameCalendarDelegatesToDirtyMark(t *testing.T) {
	db, q, src, _ := setupTwoLinkedCalendars(t)
	ctx := context.Background()
	const uid = "same-calendar-edit"
	if err := q.UpsertSyncResource(ctx, UpsertSyncResourceParams{
		CalendarID: src, Uid: uid, OwnerType: "event",
		RemoteUrl: "https://work.example.com/cal/x.ics", Etag: "e", Dirty: 0,
		SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := MoveResourceOwnership(ctx, db, src, src, uid, "event"); err != nil {
		t.Fatalf("MoveResourceOwnership: %v", err)
	}
	r := syncRow(t, q, src, uid)
	if r.Dirty != 1 || r.RemoteUrl == "" {
		t.Errorf("same-calendar result = dirty %d url %q, want dirty with href kept",
			r.Dirty, r.RemoteUrl)
	}
	if ts, _ := q.ListTombstonesByCalendar(ctx, src); len(ts) != 0 {
		t.Errorf("same-calendar edit created %d tombstones, want 0", len(ts))
	}
}

// TestMoveResourceOwnership_RollsBackWithCallerTransaction proves the intent
// writes join the caller's transaction. A rollback leaves no half state.
func TestMoveResourceOwnership_RollsBackWithCallerTransaction(t *testing.T) {
	db, q, src, dst := setupTwoLinkedCalendars(t)
	ctx := context.Background()
	const uid = "tx-rollback"
	const href = "https://work.example.com/cal/tx.ics"
	if err := q.UpsertSyncResource(ctx, UpsertSyncResourceParams{
		CalendarID: src, Uid: uid, OwnerType: "event",
		RemoteUrl: href, Etag: "e", Dirty: 0, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := MoveResourceOwnership(ctx, tx, src, dst, uid, "event"); err != nil {
		t.Fatalf("MoveResourceOwnership: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if ts, _ := q.ListTombstonesByCalendar(ctx, src); len(ts) != 0 {
		t.Errorf("tombstones after rollback = %d, want 0", len(ts))
	}
	if _, err := q.GetSyncResource(ctx, GetSyncResourceParams{
		CalendarID: dst, Uid: uid,
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("destination row after rollback = %v, want none", err)
	}
	r := syncRow(t, q, src, uid)
	if r.Dirty != 0 {
		t.Errorf("source dirty after rollback = %d, want 0", r.Dirty)
	}
}
