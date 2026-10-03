package journal

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/douglasdemoura/chroncal/internal/storage"
)

// linkJournalCalendar links one calendar to a fresh account so its
// sync-record writes fire.
func linkJournalCalendar(t *testing.T, s *Service, calendarID int64, host string) {
	t.Helper()
	ctx := context.Background()
	acct, err := s.q.CreateAccount(ctx, storage.CreateAccountParams{
		Name:      host,
		ServerUrl: "https://" + host + ".example.com",
		AuthType:  "basic",
		Username:  "user",
	})
	if err != nil {
		t.Fatalf("create account %s: %v", host, err)
	}
	remote := "https://" + host + ".example.com/cal/"
	if err := s.q.LinkCalendarToAccount(ctx, storage.LinkCalendarToAccountParams{
		AccountID: &acct.ID,
		RemoteUrl: &remote,
		ID:        calendarID,
	}); err != nil {
		t.Fatalf("link calendar %d: %v", calendarID, err)
	}
}

func insertJournalCalendar(t *testing.T, s *Service, name string) int64 {
	t.Helper()
	res, err := s.db.ExecContext(context.Background(),
		`INSERT INTO calendars (name) VALUES (?)`, name)
	if err != nil {
		t.Fatalf("insert calendar %q: %v", name, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("calendar id: %v", err)
	}
	return id
}

// TestJournalUpdate_CrossCalendarMoveSyncIntents checks the dual-intent
// contract: source tombstone with retained href/ETag and dirty cleared,
// destination dirty create.
func TestJournalUpdate_CrossCalendarMoveSyncIntents(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	dst := insertJournalCalendar(t, svc, "Notes Private")
	linkJournalCalendar(t, svc, 1, "work-j")
	linkJournalCalendar(t, svc, dst, "personal-j")

	j := createJournal(t, svc)
	const href = "https://work-j.example.com/cal/note.ics"
	if err := svc.q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: 1, Uid: j.UID, OwnerType: "journal",
		RemoteUrl: href, Etag: `"e1"`, Dirty: 0, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source resource: %v", err)
	}

	moved, err := svc.Update(ctx, j.ID, UpdateParams{
		Summary:    j.Summary,
		StartDate:  j.StartDate,
		CalendarID: dst,
	})
	if err != nil {
		t.Fatalf("Update move: %v", err)
	}
	if moved.CalendarID != dst {
		t.Fatalf("moved calendar = %d, want %d", moved.CalendarID, dst)
	}

	src, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{CalendarID: 1, Uid: j.UID})
	if err != nil {
		t.Fatalf("source resource missing: %v", err)
	}
	if src.RemoteUrl != href || src.Etag != `"e1"` || src.Dirty != 0 {
		t.Errorf("source resource = url %q etag %q dirty %d; want retained href/etag, dirty cleared",
			src.RemoteUrl, src.Etag, src.Dirty)
	}
	var n int
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tombstones WHERE calendar_id = 1 AND uid = ? AND remote_url = ?`,
		j.UID, href).Scan(&n); err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	if n != 1 {
		t.Errorf("source tombstones = %d, want 1", n)
	}
	dstRes, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{CalendarID: dst, Uid: j.UID})
	if err != nil {
		t.Fatalf("destination resource missing: %v", err)
	}
	if dstRes.Dirty != 1 || dstRes.RemoteUrl != "" {
		t.Errorf("destination resource = dirty %d url %q, want dirty create",
			dstRes.Dirty, dstRes.RemoteUrl)
	}
}

// TestJournalUpdate_CrossCalendarMoveCarriesSeries moves master and override
// rows together.
func TestJournalUpdate_CrossCalendarMoveCarriesSeries(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	dst := insertJournalCalendar(t, svc, "Notes Private 2")
	linkJournalCalendar(t, svc, 1, "work-jb")
	linkJournalCalendar(t, svc, dst, "personal-jb")

	master, err := svc.UpsertByUID(ctx, UpsertParams{
		UID: "journal-series-move", CalendarID: 1, Summary: "Recurring note",
		RecurrenceRule: "FREQ=DAILY;COUNT=3",
	})
	if err != nil {
		t.Fatalf("seed master: %v", err)
	}
	if _, err := svc.UpsertByUID(ctx, UpsertParams{
		UID: master.UID, CalendarID: 1, Summary: "Note override",
		RecurrenceID: "2026-04-02T09:00:00Z",
	}); err != nil {
		t.Fatalf("seed override: %v", err)
	}
	if err := svc.q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: 1, Uid: master.UID, OwnerType: "journal",
		RemoteUrl: "https://work-jb.example.com/cal/s.ics", Etag: "e", Dirty: 0,
		SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source resource: %v", err)
	}

	if _, err := svc.Update(ctx, master.ID, UpdateParams{
		Summary:    master.Summary,
		StartDate:  master.StartDate,
		CalendarID: dst,
	}); err != nil {
		t.Fatalf("Update move: %v", err)
	}

	var srcRows, dstRows int
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM journals WHERE uid = ? AND calendar_id = 1`, master.UID).Scan(&srcRows); err != nil {
		t.Fatalf("count source rows: %v", err)
	}
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM journals WHERE uid = ? AND calendar_id = ?`, master.UID, dst).Scan(&dstRows); err != nil {
		t.Fatalf("count destination rows: %v", err)
	}
	if srcRows != 0 || dstRows != 2 {
		t.Fatalf("rows after move: source %d, destination %d; want 0 and 2", srcRows, dstRows)
	}
}

// TestJournalUpdate_CrossCalendarMoveAtomicOnIntentFailure rolls the row move
// back when the tombstone write fails.
func TestJournalUpdate_CrossCalendarMoveAtomicOnIntentFailure(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	dst := insertJournalCalendar(t, svc, "Notes Private 3")
	linkJournalCalendar(t, svc, 1, "work-jc")
	linkJournalCalendar(t, svc, dst, "personal-jc")

	j := createJournal(t, svc)
	if err := svc.q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: 1, Uid: j.UID, OwnerType: "journal",
		RemoteUrl: "https://work-jc.example.com/cal/n.ics", Etag: "e", Dirty: 0,
		SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source resource: %v", err)
	}
	if _, err := svc.db.ExecContext(ctx, `DROP TABLE tombstones`); err != nil {
		t.Fatalf("drop tombstones: %v", err)
	}

	if _, err := svc.Update(ctx, j.ID, UpdateParams{
		Summary: j.Summary, StartDate: j.StartDate, CalendarID: dst,
	}); err == nil {
		t.Fatal("move succeeded but the tombstone write failed")
	}
	got, err := svc.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("get journal: %v", err)
	}
	if got.CalendarID != 1 {
		t.Fatalf("journal calendar = %d, want 1 (move rolled back)", got.CalendarID)
	}
	if _, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: dst, Uid: j.UID,
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("destination resource = %v, want none (rolled back)", err)
	}
}
