package todo

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/douglasdemoura/chroncal/internal/storage"
)

// linkTodoCalendar links one calendar to a fresh account so its sync-record
// writes fire.
func linkTodoCalendar(t *testing.T, s *Service, calendarID int64, host string) {
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

func insertTodoCalendar(t *testing.T, s *Service, name string) int64 {
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

// TestTodoUpdate_CrossCalendarMoveSyncIntents checks the same dual-intent
// contract as the event move: the source keeps its href/ETag behind a
// tombstone and clears dirty; the destination gets a dirty create.
func TestTodoUpdate_CrossCalendarMoveSyncIntents(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	dst := insertTodoCalendar(t, svc, "Tasks Private")
	linkTodoCalendar(t, svc, 1, "work")
	linkTodoCalendar(t, svc, dst, "personal")

	td := createTodo(t, svc)
	const href = "https://work.example.com/cal/task.ics"
	if err := svc.q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: 1, Uid: td.UID, OwnerType: "todo",
		RemoteUrl: href, Etag: `"e1"`, Dirty: 0, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source resource: %v", err)
	}

	moved, err := svc.Update(ctx, td.ID, UpdateParams{
		Summary:    td.Summary,
		DueDate:    td.DueDate,
		CalendarID: dst,
	})
	if err != nil {
		t.Fatalf("Update move: %v", err)
	}
	if moved.CalendarID != dst {
		t.Fatalf("moved calendar = %d, want %d", moved.CalendarID, dst)
	}

	src, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{CalendarID: 1, Uid: td.UID})
	if err != nil {
		t.Fatalf("source resource missing: %v", err)
	}
	if src.RemoteUrl != href || src.Etag != `"e1"` || src.Dirty != 0 {
		t.Errorf("source resource = url %q etag %q dirty %d; want href+etag retained, dirty cleared",
			src.RemoteUrl, src.Etag, src.Dirty)
	}
	var n int
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tombstones WHERE calendar_id = 1 AND uid = ? AND remote_url = ?`,
		td.UID, href).Scan(&n); err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	if n != 1 {
		t.Errorf("source tombstones = %d, want 1", n)
	}
	dstRes, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{CalendarID: dst, Uid: td.UID})
	if err != nil {
		t.Fatalf("destination resource missing: %v", err)
	}
	if dstRes.Dirty != 1 || dstRes.RemoteUrl != "" {
		t.Errorf("destination resource = dirty %d url %q, want dirty create",
			dstRes.Dirty, dstRes.RemoteUrl)
	}
}

// TestTodoUpdate_CrossCalendarMoveCarriesSeries moves master and overrides
// together and keeps their categories.
func TestTodoUpdate_CrossCalendarMoveCarriesSeries(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	dst := insertTodoCalendar(t, svc, "Tasks Private 2")
	linkTodoCalendar(t, svc, 1, "workb")
	linkTodoCalendar(t, svc, dst, "personalb")

	master, err := svc.UpsertByUID(ctx, UpsertParams{
		UID: "todo-series-move", CalendarID: 1, Summary: "Recurring task",
		RecurrenceRule: "FREQ=DAILY;COUNT=3", Categories: "Work",
	})
	if err != nil {
		t.Fatalf("seed master: %v", err)
	}
	if _, err := svc.UpsertByUID(ctx, UpsertParams{
		UID: master.UID, CalendarID: 1, Summary: "Task override",
		RecurrenceID: "2026-04-02T09:00:00Z",
	}); err != nil {
		t.Fatalf("seed override: %v", err)
	}
	if err := svc.q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: 1, Uid: master.UID, OwnerType: "todo",
		RemoteUrl: "https://workb.example.com/cal/s.ics", Etag: "e", Dirty: 0,
		SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source resource: %v", err)
	}

	if _, err := svc.Update(ctx, master.ID, UpdateParams{
		Summary:    master.Summary,
		CalendarID: dst,
		Categories: "Work",
	}); err != nil {
		t.Fatalf("Update move: %v", err)
	}

	var srcRows, dstRows int
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM todos WHERE uid = ? AND calendar_id = 1`, master.UID).Scan(&srcRows); err != nil {
		t.Fatalf("count source rows: %v", err)
	}
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM todos WHERE uid = ? AND calendar_id = ?`, master.UID, dst).Scan(&dstRows); err != nil {
		t.Fatalf("count destination rows: %v", err)
	}
	if srcRows != 0 || dstRows != 2 {
		t.Fatalf("rows after move: source %d, destination %d; want 0 and 2", srcRows, dstRows)
	}
}

// TestTodoUpdate_CrossCalendarMoveAtomicOnIntentFailure rolls the row move
// back when the tombstone write fails.
func TestTodoUpdate_CrossCalendarMoveAtomicOnIntentFailure(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	dst := insertTodoCalendar(t, svc, "Tasks Private 3")
	linkTodoCalendar(t, svc, 1, "workc")
	linkTodoCalendar(t, svc, dst, "personalc")

	td := createTodo(t, svc)
	if err := svc.q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: 1, Uid: td.UID, OwnerType: "todo",
		RemoteUrl: "https://workc.example.com/cal/t.ics", Etag: "e", Dirty: 0,
		SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source resource: %v", err)
	}
	if _, err := svc.db.ExecContext(ctx, `DROP TABLE tombstones`); err != nil {
		t.Fatalf("drop tombstones: %v", err)
	}

	if _, err := svc.Update(ctx, td.ID, UpdateParams{
		Summary: td.Summary, DueDate: td.DueDate, CalendarID: dst,
	}); err == nil {
		t.Fatal("move succeeded but the tombstone write failed")
	}
	got, err := svc.Get(ctx, td.ID)
	if err != nil {
		t.Fatalf("get todo: %v", err)
	}
	if got.CalendarID != 1 {
		t.Fatalf("todo calendar = %d, want 1 (move rolled back)", got.CalendarID)
	}
	if _, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: dst, Uid: td.UID,
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("destination resource = %v, want none (rolled back)", err)
	}
}
