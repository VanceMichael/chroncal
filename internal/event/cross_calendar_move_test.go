package event

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/douglasdemoura/chroncal/internal/model"
	"github.com/douglasdemoura/chroncal/internal/storage"
)

// linkTestCalendar links one calendar to a fresh account so its sync-record
// writes fire. Each call uses a distinct server URL so two calendars can map
// to two accounts (the "work to personal" move scenario).
func linkTestCalendar(t *testing.T, s *Service, calendarID int64, host string) {
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

func seedPushedResource(t *testing.T, s *Service, calendarID int64, uid, href string) {
	t.Helper()
	if err := s.q.UpsertSyncResource(context.Background(), storage.UpsertSyncResourceParams{
		CalendarID:   calendarID,
		Uid:          uid,
		OwnerType:    "event",
		RemoteUrl:    href,
		Etag:         `"etag-src"`,
		Dirty:        0,
		SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert source sync resource: %v", err)
	}
}

func moveParams(e Event, calendarID int64) UpdateParams {
	return UpdateParams{
		Title:          e.Title,
		Description:    e.Description,
		Location:       e.Location,
		StartTime:      e.StartTime,
		EndTime:        e.EndTime,
		AllDay:         e.AllDay,
		RecurrenceRule: e.RecurrenceRule,
		CalendarID:     calendarID,
		Timezone:       e.Timezone,
		Status:         e.Status,
		Transp:         e.Transp,
		Priority:       e.Priority,
		Class:          e.Class,
		URL:            e.URL,
		ConferenceURI:  e.ConferenceURI,
		Categories:     e.Categories,
		ExDates:        e.ExDates,
		RDates:         e.RDates,
		Geo:            e.Geo,
		DurationValue:  "",
	}
}

func getSR(t *testing.T, s *Service, calendarID int64, uid string) storage.SyncResource {
	t.Helper()
	r, err := s.q.GetSyncResource(context.Background(), storage.GetSyncResourceParams{
		CalendarID: calendarID,
		Uid:        uid,
	})
	if err != nil {
		t.Fatalf("get sync resource cal=%d uid=%s: %v", calendarID, uid, err)
	}
	return r
}

func tombstoneCount(t *testing.T, s *Service, calendarID int64, uid string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM tombstones WHERE calendar_id = ? AND uid = ?`,
		calendarID, uid).Scan(&n); err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	return n
}

// TestUpdate_CrossCalendarMoveSyncedToSynced is the core contract. Moving a
// pushed resource from one linked calendar to another must, in one
// transaction: keep the source sync_resource with its href and ETag but
// clear dirty, record a source tombstone with that href, and create a dirty
// destination resource with no href (a first-time push on the destination).
func TestUpdate_CrossCalendarMoveSyncedToSynced(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	personal := insertCalendar(t, svc, "Private")
	linkTestCalendar(t, svc, 1, "work")
	linkTestCalendar(t, svc, personal, "personal")

	evt := createEvent(t, svc)
	const href = "https://work.example.com/cal/moved.ics"
	seedPushedResource(t, svc, 1, evt.UID, href)

	moved, err := svc.Update(ctx, evt.ID, moveParams(evt, personal))
	if err != nil {
		t.Fatalf("Update move: %v", err)
	}
	if moved.CalendarID != personal {
		t.Fatalf("moved.CalendarID = %d, want %d", moved.CalendarID, personal)
	}

	src := getSR(t, svc, 1, evt.UID)
	if src.RemoteUrl != href {
		t.Errorf("source remote_url = %q, want %q retained for DELETE", src.RemoteUrl, href)
	}
	if src.Etag != `"etag-src"` {
		t.Errorf("source etag = %q, want the conditional-DELETE etag retained", src.Etag)
	}
	if src.Dirty != 0 {
		t.Errorf("source dirty = %d, want 0 (the move must not re-PUT to the source)", src.Dirty)
	}
	if n := tombstoneCount(t, svc, 1, evt.UID); n != 1 {
		t.Errorf("source tombstones = %d, want 1", n)
	}

	dst := getSR(t, svc, personal, evt.UID)
	if dst.Dirty != 1 {
		t.Errorf("destination dirty = %d, want 1 (needs push)", dst.Dirty)
	}
	if dst.RemoteUrl != "" {
		t.Errorf("destination remote_url = %q, want empty (first-time create)", dst.RemoteUrl)
	}
}

// TestUpdate_CrossCalendarMoveCarriesSeriesAndChildren proves the local
// half of the ownership migration. The master row, every override row,
// categories, attendees, and alarms all end up on the destination calendar.
// The child collections key on the row id, so they follow the moved row
// without a rewrite.
func TestUpdate_CrossCalendarMoveCarriesSeriesAndChildren(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	personal := insertCalendar(t, svc, "Private")
	linkTestCalendar(t, svc, 1, "work")
	linkTestCalendar(t, svc, personal, "personal")

	master, err := svc.UpsertByUID(ctx, UpsertParams{
		UID:            "series-move",
		CalendarID:     1,
		Title:          "Standup",
		StartTime:      time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC),
		EndTime:        time.Date(2026, 4, 1, 9, 15, 0, 0, time.UTC),
		RecurrenceRule: "FREQ=DAILY;COUNT=5",
		Categories:     "Work",
	})
	if err != nil {
		t.Fatalf("seed master: %v", err)
	}
	override, err := svc.UpsertByUID(ctx, UpsertParams{
		UID:          master.UID,
		CalendarID:   1,
		Title:        "Standup moved room",
		StartTime:    time.Date(2026, 4, 3, 9, 0, 0, 0, time.UTC),
		EndTime:      time.Date(2026, 4, 3, 9, 15, 0, 0, time.UTC),
		RecurrenceID: "2026-04-03T09:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed override: %v", err)
	}
	seedPushedResource(t, svc, 1, master.UID, "https://work.example.com/cal/series.ics")

	attendees := []model.Attendee{{
		Email: "a@example.com", Role: "REQ-PARTICIPANT", RSVPStatus: "NEEDS-ACTION",
	}}
	alarms := []model.Alarm{{
		Action: "DISPLAY", TriggerValue: "-PT15M", Description: "Reminder", Related: "START",
	}}
	p := moveParams(master, personal)
	p.Categories = "Work,Personal"
	if _, err := svc.UpdateWithRelations(ctx, master.ID, p, attendees, alarms); err != nil {
		t.Fatalf("UpdateWithRelations move: %v", err)
	}

	gotMaster, err := svc.Get(ctx, master.ID)
	if err != nil {
		t.Fatalf("get master: %v", err)
	}
	if gotMaster.CalendarID != personal {
		t.Errorf("master calendar = %d, want %d", gotMaster.CalendarID, personal)
	}
	var rowsOnSrc, rowsOnDst int
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events WHERE uid = ? AND calendar_id = 1`, master.UID).Scan(&rowsOnSrc); err != nil {
		t.Fatalf("count source rows: %v", err)
	}
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events WHERE uid = ? AND calendar_id = ?`, master.UID, personal).Scan(&rowsOnDst); err != nil {
		t.Fatalf("count destination rows: %v", err)
	}
	if rowsOnSrc != 0 {
		t.Errorf("rows left on source = %d, want 0", rowsOnSrc)
	}
	if rowsOnDst != 2 {
		t.Errorf("rows on destination = %d, want 2 (master + override)", rowsOnDst)
	}
	gotOverride, err := svc.Get(ctx, override.ID)
	if err != nil {
		t.Fatalf("get override: %v", err)
	}
	if gotOverride.CalendarID != personal {
		t.Errorf("override calendar = %d, want %d", gotOverride.CalendarID, personal)
	}
	if n := countAttendees(t, svc.db, master.ID); n != 1 {
		t.Errorf("attendees after move = %d, want 1", n)
	}
	if n := countAlarms(t, svc.db, master.ID); n != 1 {
		t.Errorf("alarms after move = %d, want 1", n)
	}
	var cats int
	if err := svc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM event_categories WHERE event_id = ?`, master.ID).Scan(&cats); err != nil {
		t.Fatalf("count categories: %v", err)
	}
	if cats != 2 {
		t.Errorf("categories after move = %d, want 2", cats)
	}
}

// TestUpdate_CrossCalendarMoveSyncedToLocal leaves no destination
// sync_resource but still records the source DELETE intent.
func TestUpdate_CrossCalendarMoveSyncedToLocal(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	personal := insertCalendar(t, svc, "Private")
	linkTestCalendar(t, svc, 1, "work")

	evt := createEvent(t, svc)
	seedPushedResource(t, svc, 1, evt.UID, "https://work.example.com/cal/m.ics")

	if _, err := svc.Update(ctx, evt.ID, moveParams(evt, personal)); err != nil {
		t.Fatalf("Update move to local calendar: %v", err)
	}
	if n := tombstoneCount(t, svc, 1, evt.UID); n != 1 {
		t.Errorf("source tombstones = %d, want 1", n)
	}
	if _, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: personal, Uid: evt.UID,
	}); err != sql.ErrNoRows {
		t.Fatalf("destination sync resource = %v, want no row on a local-only calendar", err)
	}
}

// TestUpdate_CrossCalendarMoveLocalToSynced creates only the destination
// push intent. The source never had a remote resource to DELETE.
func TestUpdate_CrossCalendarMoveLocalToSynced(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	personal := insertCalendar(t, svc, "Private")
	linkTestCalendar(t, svc, personal, "personal")

	evt := createEvent(t, svc)
	if _, err := svc.Update(ctx, evt.ID, moveParams(evt, personal)); err != nil {
		t.Fatalf("Update move: %v", err)
	}
	if n := tombstoneCount(t, svc, 1, evt.UID); n != 0 {
		t.Errorf("source tombstones = %d, want 0 (never synced)", n)
	}
	dst := getSR(t, svc, personal, evt.UID)
	if dst.Dirty != 1 || dst.RemoteUrl != "" {
		t.Errorf("destination resource = dirty %d url %q, want dirty create", dst.Dirty, dst.RemoteUrl)
	}
}

// TestUpdate_CrossCalendarMoveNeverPushedDropsSourceRow covers the case where
// the source row exists but never reached its server (empty remote_url,
// dirty). The move must drop that placeholder, or a later source push would
// create the moved item at a fresh href on the source server.
func TestUpdate_CrossCalendarMoveNeverPushedDropsSourceRow(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	personal := insertCalendar(t, svc, "Private")
	linkTestCalendar(t, svc, 1, "work")
	linkTestCalendar(t, svc, personal, "personal")

	evt := createEvent(t, svc)
	if err := svc.q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: 1, Uid: evt.UID, OwnerType: "event",
		RemoteUrl: "", Etag: "", Dirty: 1, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert unpushed source resource: %v", err)
	}

	if _, err := svc.Update(ctx, evt.ID, moveParams(evt, personal)); err != nil {
		t.Fatalf("Update move: %v", err)
	}
	if _, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: 1, Uid: evt.UID,
	}); err != sql.ErrNoRows {
		t.Fatalf("source sync resource = %v, want deleted (never pushed)", err)
	}
	if n := tombstoneCount(t, svc, 1, evt.UID); n != 0 {
		t.Errorf("tombstones = %d, want 0 (nothing to delete remotely)", n)
	}
	dst := getSR(t, svc, personal, evt.UID)
	if dst.Dirty != 1 {
		t.Errorf("destination dirty = %d, want 1", dst.Dirty)
	}
}

// TestUpdate_CrossCalendarMoveKeepsDestinationIdentity proves that a
// destination which already tracks the UID pushes to the existing href
// instead of creating a duplicate resource.
func TestUpdate_CrossCalendarMoveKeepsDestinationIdentity(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	personal := insertCalendar(t, svc, "Private")
	linkTestCalendar(t, svc, 1, "work")
	linkTestCalendar(t, svc, personal, "personal")

	evt := createEvent(t, svc)
	seedPushedResource(t, svc, 1, evt.UID, "https://work.example.com/cal/m.ics")
	const dstHref = "https://personal.example.com/cal/existing.ics"
	if err := svc.q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: personal, Uid: evt.UID, OwnerType: "event",
		RemoteUrl: dstHref, Etag: `"etag-dst"`, Dirty: 0, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("upsert destination resource: %v", err)
	}

	if _, err := svc.Update(ctx, evt.ID, moveParams(evt, personal)); err != nil {
		t.Fatalf("Update move: %v", err)
	}
	dst := getSR(t, svc, personal, evt.UID)
	if dst.RemoteUrl != dstHref {
		t.Errorf("destination remote_url = %q, want %q (update, not create)", dst.RemoteUrl, dstHref)
	}
	if dst.Etag != `"etag-dst"` || dst.Dirty != 1 {
		t.Errorf("destination etag=%q dirty=%d, want etag kept and dirty", dst.Etag, dst.Dirty)
	}
}

// TestUpdate_CrossCalendarMoveAtomicOnIntentFailure forces the tombstone
// write to fail. The row move and the destination intent must roll back with
// it. The entry must not stay "moved" with only half its sync intent.
func TestUpdate_CrossCalendarMoveAtomicOnIntentFailure(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	personal := insertCalendar(t, svc, "Private")
	linkTestCalendar(t, svc, 1, "work")
	linkTestCalendar(t, svc, personal, "personal")

	evt := createEvent(t, svc)
	seedPushedResource(t, svc, 1, evt.UID, "https://work.example.com/cal/m.ics")

	if _, err := svc.db.ExecContext(ctx, `DROP TABLE tombstones`); err != nil {
		t.Fatalf("drop tombstones: %v", err)
	}

	if _, err := svc.Update(ctx, evt.ID, moveParams(evt, personal)); err == nil {
		t.Fatal("move succeeded but the tombstone write failed; the error was discarded")
	}
	got, err := svc.Get(ctx, evt.ID)
	if err != nil {
		t.Fatalf("get event after failed move: %v", err)
	}
	if got.CalendarID != 1 {
		t.Fatalf("event calendar = %d, want 1 (row move rolled back)", got.CalendarID)
	}
	if _, err := svc.q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: personal, Uid: evt.UID,
	}); err != sql.ErrNoRows {
		t.Fatalf("destination sync resource = %v, want none (intent rolled back)", err)
	}
}
