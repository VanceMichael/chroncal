package sync

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/douglasdemoura/chroncal/internal/auth"
	"github.com/douglasdemoura/chroncal/internal/caldav"
	"github.com/douglasdemoura/chroncal/internal/storage"
)

// moveFixture holds the ids and paths the cross-calendar move tests share.
type moveFixture struct {
	srcID   int64
	dstID   int64
	srcRef  string
	dstRef  string
	srcHref string
	uid     string
}

func countTombstones(t *testing.T, db *sql.DB, calendarID int64, uid string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM tombstones WHERE calendar_id = ? AND uid = ?`,
		calendarID, uid).Scan(&n); err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	return n
}

func getDstResource(t *testing.T, q *storage.Queries, f moveFixture) storage.SyncResource {
	t.Helper()
	r, err := q.GetSyncResource(context.Background(), storage.GetSyncResourceParams{
		CalendarID: f.dstID, Uid: f.uid,
	})
	if err != nil {
		t.Fatalf("get destination resource: %v", err)
	}
	return r
}

// setupMoveFixtures links the seeded calendar (source) and a new calendar
// (destination) to two accounts on the same test server, with distinct
// collections.
func setupMoveFixtures(t *testing.T, engine *Engine, q *storage.Queries, serverURL string) moveFixture {
	t.Helper()
	ctx := context.Background()

	mkAccount := func(host string) storage.Account {
		acct, err := q.CreateAccount(ctx, storage.CreateAccountParams{
			Name: host, ServerUrl: serverURL, AuthType: "basic", Username: "user",
		})
		if err != nil {
			t.Fatalf("create account %s: %v", host, err)
		}
		engine.credStore.(*mockCredStore).creds[acct.ID] = auth.Credential{
			AccountID: acct.ID, Username: "user", Password: "secret",
		}
		return acct
	}
	srcAcct := mkAccount("work")
	dstAcct := mkAccount("personal")

	cals, err := q.ListCalendars(ctx)
	if err != nil {
		t.Fatalf("ListCalendars: %v", err)
	}
	srcID := cals[0].ID
	dstCal, err := q.CreateCalendar(ctx, storage.CreateCalendarParams{
		Name: "Private", Color: "#0284C7",
	})
	if err != nil {
		t.Fatalf("CreateCalendar: %v", err)
	}
	dstID := dstCal.ID

	srcRef := serverURL + "/calsrc/"
	dstRef := serverURL + "/caldst/"
	if err := q.LinkCalendarToAccount(ctx, storage.LinkCalendarToAccountParams{
		ID: srcID, AccountID: &srcAcct.ID, RemoteUrl: &srcRef,
	}); err != nil {
		t.Fatalf("link source: %v", err)
	}
	if err := q.LinkCalendarToAccount(ctx, storage.LinkCalendarToAccountParams{
		ID: dstID, AccountID: &dstAcct.ID, RemoteUrl: &dstRef,
	}); err != nil {
		t.Fatalf("link destination: %v", err)
	}
	return moveFixture{
		srcID:   srcID,
		dstID:   dstID,
		srcRef:  srcRef,
		dstRef:  dstRef,
		srcHref: srcRef + "moved-meeting.ics",
		uid:     "moved-meeting",
	}
}

// seedMovedRows writes the post-move row and intent state: the live event on
// the destination with a dirty create intent, and the source sync_resource
// plus tombstone that record the source DELETE.
func seedMovedRows(t *testing.T, db *sql.DB, q *storage.Queries, f moveFixture) {
	t.Helper()
	ctx := context.Background()
	insertTestEvent(t, db, f.dstID, f.uid)
	if err := q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: f.srcID, Uid: f.uid, OwnerType: "event",
		RemoteUrl: f.srcHref, Etag: `"src-etag"`, Dirty: 0, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("source sync resource: %v", err)
	}
	if err := q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: f.dstID, Uid: f.uid, OwnerType: "event",
		RemoteUrl: "", Etag: "", Dirty: 1, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("destination sync resource: %v", err)
	}
	if err := q.CreateTombstone(ctx, storage.CreateTombstoneParams{
		CalendarID: f.srcID, Uid: f.uid, RemoteUrl: f.srcHref,
	}); err != nil {
		t.Fatalf("source tombstone: %v", err)
	}
}

const movedMeetingICS = `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//chroncal//tests//EN
BEGIN:VEVENT
UID:moved-meeting
DTSTAMP:20260403T120000Z
DTSTART:20260403T120000Z
DTEND:20260403T130000Z
SUMMARY:Moved meeting
END:VEVENT
END:VCALENDAR
`

// TestCrossCalendarMove_EndToEnd runs the full cycle: the source calendar
// converges its DELETE while an empty initial snapshot must not resurrect or
// absence-delete the moved row; the destination calendar then PUTs a new
// resource. One side succeeding must not clear the other side's intent.
func TestCrossCalendarMove_EndToEnd(t *testing.T) {
	t.Parallel()

	var deleteIfMatch string
	var deleteCalls, putCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodDelete && strings.HasPrefix(path, "/calsrc/"):
			deleteCalls++
			deleteIfMatch = r.Header.Get("If-Match")
			w.WriteHeader(http.StatusNoContent)
			return
		case r.Method == http.MethodPut && strings.HasPrefix(path, "/caldst/"):
			putCalls++
			if got := r.Header.Get("If-Match"); got != "" {
				t.Errorf("destination create PUT carried If-Match %q, want empty", got)
			}
			w.Header().Set("ETag", `"dst-etag"`)
			w.WriteHeader(http.StatusCreated)
			return
		case r.Method == "PROPFIND":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:"/>`)
			return
		case r.Method != "REPORT":
			t.Errorf("unexpected %s %s", r.Method, path)
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
			return
		}

		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case strings.Contains(path, "/calsrc/"):
			// Initial snapshot with no changes. The tombstoned resource must
			// not be absence-deleted, and the server still carries it here.
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:">
  <d:sync-token>https://server/sync/src-1</d:sync-token>
</d:multistatus>`)
		case strings.Contains(string(body), "calendar-multiget"):
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:response>
    <d:href>/caldst/moved-meeting.ics</d:href>
    <d:propstat>
      <d:prop>
        <d:getetag>&quot;dst-etag&quot;</d:getetag>
        <cal:calendar-data>`+movedMeetingICS+`</cal:calendar-data>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
</d:multistatus>`)
		default:
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>/caldst/moved-meeting.ics</d:href>
    <d:propstat>
      <d:prop><d:getetag>&quot;dst-etag&quot;</d:getetag></d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
  <d:sync-token>https://server/sync/dst-1</d:sync-token>
</d:multistatus>`)
		}
	}))
	defer server.Close()

	engine, db, q := newTestEngine(t)
	ctx := context.Background()
	f := setupMoveFixtures(t, engine, q, server.URL)
	seedMovedRows(t, db, q, f)

	// Sync the source calendar first.
	if _, err := engine.SyncCalendar(ctx, f.srcID, ConflictPrompt); err != nil {
		t.Fatalf("SyncCalendar source: %v", err)
	}
	if deleteCalls != 1 {
		t.Fatalf("source DELETE calls = %d, want 1", deleteCalls)
	}
	if deleteIfMatch != `"src-etag"` {
		t.Errorf("source DELETE If-Match = %q, want conditional ETag", deleteIfMatch)
	}
	// The moved row stays live on the destination calendar.
	r, err := q.GetEventByUID(ctx, f.uid)
	if err != nil {
		t.Fatalf("moved event missing after source sync: %v", err)
	}
	if r.CalendarID != f.dstID {
		t.Errorf("event calendar = %d, want %d", r.CalendarID, f.dstID)
	}
	var srcLive int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events WHERE uid = ? AND calendar_id = ? AND deleted_at IS NULL`,
		f.uid, f.srcID).Scan(&srcLive); err != nil {
		t.Fatalf("count source rows: %v", err)
	}
	if srcLive != 0 {
		t.Errorf("live source rows = %d, want 0", srcLive)
	}
	if _, err := q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: f.srcID, Uid: f.uid,
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("source sync resource after DELETE = %v, want gone", err)
	}
	if n := countTombstones(t, db, f.srcID, f.uid); n != 0 {
		t.Errorf("source tombstones = %d, want 0 after convergence", n)
	}
	// Source success must not clear the destination push intent.
	dstRes, err := q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: f.dstID, Uid: f.uid,
	})
	if err != nil {
		t.Fatalf("destination resource missing: %v", err)
	}
	if dstRes.Dirty != 1 || dstRes.RemoteUrl != "" {
		t.Fatalf("destination resource = dirty %d url %q, want dirty create still pending",
			dstRes.Dirty, dstRes.RemoteUrl)
	}

	// Sync the destination calendar. It must PUT a new resource and record
	// the assigned href.
	if _, err := engine.SyncCalendar(ctx, f.dstID, ConflictPrompt); err != nil {
		t.Fatalf("SyncCalendar destination: %v", err)
	}
	if putCalls != 1 {
		t.Fatalf("destination PUT calls = %d, want 1", putCalls)
	}
	dstRes, err = q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: f.dstID, Uid: f.uid,
	})
	if err != nil {
		t.Fatalf("destination resource after push: %v", err)
	}
	if dstRes.Dirty != 0 {
		t.Errorf("destination dirty = %d, want 0 after PUT", dstRes.Dirty)
	}
	if !strings.Contains(dstRes.RemoteUrl, "/caldst/moved-meeting.ics") {
		t.Errorf("destination remote_url = %q, want the new href", dstRes.RemoteUrl)
	}
}

// TestCrossCalendarMove_TombstoneDELETE412 exercises the existing conflict
// protection on the source DELETE. A 412 drops the tombstone but keeps the
// source sync_resource so a later pull re-imports the remotely edited copy.
// The destination intent and the moved row stay untouched.
func TestCrossCalendarMove_TombstoneDELETE412(t *testing.T) {
	t.Parallel()

	engine, db, q := newTestEngine(t)
	ctx := context.Background()
	f := setupMoveFixtures(t, engine, q, "https://example.com")
	seedMovedRows(t, db, q, f)

	var gotIfMatch string
	client := newTestCalDAVClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodDelete {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		gotIfMatch = r.Header.Get("If-Match")
		return newResponse(http.StatusPreconditionFailed, nil), nil
	})

	res, err := engine.processTombstones(ctx, client, f.srcID, f.srcRef)
	if err != nil {
		t.Fatalf("processTombstones: %v", err)
	}
	if res.autoResolved != 1 {
		t.Fatalf("autoResolved = %d, want 1 (412 preserves remote edit)", res.autoResolved)
	}
	if gotIfMatch != `"src-etag"` {
		t.Errorf("DELETE If-Match = %q, want conditional ETag", gotIfMatch)
	}
	if n := countTombstones(t, db, f.srcID, f.uid); n != 0 {
		t.Errorf("tombstones after 412 = %d, want 0 (DELETE abandoned)", n)
	}
	src, err := q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: f.srcID, Uid: f.uid,
	})
	if err != nil {
		t.Fatalf("source resource must stay for re-import after 412: %v", err)
	}
	if src.RemoteUrl != f.srcHref || src.Etag != `"src-etag"` {
		t.Errorf("source resource = %q/%q, want href and etag retained", src.RemoteUrl, src.Etag)
	}
	dst := getDstResource(t, q, f)
	if dst.Dirty != 1 {
		t.Errorf("destination dirty = %d, want 1 (412 on source must not touch destination)", dst.Dirty)
	}
	r, err := q.GetEventByUID(ctx, f.uid)
	if err != nil || r.CalendarID != f.dstID {
		t.Fatalf("moved event after 412: %v cal %d", err, r.CalendarID)
	}
}

// TestCrossCalendarMove_TombstoneDELETE404Converges treats an already-absent
// source resource as success and clears both local source rows, without
// touching the destination calendar.
func TestCrossCalendarMove_TombstoneDELETE404Converges(t *testing.T) {
	t.Parallel()

	engine, db, q := newTestEngine(t)
	ctx := context.Background()
	f := setupMoveFixtures(t, engine, q, "https://example.com")
	seedMovedRows(t, db, q, f)

	client := newTestCalDAVClient(t, func(r *http.Request) (*http.Response, error) {
		return newResponse(http.StatusNotFound, nil), nil
	})

	res, err := engine.processTombstones(ctx, client, f.srcID, f.srcRef)
	if err != nil {
		t.Fatalf("processTombstones: %v", err)
	}
	if res.deleted != 1 {
		t.Fatalf("deleted = %d, want 1 (404 converges)", res.deleted)
	}
	if _, err := q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: f.srcID, Uid: f.uid,
	}); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("source resource = %v, want gone after 404", err)
	}
	if n := countTombstones(t, db, f.srcID, f.uid); n != 0 {
		t.Errorf("tombstones = %d, want 0", n)
	}
	dst := getDstResource(t, q, f)
	if dst.Dirty != 1 || dst.RemoteUrl != "" {
		t.Errorf("destination resource = dirty %d url %q, want intact create intent",
			dst.Dirty, dst.RemoteUrl)
	}
}

// TestCrossCalendarMove_FullSnapshotKeepsTombstonedResource covers the
// QueryAll fallback. The source server still lists the tombstoned resource.
// The pull must neither import it back nor absence-delete its sync_resource
// or the moved destination row.
func TestCrossCalendarMove_FullSnapshotKeepsTombstonedResource(t *testing.T) {
	t.Parallel()

	engine, db, q := newTestEngine(t)
	ctx := context.Background()
	f := setupMoveFixtures(t, engine, q, "https://example.com")
	seedMovedRows(t, db, q, f)

	client := newTestCalDAVClient(t, func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "sync-collection") {
			// Force the QueryAll fallback.
			return &http.Response{
				StatusCode: http.StatusUnprocessableEntity,
				Status:     "422 Unprocessable Entity",
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body:       io.NopCloser(strings.NewReader(`<?xml version="1.0"?><error xmlns="DAV:"/>`)),
				Request:    r,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusMultiStatus,
			Status:     "207 Multi-Status",
			Header:     http.Header{"Content-Type": []string{"application/xml"}},
			Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:response>
    <d:href>/calsrc/moved-meeting.ics</d:href>
    <d:propstat>
      <d:prop>
        <d:getetag>&quot;src-etag&quot;</d:getetag>
        <cal:calendar-data>BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//chroncal//tests//EN
BEGIN:VEVENT
UID:moved-meeting
DTSTAMP:20260403T120000Z
DTSTART:20260403T120000Z
DTEND:20260403T130000Z
SUMMARY:Old source copy
END:VEVENT
END:VCALENDAR
</cal:calendar-data>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
</d:multistatus>`)),
			Request: r,
		}, nil
	})

	result, err := engine.pull(ctx, client, f.srcID, f.srcRef)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if result.deleted != 0 {
		t.Fatalf("deleted = %d, want 0 (tombstoned resource protected)", result.deleted)
	}
	if n := countTombstones(t, db, f.srcID, f.uid); n != 1 {
		t.Errorf("tombstones = %d, want 1 (DELETE still pending)", n)
	}
	if _, err := q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: f.srcID, Uid: f.uid,
	}); err != nil {
		t.Fatalf("source resource must stay for the conditional DELETE: %v", err)
	}
	r, err := q.GetEventByUID(ctx, f.uid)
	if err != nil || r.CalendarID != f.dstID {
		t.Fatalf("moved destination row changed: %v cal %d", err, r.CalendarID)
	}
	if strings.Contains(r.Title, "Old source") {
		t.Errorf("destination row was overwritten by the tombstoned source body: %q", r.Title)
	}
}

// TestCrossCalendarMove_ExplicitDeletionScopedToSource proves the pull
// deletion chokepoint soft-deletes rows only on the calendar that lost the
// resource. A server-reported deletion on the source must never touch the
// moved destination row (same UID, another calendar).
func TestCrossCalendarMove_ExplicitDeletionScopedToSource(t *testing.T) {
	t.Parallel()

	engine, db, q := newTestEngine(t)
	ctx := context.Background()
	f := setupMoveFixtures(t, engine, q, "https://example.com")

	// State after the source DELETE already converged remotely, with no
	// tombstone row left: only the source sync_resource remains, and the
	// live row is on the destination.
	insertTestEvent(t, db, f.dstID, f.uid)
	if err := q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: f.srcID, Uid: f.uid, OwnerType: "event",
		RemoteUrl: f.srcHref, Etag: `"src-etag"`, Dirty: 0, SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("source sync resource: %v", err)
	}
	if err := q.UpsertSyncResource(ctx, storage.UpsertSyncResourceParams{
		CalendarID: f.dstID, Uid: f.uid, OwnerType: "event",
		RemoteUrl: f.dstRef + "moved-meeting.ics", Etag: `"dst-etag"`, Dirty: 0,
		SyncStrategy: "sync-token",
	}); err != nil {
		t.Fatalf("destination sync resource: %v", err)
	}

	cal, err := q.GetCalendar(ctx, f.srcID)
	if err != nil {
		t.Fatalf("GetCalendar: %v", err)
	}
	client := newTestCalDAVClient(t, func(r *http.Request) (*http.Response, error) {
		t.Fatalf("deletion-only pull must not issue HTTP requests: %s %s", r.Method, r.URL.Path)
		return nil, nil
	})
	syncResult := &caldav.SyncCollectionResult{
		SyncToken: "https://example.com/sync/after-delete",
		Changes: []caldav.SyncChange{
			{Path: f.srcHref, Deleted: true},
		},
	}
	result, err := engine.applySyncCollection(ctx, client, f.srcID, f.srcRef, cal, syncResult, false)
	if err != nil {
		t.Fatalf("applySyncCollection: %v", err)
	}
	if result.deleted != 1 {
		t.Fatalf("deleted = %d, want 1 (source resource removed)", result.deleted)
	}
	r, err := q.GetEventByUID(ctx, f.uid)
	if err != nil {
		t.Fatalf("destination row soft-deleted by a source-calendar deletion: %v", err)
	}
	if r.CalendarID != f.dstID {
		t.Errorf("event calendar = %d, want destination %d", r.CalendarID, f.dstID)
	}
	if _, err := q.GetSyncResource(ctx, storage.GetSyncResourceParams{
		CalendarID: f.dstID, Uid: f.uid,
	}); err != nil {
		t.Fatalf("destination sync resource must stay: %v", err)
	}
}

// TestPendingDeletions_TombstoneExcluded pins the move-specific gate. A
// tombstoned UID is neither explicit-deleted nor absence-deleted by pull.
// Its convergence belongs to processTombstones.
func TestPendingDeletions_TombstoneExcluded(t *testing.T) {
	t.Parallel()
	p := newPendingDeletions(discardLogger())
	tombstoned := map[string]bool{"moved": true}

	p.markExplicit(storage.SyncResource{
		CalendarID: 1, Uid: "moved", OwnerType: "event",
	}, tombstoned)
	if got := uidSet(p.items); len(got) != 0 {
		t.Fatalf("tombstoned explicit deletion recorded: %v", got)
	}

	locals := []storage.SyncResource{
		{CalendarID: 1, Uid: "moved", OwnerType: "event", RemoteUrl: "/calsrc/moved.ics"},
		{CalendarID: 1, Uid: "gone", OwnerType: "event", RemoteUrl: "/calsrc/gone.ics"},
	}
	p.inferFromAbsence(1, locals, map[string]bool{}, true, "complete", tombstoned)
	got := uidSet(p.items)
	if got["moved"] {
		t.Error("tombstoned UID must not be absence-deleted")
	}
	if !got["gone"] {
		t.Error("a genuinely absent non-tombstoned UID must still be deleted")
	}
	if item := p.items["gone"]; item.calendarID != 1 {
		t.Errorf("deletion calendar = %d, want the resource's own calendar", item.calendarID)
	}
}
