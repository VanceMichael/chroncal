package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/douglasdemoura/chroncal/internal/alarm"
	"github.com/douglasdemoura/chroncal/internal/app"
	"github.com/douglasdemoura/chroncal/internal/config"
	"github.com/douglasdemoura/chroncal/internal/event"
	"github.com/douglasdemoura/chroncal/internal/model"
	"github.com/douglasdemoura/chroncal/internal/todo"
	"github.com/spf13/cobra"
)

// stubFireAlarmWithError installs a controllable fireAlarmFn stub. It
// returns the invocation count. fire returns the error the next dispatch
// attempt must see.
func stubFireAlarmWithError(t *testing.T, fire func() error) *int {
	t.Helper()
	orig := fireAlarmFn
	t.Cleanup(func() { fireAlarmFn = orig })
	var calls int
	fireAlarmFn = func(alarm.DueAlarm, alarmExecutionPolicy) error {
		calls++
		return fire()
	}
	return &calls
}

// withJSONOutput flips outputFmt to json for the test and restores it.
func withJSONOutput(t *testing.T) {
	t.Helper()
	prev := outputFmt
	outputFmt = "json"
	t.Cleanup(func() { outputFmt = prev })
}

func createDueEventForDispatch(t *testing.T, ctx context.Context, a *app.App, title string) {
	t.Helper()
	cal, err := a.Calendars.Create(ctx, "Cal "+title, "", "")
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	start := time.Now().Add(-time.Minute)
	evt, err := a.Events.Create(ctx, event.CreateParams{
		CalendarID: cal.ID,
		Title:      title,
		StartTime:  start,
		EndTime:    start.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	if err := a.Events.ReplaceAlarms(ctx, evt.ID, []model.Alarm{{
		Action:       "DISPLAY",
		TriggerValue: "-PT15M",
		Description:  "Reminder",
		Related:      "START",
	}}); err != nil {
		t.Fatalf("replace alarms: %v", err)
	}
}

func parseCheckRecords(t *testing.T, out []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	if err := json.Unmarshal(out, &records); err != nil {
		t.Fatalf("parse check JSON %q: %v", string(out), err)
	}
	return records
}

// TestRunAlarmCheck_AllBackendsFailThenRetrySucceeds is the end-to-end
// regression: every backend fails, the check records a retry, a second
// immediate check dispatches nothing, and a check after retry_at delivers.
func TestRunAlarmCheck_AllBackendsFailThenRetrySucceeds(t *testing.T) {
	ctx := context.Background()
	a := newAlarmTestApp(t)
	createDueEventForDispatch(t, ctx, a, "No Session")
	withJSONOutput(t)

	shouldFail := true
	calls := stubFireAlarmWithError(t, func() error {
		if shouldFail {
			return errors.New("no graphical session")
		}
		return nil
	})

	var out bytes.Buffer
	if err := runAlarmCheck(ctx, a, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("first check: %v", err)
	}
	records := parseCheckRecords(t, out.Bytes())
	if len(records) != 1 {
		t.Fatalf("first check emitted %d records, want 1: %s", len(records), out.String())
	}
	rec := records[0]
	if rec["delivery"] != alarm.DispatchRetry {
		t.Errorf("delivery = %v, want %q", rec["delivery"], alarm.DispatchRetry)
	}
	if status, _ := rec["status"].(string); !strings.HasPrefix(status, "error:") {
		t.Errorf("status = %v, want an error: prefix", rec["status"])
	}
	if rec["retry_at"] == nil || rec["retry_at"] == "" {
		t.Error("retry_at missing from retry record")
	}
	if attempts, _ := rec["attempts"].(float64); attempts != 1 {
		t.Errorf("attempts = %v, want 1", rec["attempts"])
	}

	// The row is in the retry state.
	var status, token any
	err := a.DB.QueryRowContext(ctx,
		`SELECT dispatch_status, claim_token FROM alarm_state WHERE id = ?`, rec["state_id"]).Scan(&status, &token)
	if err != nil {
		t.Fatalf("read state row: %v", err)
	}
	if status != alarm.DispatchRetry || token != nil {
		t.Fatalf("row = status=%v token=%v, want retry with no owner", status, token)
	}

	// A check before retry_at dispatches nothing.
	out.Reset()
	if err := runAlarmCheck(ctx, a, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("second check: %v", err)
	}
	if strings.TrimSpace(out.String()) != "[]" {
		t.Fatalf("check before retry_at emitted %q, want []", out.String())
	}
	if *calls != 1 {
		t.Fatalf("dispatch ran %d times before retry_at, want 1", *calls)
	}

	// Time reaches retry_at (simulated by backdating the schedule).
	past := time.Now().Add(-time.Second).UTC().Format(time.RFC3339)
	if _, err := a.DB.ExecContext(ctx, `UPDATE alarm_state SET retry_at = ? WHERE id = ?`,
		past, int64(rec["state_id"].(float64))); err != nil {
		t.Fatalf("backdate retry_at: %v", err)
	}

	shouldFail = false
	out.Reset()
	if err := runAlarmCheck(ctx, a, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("retry check: %v", err)
	}
	records = parseCheckRecords(t, out.Bytes())
	if len(records) != 1 {
		t.Fatalf("retry check emitted %d records, want 1: %s", len(records), out.String())
	}
	if records[0]["delivery"] != alarm.DispatchDelivered || records[0]["status"] != "fired" {
		t.Fatalf("retry record = %v, want fired/delivered", records[0])
	}
	if attempts, _ := records[0]["attempts"].(float64); attempts != 2 {
		t.Errorf("attempts = %v, want 2", records[0]["attempts"])
	}
	if *calls != 2 {
		t.Fatalf("dispatch ran %d times, want exactly 2", *calls)
	}

	// Delivered rows do not fire again.
	out.Reset()
	if err := runAlarmCheck(ctx, a, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("final check: %v", err)
	}
	if strings.TrimSpace(out.String()) != "[]" || *calls != 2 {
		t.Fatalf("delivered alarm re-fired: calls=%d out=%q", *calls, out.String())
	}
}

// TestRunAlarmCheck_OrphanedDispatchRecoveredByRestart reproduces the
// crash window: process A claims and dies before delivery. After the
// lease, restarted process B takes the row over and delivers once.
func TestRunAlarmCheck_OrphanedDispatchRecoveredByRestart(t *testing.T) {
	ctx := context.Background()
	a, dbPath := newAlarmTestAppWithPath(t)
	createDueEventForDispatch(t, ctx, a, "Crashed Daemon")

	// Restarted process B.
	b, err := app.New(dbPath)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	defer b.Close()

	due, _, err := b.Alarms.Check(ctx, time.Now())
	if err != nil || len(due) != 1 {
		t.Fatalf("precondition check: err=%v due=%d", err, len(due))
	}

	// Process A claims, then exits without delivering.
	stateID, err := a.Alarms.MarkFired(ctx, due[0])
	if err != nil {
		t.Fatalf("orphan claim: %v", err)
	}
	old := time.Now().Add(-alarm.DispatchLease - time.Minute).UTC().Format(time.RFC3339)
	if _, err := b.DB.ExecContext(ctx, `UPDATE alarm_state SET claimed_at = ? WHERE id = ?`, old, stateID); err != nil {
		t.Fatalf("backdate claim: %v", err)
	}

	calls := stubFireAlarmWithError(t, func() error { return nil })
	withJSONOutput(t)

	var out bytes.Buffer
	if err := runAlarmCheck(ctx, b, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("recovery check: %v", err)
	}
	records := parseCheckRecords(t, out.Bytes())
	if len(records) != 1 || records[0]["delivery"] != alarm.DispatchDelivered {
		t.Fatalf("recovery records = %s, want one delivered", out.String())
	}
	if *calls != 1 {
		t.Fatalf("dispatch ran %d times, want 1", *calls)
	}

	var status string
	if err := b.DB.QueryRowContext(ctx, `SELECT dispatch_status FROM alarm_state WHERE id = ?`, stateID).Scan(&status); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != alarm.DispatchDelivered {
		t.Fatalf("status = %q, want delivered", status)
	}

	// No redelivery on the next pass.
	out.Reset()
	if err := runAlarmCheck(ctx, b, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("post-recovery check: %v", err)
	}
	if strings.TrimSpace(out.String()) != "[]" || *calls != 1 {
		t.Fatalf("recovered alarm re-fired: calls=%d out=%q", *calls, out.String())
	}
}

// TestRunAlarmCheck_RecoveryDeduplicatesConcurrentCheckers proves two
// checkers cannot dispatch the same recovered trigger. When checker B
// reaches its claim, checker A has already taken the retry row over.
func TestRunAlarmCheck_RecoveryDeduplicatesConcurrentCheckers(t *testing.T) {
	ctx := context.Background()
	a, dbPath := newAlarmTestAppWithPath(t)
	createDueEventForDispatch(t, ctx, a, "Concurrent Recovery")

	// First process fails every backend: the row enters retry.
	calls := stubFireAlarmWithError(t, func() error { return errors.New("down") })
	var first bytes.Buffer
	if err := runAlarmCheck(ctx, a, &first, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("failing check: %v", err)
	}
	if *calls != 1 {
		t.Fatalf("failing dispatch count = %d, want 1", *calls)
	}

	// Retry becomes due. Restarted process B observes the due row.
	b, err := app.New(dbPath)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	defer b.Close()
	if _, err := a.DB.ExecContext(ctx, `UPDATE alarm_state SET retry_at = ?`,
		time.Now().Add(-time.Second).UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("backdate retry: %v", err)
	}
	due, _, err := b.Alarms.Check(ctx, time.Now())
	if err != nil || len(due) != 1 {
		t.Fatalf("recovery precondition: err=%v due=%d", err, len(due))
	}
	recoveryDue := due[0]

	// From now on backends work.
	stubFireAlarmWithError(t, func() error { return nil })

	// A wins the recovery claim inside B's Check-to-claim window.
	aRecovered := false
	prevHook := afterCheckForTest
	afterCheckForTest = func() {
		if aRecovered {
			return
		}
		aRecovered = true
		if res := markAndFireEventAlarm(ctx, a, recoveryDue, alarmExecutionPolicy{}); !res.Fired {
			t.Errorf("rival recovery failed: %+v", res)
		}
	}
	t.Cleanup(func() { afterCheckForTest = prevHook })

	withJSONOutput(t)
	var out bytes.Buffer
	if err := runAlarmCheck(ctx, b, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("loser check: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "[]" {
		t.Fatalf("losing checker emitted %q, want []", got)
	}
	if *calls != 1 {
		t.Fatalf("recovered trigger dispatched %d times, want 1", *calls)
	}
}

// TestRunAlarmCheck_TodoFailureRetriesDelivers gives the todo side the
// same failure-and-retry semantics.
func TestRunAlarmCheck_TodoFailureRetriesDelivers(t *testing.T) {
	ctx := context.Background()
	a := newAlarmTestApp(t)
	withJSONOutput(t)

	cal, err := a.Calendars.Create(ctx, "Work", "", "")
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	dueTime := time.Now().Add(-time.Minute)
	td, err := a.Todos.Create(ctx, todo.CreateParams{
		CalendarID: cal.ID,
		Summary:    "Send report",
		DueDate:    dueTime.UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("create todo: %v", err)
	}
	if err := a.Todos.ReplaceAlarms(ctx, td.ID, []model.Alarm{{
		Action: "DISPLAY", TriggerValue: "-PT15M", Related: "START",
	}}); err != nil {
		t.Fatalf("replace alarms: %v", err)
	}

	shouldFail := true
	calls := stubFireAlarmWithError(t, func() error {
		if shouldFail {
			return errors.New("mail server unavailable")
		}
		return nil
	})

	var out bytes.Buffer
	if err := runAlarmCheck(ctx, a, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("failing check: %v", err)
	}
	records := parseCheckRecords(t, out.Bytes())
	if len(records) != 1 {
		t.Fatalf("failing check records = %s", out.String())
	}
	if records[0]["delivery"] != alarm.DispatchRetry || records[0]["todo_id"] == nil {
		t.Fatalf("todo retry record = %v", records[0])
	}
	stateID := int64(records[0]["state_id"].(float64))

	if _, err := a.DB.ExecContext(ctx, `UPDATE todo_alarm_state SET retry_at = ? WHERE id = ?`,
		time.Now().Add(-time.Second).UTC().Format(time.RFC3339), stateID); err != nil {
		t.Fatalf("backdate todo retry: %v", err)
	}
	shouldFail = false
	out.Reset()
	if err := runAlarmCheck(ctx, a, &out, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("retry check: %v", err)
	}
	records = parseCheckRecords(t, out.Bytes())
	if len(records) != 1 || records[0]["delivery"] != alarm.DispatchDelivered {
		t.Fatalf("todo redelivery record = %s", out.String())
	}
	if *calls != 2 {
		t.Fatalf("todo dispatch ran %d times, want 2", *calls)
	}
}

// TestAlarmList_ShowsDeliveryStates checks that "alarm list" distinguishes
// delivered and retry rows in JSON and text output.
func TestAlarmList_ShowsDeliveryStates(t *testing.T) {
	ctx := context.Background()
	a := newAlarmTestApp(t)
	withJSONOutput(t)

	// Event A: failed delivery, waiting for retry.
	createDueEventForDispatch(t, ctx, a, "Retrying Event")
	failStub := stubFireAlarmWithError(t, func() error { return errors.New("no display") })
	if err := runAlarmCheck(ctx, a, &bytes.Buffer{}, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("failing check: %v", err)
	}
	if *failStub != 1 {
		t.Fatalf("failing dispatch count = %d, want 1", *failStub)
	}

	// Event B: delivered.
	createDueEventForDispatch(t, ctx, a, "Delivered Event")
	okStub := stubFireAlarmWithError(t, func() error { return nil })
	if err := runAlarmCheck(ctx, a, &bytes.Buffer{}, time.Now(), alarmExecutionPolicy{}); err != nil {
		t.Fatalf("successful check: %v", err)
	}
	if *okStub != 1 {
		t.Fatalf("successful dispatch count = %d, want 1", *okStub)
	}

	// --- JSON output ---
	root := &cobra.Command{
		Use: "chroncal-test",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var loadErr error
			cfg, loadErr = config.Load()
			return loadErr
		},
	}
	root.AddCommand(alarmListCmd())
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("alarm list: %v", err)
	}
	var items []map[string]any
	if err := json.Unmarshal(out.Bytes(), &items); err != nil {
		t.Fatalf("parse list JSON: %v\n%s", err, out.String())
	}
	states := map[string]map[string]any{}
	for _, it := range items {
		title, _ := it["title"].(string)
		states[title] = it
		if it["delivery"] == nil {
			t.Errorf("item %v missing delivery field", it["title"])
		}
		if it["attempts"] == nil || it["retry_at"] == nil || it["last_error"] == nil || it["delivered_at"] == nil {
			t.Errorf("item %v missing lifecycle fields: %v", it["title"], it)
		}
	}
	retryItem := states["Retrying Event"]
	if retryItem == nil || retryItem["delivery"] != alarm.DispatchRetry {
		t.Fatalf("retry item = %v", retryItem)
	}
	if retryItem["retry_at"] == "" {
		t.Error("retry item retry_at is empty")
	}
	deliveredItem := states["Delivered Event"]
	if deliveredItem == nil || deliveredItem["delivery"] != alarm.DispatchDelivered {
		t.Fatalf("delivered item = %v", deliveredItem)
	}
	if deliveredItem["delivered_at"] == "" {
		t.Error("delivered item delivered_at is empty")
	}

	// --- Text output carries the retry note ---
	outputFmt = "text"
	t.Cleanup(func() { outputFmt = "json" })
	out.Reset()
	root.SetArgs([]string{"list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("alarm list text: %v", err)
	}
	text := out.String()
	if !strings.Contains(text, "(retry at ") {
		t.Errorf("text list missing retry note:\n%s", text)
	}
	if !strings.Contains(text, "Retrying Event") || !strings.Contains(text, "Delivered Event") {
		t.Errorf("text list missing titles:\n%s", text)
	}
}

// TestAlarmList_DispatchingStateNote covers the in-flight marker. A row
// stuck in the dispatching state renders "(sending)".
func TestAlarmList_DispatchingStateNote(t *testing.T) {
	ctx := context.Background()
	a := newAlarmTestApp(t)
	createDueEventForDispatch(t, ctx, a, "In Flight")

	// Claim without completing, like a process that dies mid-dispatch.
	due, _, err := a.Alarms.Check(ctx, time.Now())
	if err != nil || len(due) != 1 {
		t.Fatalf("check: err=%v due=%d", err, len(due))
	}
	if _, err := a.Alarms.MarkFired(ctx, due[0]); err != nil {
		t.Fatalf("claim: %v", err)
	}

	pending, err := a.Alarms.ListPending(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending: err=%v rows=%d", err, len(pending))
	}
	note := pendingDispatchNote(false, pending[0].DispatchStatus, pending[0].RetryAt)
	if note != " (sending)" {
		t.Fatalf("dispatching note = %q, want %q", note, " (sending)")
	}
}

// TestMissedCommand_IncludesDeliveryField is the command-level check that
// missed JSON carries the delivery discriminator.
func TestMissedCommand_IncludesDeliveryField(t *testing.T) {
	ctx := context.Background()
	a := newAlarmTestApp(t)
	withJSONOutput(t)

	cal, err := a.Calendars.Create(ctx, "Work", "", "")
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	start := time.Now().Add(-72 * time.Hour)
	evt, err := a.Events.Create(ctx, event.CreateParams{
		CalendarID: cal.ID,
		Title:      "Ancient Meeting",
		StartTime:  start,
		EndTime:    start.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	if err := a.Events.ReplaceAlarms(ctx, evt.ID, []model.Alarm{{
		Action: "DISPLAY", TriggerValue: "-PT15M",
	}}); err != nil {
		t.Fatalf("replace alarms: %v", err)
	}
	alarms, err := a.Events.ListAlarms(ctx, evt.ID)
	if err != nil {
		t.Fatalf("list alarms: %v", err)
	}
	trigger := start.Add(-15 * time.Minute).UTC().Format(time.RFC3339)
	fired := start.UTC().Format(time.RFC3339)
	if _, err := a.DB.ExecContext(ctx,
		`INSERT INTO alarm_state (alarm_id, event_id, trigger_at, fired_at, dispatch_status, attempts, retry_at)
		 VALUES (?, ?, ?, ?, ?, 1, ?)`,
		alarms[0].ID, evt.ID, trigger, fired, alarm.DispatchRetry, fired); err != nil {
		t.Fatalf("insert retry state: %v", err)
	}

	root := &cobra.Command{
		Use: "chroncal-test",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var loadErr error
			cfg, loadErr = config.Load()
			return loadErr
		},
	}
	root.AddCommand(alarmMissedCmd())
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"missed", "--days", "7"})
	if err := root.Execute(); err != nil {
		t.Fatalf("alarm missed: %v", err)
	}
	var items []map[string]any
	if err := json.Unmarshal(out.Bytes(), &items); err != nil {
		t.Fatalf("parse missed JSON: %v\n%s", err, out.String())
	}
	found := false
	for _, it := range items {
		if it["title"] == "Ancient Meeting" {
			found = true
			if it["delivery"] != alarm.DispatchRetry {
				t.Fatalf("delivery = %v, want %q", it["delivery"], alarm.DispatchRetry)
			}
		}
	}
	if !found {
		t.Fatalf("stale retry row not listed as missed: %s", out.String())
	}
}

// TestRecoverDispatchLeaseConstantDocumentsBoundary pins the documented
// five-minute takeover boundary.
func TestRecoverDispatchLeaseConstantDocumentsBoundary(t *testing.T) {
	if alarm.DispatchLease != 5*time.Minute {
		t.Fatalf("DispatchLease = %v, want 5m", alarm.DispatchLease)
	}
}
