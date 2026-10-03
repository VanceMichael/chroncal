package alarm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/douglasdemoura/chroncal/internal/event"
	"github.com/douglasdemoura/chroncal/internal/model"
	"github.com/douglasdemoura/chroncal/internal/storage"
	"github.com/douglasdemoura/chroncal/internal/testutil"
	"github.com/douglasdemoura/chroncal/internal/todo"
)

func TestRetryDelaySchedule(t *testing.T) {
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{1, 30 * time.Second},
		{2, time.Minute},
		{3, 2 * time.Minute},
		{4, 5 * time.Minute},
		{5, maxRetryDelay},
		{99, maxRetryDelay},
		{0, 30 * time.Second},
		{-1, 30 * time.Second},
	}
	for _, tc := range cases {
		if got := RetryDelay(tc.attempts); got != tc.want {
			t.Errorf("RetryDelay(%d) = %v, want %v", tc.attempts, got, tc.want)
		}
	}
}

// createDispatchTestEvent creates an event whose -PT15M alarm is already
// due. It returns the due alarm from a Check at time now.
func createDispatchTestEvent(t *testing.T, ctx context.Context, svc *Service, evtSvc *event.Service, title string, now time.Time) DueAlarm {
	t.Helper()
	start := now.Add(-2 * time.Minute)
	evt, err := evtSvc.Create(ctx, event.CreateParams{
		CalendarID: 1,
		Title:      title,
		StartTime:  start,
		EndTime:    start.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	if err := evtSvc.ReplaceAlarms(ctx, evt.ID, []model.Alarm{{
		Action:       "DISPLAY",
		TriggerValue: "-PT15M",
		Description:  "Reminder",
		Related:      "START",
	}}); err != nil {
		t.Fatalf("replace alarms: %v", err)
	}
	due, _, err := svc.Check(ctx, now)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("Check returned %d due alarms, want 1", len(due))
	}
	return due[0]
}

// TestEventDispatch_FailureEntersRetryAndRecovers is the core lifecycle
// regression. Every backend fails: the claim stays retryable. A check
// before retry_at dispatches nothing. A check at retry_at reclaims the
// row. Completion ends the cycle. The row never goes back to a fresh
// fire.
func TestEventDispatch_FailureEntersRetryAndRecovers(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	ctx := context.Background()
	evtSvc := event.NewService(db, q)
	svc := NewService(db, q, evtSvc, nil)
	now := time.Now()

	da := createDispatchTestEvent(t, ctx, svc, evtSvc, "Retry Meeting", now)

	stateID, err := svc.MarkFired(ctx, da)
	if err != nil {
		t.Fatalf("mark fired: %v", err)
	}

	// A live dispatching claim is not re-dispatched by the same check pass.
	if due, _, _ := svc.Check(ctx, now); len(due) != 0 {
		t.Fatalf("Check during live dispatch returned %d due alarms, want 0", len(due))
	}

	// The notification backend fails completely.
	retryAt, owned, err := svc.ScheduleAlarmRetry(ctx, stateID, 1, errors.New("no graphical session"), now)
	if err != nil || !owned {
		t.Fatalf("schedule retry: owned=%v err=%v", owned, err)
	}
	if got := retryAt.Sub(now); got != 30*time.Second {
		t.Errorf("first retry delay = %v, want 30s", got)
	}

	st, err := q.GetAlarmState(ctx, storage.GetAlarmStateParams{
		AlarmID:   da.Alarm.ID,
		TriggerAt: da.TriggerAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	if st.DispatchStatus != DispatchRetry || st.ClaimToken != nil || st.RetryAt == nil {
		t.Fatalf("state after failure = status=%q token=%v retry_at=%v, want retry/token-null/retry_at set",
			st.DispatchStatus, st.ClaimToken, st.RetryAt)
	}
	if st.LastError == nil || *st.LastError != "no graphical session" {
		t.Errorf("last_error = %v, want the backend error", st.LastError)
	}

	// Before retry_at: nothing dispatches.
	if due, _, _ := svc.Check(ctx, now.Add(10*time.Second)); len(due) != 0 {
		t.Fatalf("Check before retry_at returned %d due alarms, want 0", len(due))
	}

	// At retry_at: the trigger is due again as a recovery, attempt 2.
	due, _, err := svc.Check(ctx, now.Add(31*time.Second))
	if err != nil {
		t.Fatalf("check at retry time: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("Check at retry_at returned %d due alarms, want 1", len(due))
	}
	got := due[0]
	if got.Claim != ClaimRecover || got.StateID != stateID || got.Attempts != 2 {
		t.Fatalf("recovery due = claim=%d state=%d attempts=%d, want recover/%d/2",
			got.Claim, got.StateID, got.Attempts, stateID)
	}

	claimed, err := svc.RecoverAlarmDispatch(ctx, stateID, now.Add(31*time.Second))
	if err != nil || !claimed {
		t.Fatalf("recover: claimed=%v err=%v", claimed, err)
	}

	// Second dispatch succeeds. Delivery is final.
	delivered, err := svc.CompleteAlarmDelivery(ctx, stateID, now.Add(31*time.Second))
	if err != nil || !delivered {
		t.Fatalf("complete: delivered=%v err=%v", delivered, err)
	}

	if due, _, _ := svc.Check(ctx, now.Add(32*time.Second)); len(due) != 0 {
		t.Fatalf("Check after delivery returned %d due alarms, want 0", len(due))
	}

	st, _ = q.GetAlarmStateByID(ctx, stateID)
	if st.DispatchStatus != DispatchDelivered || st.DeliveredAt == nil {
		t.Fatalf("final state = %q delivered_at=%v, want delivered with delivered_at",
			st.DispatchStatus, st.DeliveredAt)
	}
	if st.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", st.Attempts)
	}

	// Completion is one-shot. Another instance token cannot complete it again.
	other := NewService(db, q, evtSvc, nil)
	if delivered, err := other.CompleteAlarmDelivery(ctx, stateID, now.Add(33*time.Second)); err != nil || delivered {
		t.Fatalf("second complete = delivered=%v err=%v, want false/nil", delivered, err)
	}
}

// TestEventDispatch_OrphanedClaimRecoveredByNextInstance simulates a
// process that exits after the claim and before delivery. The row stays
// dispatching. Inside the lease, no checker takes it. Past the lease, a
// later service instance with a different token takes it over.
func TestEventDispatch_OrphanedClaimRecoveredByNextInstance(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	ctx := context.Background()
	evtSvc := event.NewService(db, q)
	svc := NewService(db, q, evtSvc, nil)
	now := time.Now()

	da := createDispatchTestEvent(t, ctx, svc, evtSvc, "Crashed Checker", now)
	stateID, err := svc.MarkFired(ctx, da)
	if err != nil {
		t.Fatalf("mark fired: %v", err)
	}

	// Fresh claim: another instance must wait for the lease.
	next := NewService(db, q, evtSvc, nil)
	if claimed, _ := next.RecoverAlarmDispatch(ctx, stateID, now.Add(time.Minute)); claimed {
		t.Fatal("a claim inside its lease was taken over")
	}
	if due, _, _ := next.Check(ctx, now.Add(time.Minute)); len(due) != 0 {
		t.Fatalf("Check inside lease returned %d due alarms, want 0", len(due))
	}

	// Process death: backdate claimed_at past the lease.
	old := now.Add(-DispatchLease - time.Minute).UTC().Format(time.RFC3339)
	if _, err := db.ExecContext(ctx,
		`UPDATE alarm_state SET claimed_at = ? WHERE id = ?`, old, stateID); err != nil {
		t.Fatalf("backdate claim: %v", err)
	}

	due, _, err := next.Check(ctx, now.Add(DispatchLease+2*time.Minute))
	if err != nil {
		t.Fatalf("check after lease: %v", err)
	}
	if len(due) != 1 || due[0].Claim != ClaimRecover || due[0].StateID != stateID {
		t.Fatalf("orphaned recovery due = %+v, want one ClaimRecover for state %d", due, stateID)
	}

	recoveryTime := now.Add(DispatchLease + 2*time.Minute)
	claimed, err := next.RecoverAlarmDispatch(ctx, stateID, recoveryTime)
	if err != nil || !claimed {
		t.Fatalf("takeover after lease: claimed=%v err=%v", claimed, err)
	}

	// The dead instance cannot settle a row it no longer owns.
	if owned, err := svc.CompleteAlarmDelivery(ctx, stateID, recoveryTime); err != nil || owned {
		t.Fatalf("stale owner complete = owned=%v err=%v, want false/nil", owned, err)
	}

	if delivered, err := next.CompleteAlarmDelivery(ctx, stateID, recoveryTime); err != nil || !delivered {
		t.Fatalf("new owner complete = delivered=%v err=%v", delivered, err)
	}

	st, _ := q.GetAlarmStateByID(ctx, stateID)
	if st.DispatchStatus != DispatchDelivered {
		t.Fatalf("status = %q, want delivered", st.DispatchStatus)
	}
}

// TestRecoverClaimedExactlyOnce pins the concurrency contract for
// retry-due rows. Two instances race on one takeover UPDATE. Exactly one
// wins. The loser dispatches nothing.
func TestRecoverClaimedExactlyOnce(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	ctx := context.Background()
	evtSvc := event.NewService(db, q)
	a := NewService(db, q, evtSvc, nil)
	b := NewService(db, q, evtSvc, nil)
	now := time.Now()

	da := createDispatchTestEvent(t, ctx, a, evtSvc, "Race Retry", now)
	stateID, err := a.MarkFired(ctx, da)
	if err != nil {
		t.Fatalf("mark fired: %v", err)
	}
	if _, _, err := a.ScheduleAlarmRetry(ctx, stateID, 1, errors.New("down"), now); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}

	recoveryTime := now.Add(31 * time.Second)
	aWon, err := a.RecoverAlarmDispatch(ctx, stateID, recoveryTime)
	if err != nil || !aWon {
		t.Fatalf("first recover: won=%v err=%v", aWon, err)
	}
	bWon, err := b.RecoverAlarmDispatch(ctx, stateID, recoveryTime)
	if err != nil {
		t.Fatalf("second recover: %v", err)
	}
	if bWon {
		t.Fatal("two checkers recovered the same trigger")
	}
}

// TestAckedAndSnoozedRowsAreNotRecovered guards the takeover predicate.
// Dismissal ends the lifecycle. A snoozed row belongs to the snooze path.
func TestAckedAndSnoozedRowsAreNotRecovered(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	ctx := context.Background()
	evtSvc := event.NewService(db, q)
	svc := NewService(db, q, evtSvc, nil)
	now := time.Now()

	// Acknowledged row.
	da := createDispatchTestEvent(t, ctx, svc, evtSvc, "Acked", now)
	stateID, _ := svc.MarkFired(ctx, da)
	nowStr := now.UTC().Format(time.RFC3339)
	if err := q.AcknowledgeAlarmState(ctx, storage.AcknowledgeAlarmStateParams{
		AckedAt: &nowStr, ID: stateID,
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if claimed, _ := svc.RecoverAlarmDispatch(ctx, stateID, now.Add(time.Hour)); claimed {
		t.Fatal("an acknowledged row was recovered")
	}

	// Snoozed retry row: the snooze path owns the next fire.
	da2 := createDispatchTestEvent(t, ctx, svc, evtSvc, "Snoozed Retry", now.Add(time.Minute))
	stateID2, _ := svc.MarkFired(ctx, da2)
	if _, _, err := svc.ScheduleAlarmRetry(ctx, stateID2, 1, errors.New("down"), now.Add(time.Minute)); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	snoozed := now.Add(2 * time.Minute).UTC().Format(time.RFC3339)
	if err := q.SnoozeAlarmState(ctx, storage.SnoozeAlarmStateParams{
		SnoozedTo: &snoozed, ID: stateID2,
	}); err != nil {
		t.Fatalf("snooze: %v", err)
	}
	if claimed, _ := svc.RecoverAlarmDispatch(ctx, stateID2, now.Add(time.Hour)); claimed {
		t.Fatal("a snoozed row was recovered by the retry path")
	}
}

// TestRefireFailureRetriesAndRecovers gives a snoozed event alarm the same
// lifecycle as a fresh trigger: failed refire -> retry -> recover ->
// delivered.
func TestRefireFailureRetriesAndRecovers(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	ctx := context.Background()
	evtSvc := event.NewService(db, q)
	svc := NewService(db, q, evtSvc, nil)
	now := time.Now()

	da := createDispatchTestEvent(t, ctx, svc, evtSvc, "Snoozed Then Failed", now)
	stateID, err := svc.MarkFired(ctx, da)
	if err != nil {
		t.Fatalf("mark fired: %v", err)
	}
	if _, err := svc.CompleteAlarmDelivery(ctx, stateID, now); err != nil {
		t.Fatalf("first delivery: %v", err)
	}

	// Snooze one minute out, then expire it.
	snoozed := now.Add(time.Minute).UTC().Format(time.RFC3339)
	if err := q.SnoozeAlarmState(ctx, storage.SnoozeAlarmStateParams{
		SnoozedTo: &snoozed, ID: stateID,
	}); err != nil {
		t.Fatalf("snooze: %v", err)
	}
	expired, err := svc.ListExpiredSnoozed(ctx, now.Add(2*time.Minute))
	if err != nil || len(expired) != 1 {
		t.Fatalf("expired snoozed = %d err=%v, want 1", len(expired), err)
	}
	if expired[0].Claim != ClaimRefire || expired[0].StateID != stateID {
		t.Fatalf("expired refire = claim=%d state=%d", expired[0].Claim, expired[0].StateID)
	}

	refireTime := now.Add(2 * time.Minute)
	claimed, err := svc.MarkRefired(ctx, stateID)
	if err != nil || !claimed {
		t.Fatalf("mark refired: claimed=%v err=%v", claimed, err)
	}
	st, _ := q.GetAlarmStateByID(ctx, stateID)
	if st.DispatchStatus != DispatchDispatching || st.SnoozedTo != nil || st.Attempts != 1 {
		t.Fatalf("state after refire = status=%q snoozed=%v attempts=%d",
			st.DispatchStatus, st.SnoozedTo, st.Attempts)
	}

	// Refire dispatch fails: retry, then recovery at retry_at.
	if _, owned, err := svc.ScheduleAlarmRetry(ctx, stateID, 1, errors.New("mail server down"), refireTime); err != nil || !owned {
		t.Fatalf("refire retry: owned=%v err=%v", owned, err)
	}
	due, _, err := svc.Check(ctx, refireTime.Add(31*time.Second))
	if err != nil {
		t.Fatalf("recover check: %v", err)
	}
	if len(due) != 1 || due[0].Claim != ClaimRecover {
		t.Fatalf("post-refire recovery = %+v, want one ClaimRecover", due)
	}
	if _, err := svc.RecoverAlarmDispatch(ctx, stateID, refireTime.Add(31*time.Second)); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if delivered, err := svc.CompleteAlarmDelivery(ctx, stateID, refireTime.Add(31*time.Second)); err != nil || !delivered {
		t.Fatalf("final delivery: delivered=%v err=%v", delivered, err)
	}
}

// TestTodoDispatch_FailureRetriesAndRecovers mirrors the event lifecycle
// for todo alarms.
func TestTodoDispatch_FailureRetriesAndRecovers(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	ctx := context.Background()
	todoSvc := todo.NewService(db, q)
	svc := NewService(db, q, event.NewService(db, q), todoSvc)
	now := time.Now()

	due := now.Add(-2 * time.Minute)
	td, err := todoSvc.Create(ctx, todo.CreateParams{
		CalendarID: 1,
		Summary:    "File taxes",
		DueDate:    due.UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("create todo: %v", err)
	}
	if err := todoSvc.ReplaceAlarms(ctx, td.ID, []model.Alarm{{
		Action:       "DISPLAY",
		TriggerValue: "-PT15M",
		Related:      "START",
	}}); err != nil {
		t.Fatalf("replace alarms: %v", err)
	}

	_, todoDue, err := svc.Check(ctx, now)
	if err != nil || len(todoDue) != 1 {
		t.Fatalf("check = %d todo due err=%v, want 1", len(todoDue), err)
	}
	tda := todoDue[0]
	stateID, err := svc.MarkTodoFired(ctx, tda)
	if err != nil {
		t.Fatalf("mark todo fired: %v", err)
	}
	if _, owned, err := svc.ScheduleTodoRetry(ctx, stateID, 1, errors.New("no display"), now); err != nil || !owned {
		t.Fatalf("todo retry: owned=%v err=%v", owned, err)
	}
	if _, todoDueEarly, _ := svc.Check(ctx, now.Add(10*time.Second)); len(todoDueEarly) != 0 {
		t.Fatalf("todo check before retry_at returned %d due alarms", len(todoDueEarly))
	}
	retryTime := now.Add(31 * time.Second)
	_, todoDue, err = svc.Check(ctx, retryTime)
	if err != nil || len(todoDue) != 1 {
		t.Fatalf("todo check at retry_at = %d due err=%v, want 1", len(todoDue), err)
	}
	if todoDue[0].Claim != ClaimRecover || todoDue[0].Attempts != 2 || todoDue[0].StateID != stateID {
		t.Fatalf("todo recovery = %+v", todoDue[0])
	}
	if claimed, err := svc.RecoverTodoAlarmDispatch(ctx, stateID, retryTime); err != nil || !claimed {
		t.Fatalf("todo recover: claimed=%v err=%v", claimed, err)
	}
	if delivered, err := svc.CompleteTodoDelivery(ctx, stateID, retryTime); err != nil || !delivered {
		t.Fatalf("todo complete: delivered=%v err=%v", delivered, err)
	}
	if _, todoDueAfter, _ := svc.Check(ctx, retryTime.Add(time.Second)); len(todoDueAfter) != 0 {
		t.Fatalf("todo check after delivery returned %d due alarms", len(todoDueAfter))
	}
}

// TestCheckMissed_DistinguishesDeliveryStates checks the four lifecycle
// outcomes on stale triggers: no row (unclaimed) is missed, delivered is
// not missed, retry and dispatching are missed with their stored state.
func TestCheckMissed_DistinguishesDeliveryStates(t *testing.T) {
	db, q := testutil.NewTestDB(t)
	ctx := context.Background()
	evtSvc := event.NewService(db, q)
	svc := NewService(db, q, evtSvc, nil)

	createStaleEvent := func(title string) (int64, int64, time.Time) {
		start := time.Now().Add(-26 * time.Hour)
		evt, err := evtSvc.Create(ctx, event.CreateParams{
			CalendarID: 1,
			Title:      title,
			StartTime:  start,
			EndTime:    start.Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("create event: %v", err)
		}
		if err := evtSvc.ReplaceAlarms(ctx, evt.ID, []model.Alarm{{
			Action: "DISPLAY", TriggerValue: "-PT15M",
		}}); err != nil {
			t.Fatalf("replace alarms: %v", err)
		}
		alarms, err := evtSvc.ListAlarms(ctx, evt.ID)
		if err != nil || len(alarms) != 1 {
			t.Fatalf("list alarms: %v", err)
		}
		return alarms[0].ID, evt.ID, start.Add(-15 * time.Minute)
	}

	createStaleEvent("Unclaimed")
	deliveredAlarm, deliveredEvent, deliveredTrigger := createStaleEvent("Delivered")
	retryAlarm, retryEvent, retryTrigger := createStaleEvent("Retrying")
	dspAlarm, dspEvent, dspTrigger := createStaleEvent("Stalled")

	fired := time.Now().Add(-26 * time.Hour).UTC().Format(time.RFC3339)
	insertState := func(alarmID, eventID int64, trigger time.Time, status string) {
		triggerStr := trigger.UTC().Format(time.RFC3339)
		query := `INSERT INTO alarm_state
		    (alarm_id, event_id, trigger_at, fired_at, dispatch_status, attempts`
		values := `) VALUES (?, ?, ?, ?, ?, 0`
		args := []any{alarmID, eventID, triggerStr, fired, status}
		switch status {
		case DispatchDelivered:
			query += ", delivered_at"
			values += ", ?"
			args = append(args, fired)
		case DispatchRetry:
			query += ", retry_at, last_error"
			values += ", ?, ?"
			args = append(args, fired, "down")
		case DispatchDispatching:
			query += ", claimed_at, claim_token"
			values += ", ?, ?"
			args = append(args, fired, "dead-token")
		}
		query += values + ")"
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("insert %s state: %v", status, err)
		}
	}
	insertState(deliveredAlarm, deliveredEvent, deliveredTrigger, DispatchDelivered)
	insertState(retryAlarm, retryEvent, retryTrigger, DispatchRetry)
	insertState(dspAlarm, dspEvent, dspTrigger, DispatchDispatching)

	missed, _, err := svc.CheckMissed(ctx, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatalf("check missed: %v", err)
	}
	got := map[string]string{}
	for _, m := range missed {
		got[m.EventTitle] = m.Delivery
	}
	if got["Unclaimed"] != DispatchUnclaimed {
		t.Errorf("unclaimed trigger reported as %q, want %q", got["Unclaimed"], DispatchUnclaimed)
	}
	if _, ok := got["Delivered"]; ok {
		t.Error("a delivered trigger was reported as missed")
	}
	if got["Retrying"] != DispatchRetry {
		t.Errorf("retry trigger reported as %q, want %q", got["Retrying"], DispatchRetry)
	}
	if got["Stalled"] != DispatchDispatching {
		t.Errorf("dispatching trigger reported as %q, want %q", got["Stalled"], DispatchDispatching)
	}
}
