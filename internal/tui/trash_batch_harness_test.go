package tui

import (
	"context"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/require"

	"github.com/douglasdemoura/chroncal/internal/event"
	"github.com/douglasdemoura/chroncal/internal/trash"
	"github.com/douglasdemoura/chroncal/internal/todo"
)

// press sends one message through the root model and asserts the result
// stays a Model.
func press(t *testing.T, m Model, msg tea.Msg) (Model, tea.Cmd) {
	t.Helper()
	updated, cmd := m.Update(msg)
	mm, ok := updated.(Model)
	require.True(t, ok, "Update returned %T, want Model", updated)
	return mm, cmd
}

// openTrashInModel opens the trash overlay and drains the initial load so
// the rows are mounted.
func openTrashInModel(t *testing.T, m Model) Model {
	t.Helper()
	m, next := step(t, m, TrashViewRequestedMsg{})
	require.IsType(t, trashLoadedMsg{}, next, "opening trash must query the trash list, got %T", next)
	m, _ = step(t, m, next)
	require.True(t, m.trashOpen, "the trash overlay did not open")
	return m
}

// drainActionCmd executes the command a restore/purge request returned
// (it resolves to trashActionDoneMsg) and then drains the follow-up
// reload and toast batch.
func drainActionCmd(t *testing.T, m Model, cmd tea.Cmd) Model {
	t.Helper()
	require.NotNil(t, cmd)
	done := cmd()
	require.IsType(t, trashActionDoneMsg{}, done, "got %T", done)
	if batch, ok := done.(tea.BatchMsg); ok {
		m, _ = drainBatch(t, m, batch)
		return m
	}
	m, next := step(t, m, done)
	if next != nil {
		if batch, ok := next.(tea.BatchMsg); ok {
			m, _ = drainBatch(t, m, batch)
		}
	}
	return m
}

// TestHarness_TrashBulkRestore_AtomicBatchAndReload marks an event and a
// todo with space and restores both with r. Both rows come back, the
// trash dialog re-reads the real (empty) list, and marks clear.
func TestHarness_TrashBulkRestore_AtomicBatchAndReload(t *testing.T) {
	m, a := newDBBackedModel(t)
	ctx := context.Background()

	ev, err := a.Events.Create(ctx, event.CreateParams{
		CalendarID: 1,
		Title:      "Trash Event",
		StartTime:  time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC),
		EndTime:    time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	td, err := a.Todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Trash Todo"})
	require.NoError(t, err)
	require.NoError(t, a.Events.Delete(ctx, ev.ID))
	require.NoError(t, a.Todos.Delete(ctx, td.ID))

	m = openTrashInModel(t, m)
	require.Equal(t, 2, m.trash.Len(), "trash must list the event and the todo")

	// Mark the cursor row, move down, mark the second row.
	m, _ = press(t, m, keyPressMsg("space"))
	m, _ = press(t, m, tea.KeyPressMsg{Code: tea.KeyDown})
	m, _ = press(t, m, keyPressMsg("space"))
	require.Len(t, m.trash.marked, 2, "space must mark both rows")

	_, requestCmd := press(t, m, keyPressMsg("r"))
	req, ok := requestCmd().(TrashRestoreRequestedMsg)
	require.True(t, ok, "r must request a restore, got %T", requestCmd)
	require.Len(t, req.Entries, 2, "restore request must carry both marks")

	_, actionCmd := press(t, m, req)
	m = drainActionCmd(t, m, actionCmd)

	require.Equal(t, 0, m.trash.Len(), "the dialog must re-read the now-empty trash")
	require.Empty(t, m.trash.marked, "marks clear after a successful batch")
	gotEv, err := a.Events.Get(ctx, ev.ID)
	require.NoError(t, err)
	require.Nil(t, gotEv.DeletedAt)
	gotTd, err := a.Todos.Get(ctx, td.ID)
	require.NoError(t, err)
	require.Nil(t, gotTd.DeletedAt)
}

// TestHarness_TrashBulkRestore_FailureReloadsAndKeepsValidMarks sends a
// batch that contains the marked real todo plus one stale entry another
// operation already removed. The atomic batch fails and rolls back. The
// dialog re-reads real state, the stale mark drops, and the retry built
// from the remaining marks targets only the real todo.
func TestHarness_TrashBulkRestore_FailureReloadsAndKeepsValidMarks(t *testing.T) {
	m, a := newDBBackedModel(t)
	ctx := context.Background()

	td, err := a.Todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Still Deleted"})
	require.NoError(t, err)
	require.NoError(t, a.Todos.Delete(ctx, td.ID))

	m = openTrashInModel(t, m)
	require.Equal(t, 1, m.trash.Len())
	m, _ = press(t, m, keyPressMsg("space"))
	require.Len(t, m.trash.marked, 1)

	real := m.trash.entries[0]
	stale := trash.Entry{Kind: trash.KindTodo, ID: 999_999, CalendarID: 1, Title: "Already gone"}
	_, cmd := press(t, m, TrashRestoreRequestedMsg{Entries: []trash.Entry{real, stale}})
	require.NotNil(t, cmd, "the restore request must run the batch")
	m = drainActionCmd(t, m, cmd)

	// The batch rolled back: the todo is still soft-deleted.
	_, err = a.Todos.Get(ctx, td.ID)
	require.Error(t, err, "the todo must stay deleted after a rolled-back batch")

	// Real state was re-read. The stale selection is gone; the real row
	// and its mark survive so the retry acts on the real row only.
	require.Equal(t, 1, m.trash.Len(), "the dialog must re-read the real trash list")
	require.Len(t, m.trash.marked, 1, "the valid mark stays for a retry")
	for key := range m.trash.marked {
		require.Equal(t, entryKey(m.trash.entries[0]), key)
	}

	// The retry derived from the current marks never carries the stale
	// entry. It succeeds against the real row.
	_, retryRequestCmd := press(t, m, keyPressMsg("r"))
	retry, ok := retryRequestCmd().(TrashRestoreRequestedMsg)
	require.True(t, ok, "r must request the retry restore, got %T", retryRequestCmd)
	require.Len(t, retry.Entries, 1)
	require.Equal(t, td.ID, retry.Entries[0].ID)
	_, retryActionCmd := press(t, m, retry)
	m = drainActionCmd(t, m, retryActionCmd)

	require.Equal(t, 0, m.trash.Len())
	got, err := a.Todos.Get(ctx, td.ID)
	require.NoError(t, err)
	require.Nil(t, got.DeletedAt)
}

// TestHarness_TrashBulkPurge_ConfirmRunsOneAtomicBatch marks both rows
// and confirms the purge dialog. Both hard-delete together and the list
// reloads empty.
func TestHarness_TrashBulkPurge_ConfirmRunsOneAtomicBatch(t *testing.T) {
	m, a := newDBBackedModel(t)
	ctx := context.Background()

	ev, err := a.Events.Create(ctx, event.CreateParams{
		CalendarID: 1,
		Title:      "Purge Event",
		StartTime:  time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC),
		EndTime:    time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	td, err := a.Todos.Create(ctx, todo.CreateParams{CalendarID: 1, Summary: "Purge Todo"})
	require.NoError(t, err)
	require.NoError(t, a.Events.Delete(ctx, ev.ID))
	require.NoError(t, a.Todos.Delete(ctx, td.ID))

	m = openTrashInModel(t, m)
	require.Equal(t, 2, m.trash.Len())
	m, _ = press(t, m, keyPressMsg("space"))
	m, _ = press(t, m, tea.KeyPressMsg{Code: tea.KeyDown})
	m, _ = press(t, m, keyPressMsg("space"))
	require.Len(t, m.trash.marked, 2)

	// x opens the destructive confirm; confirm dispatches the batch.
	_, requestCmd := press(t, m, keyPressMsg("x"))
	purgeReq, ok := requestCmd().(TrashPurgeRequestedMsg)
	require.True(t, ok, "x must request the purge confirm, got %T", requestCmd)
	require.Len(t, purgeReq.Entries, 2)
	m, _ = press(t, m, purgeReq)
	// The request installs the pending purge and opens the confirm. It
	// must not perform the purge yet.
	_, err = a.Events.GetIncludingDeleted(ctx, ev.ID)
	require.NoError(t, err, "the event must still exist before confirmation")
	require.True(t, m.confirmOpen)

	_, confirmedCmd := press(t, m, ConfirmDialogResultMsg{Confirmed: true})
	require.NotNil(t, confirmedCmd, "the confirmed purge must run the batch")
	m = drainActionCmd(t, m, confirmedCmd)

	require.Equal(t, 0, m.trash.Len(), "the dialog must re-read the now-empty trash")
	require.Empty(t, m.trash.marked)
	_, err = a.Events.GetIncludingDeleted(ctx, ev.ID)
	require.Error(t, err, "the event row must be hard-deleted")
	_, err = a.Todos.GetIncludingDeleted(ctx, td.ID)
	require.Error(t, err, "the todo row must be hard-deleted")
}
