using System.Collections.Generic;

namespace ParadoxDesktop
{
    /// <summary>
    /// A single reversible action recorded in an <see cref="UndoRedoManager"/>
    /// history. <see cref="Undo"/>/<see cref="Redo"/> must each leave the
    /// underlying data in a well-defined state (the "before" and "after"
    /// state respectively) so repeated Undo/Redo cycles are stable.
    /// </summary>
    internal interface IUndoableAction
    {
        void Undo();
        void Redo();
    }

    /// <summary>
    /// Simple linear undo/redo history: recording a new action clears any
    /// pending redo history (standard editor behavior - once you make a new
    /// change, the old "future" redo branch is discarded).
    /// </summary>
    internal sealed class UndoRedoManager
    {
        private readonly Stack<IUndoableAction> undoStack = new Stack<IUndoableAction>();
        private readonly Stack<IUndoableAction> redoStack = new Stack<IUndoableAction>();

        public bool CanUndo => undoStack.Count > 0;
        public bool CanRedo => redoStack.Count > 0;

        /// <summary>
        /// Records an action that has already been applied (its "do" step is
        /// the caller's responsibility); this just makes it undoable/redoable.
        /// </summary>
        public void Push(IUndoableAction action)
        {
            undoStack.Push(action);
            redoStack.Clear();
        }

        public void Undo()
        {
            if (!CanUndo) return;
            var action = undoStack.Pop();
            action.Undo();
            redoStack.Push(action);
        }

        public void Redo()
        {
            if (!CanRedo) return;
            var action = redoStack.Pop();
            action.Redo();
            undoStack.Push(action);
        }

        /// <summary>
        /// Discards all history. Call this whenever the underlying data's
        /// row/record identity changes in a way the recorded actions can no
        /// longer safely reference (insert, delete, rebuild, reload).
        /// </summary>
        public void Clear()
        {
            undoStack.Clear();
            redoStack.Clear();
        }
    }
}
