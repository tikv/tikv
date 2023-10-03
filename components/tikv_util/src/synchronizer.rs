use std::fmt;

// A wrapper of a closure that will be invoked when it is dropped.
// This design has two benefits:
//   1. Using a closure (dynamically dispatched), so that it can avoid having
//      generic member fields like RaftRouter, thus avoid having Rust generic
//      type explosion problem.
//   2. Invoke on drop, so that it can be easily and safely used (together with
//      Arc) as a coordinator between all tasks that need to be synchronized.
//      Each of the task holds a reference to the same structure, and whoever
//      finishes drops its reference. Once the last reference is dropped,
//      indicating all the tasks have finished, the closure is invoked.
pub struct InvokeClosureOnDrop(pub Option<Box<dyn FnOnce() + Send + Sync>>);

impl fmt::Debug for InvokeClosureOnDrop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvokeClosureOnDrop")
    }
}

impl Drop for InvokeClosureOnDrop {
    fn drop(&mut self) {
        if let Some(on_drop) = self.0.take() {
            on_drop();
        }
    }
}
