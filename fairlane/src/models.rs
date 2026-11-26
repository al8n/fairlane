//! The loom models: every interleaving of two or three threads over the crate's atomics and
//! cells, within a preemption bound. Run them with
//! `RUSTFLAGS="--cfg loom" cargo test -p fairlane --features loom --release --lib models`.

mod ring;

/// Checks `f` under loom. The preemption bound is `LOOM_MAX_PREEMPTIONS` where it is set, else 3.
///
/// Every model spawns each of its actors and leaves the main thread only to set up and join.
/// loom 0.7 keeps one last access per atomic for its DPOR (`rt/atomic.rs`): an actor that ran on
/// the main thread before the others started had its accesses overwritten by a later actor's own
/// load, and the race between them was never explored.
fn model(f: impl Fn() + Sync + Send + 'static) {
  model_with_branches(loom::model::Builder::new().max_branches, f);
}

/// [`model`], allowing `max_branches` branches in one execution in place of loom's default.
fn model_with_branches(max_branches: usize, f: impl Fn() + Sync + Send + 'static) {
  use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  };

  let mut builder = loom::model::Builder::new();
  builder.max_branches = max_branches;
  if builder.preemption_bound.is_none() {
    builder.preemption_bound = Some(3);
  }
  let iterations = Arc::new(AtomicUsize::new(0));
  let counted = iterations.clone();
  builder.check(move || {
    counted.fetch_add(1, Ordering::Relaxed);
    f();
  });
  eprintln!(
    "loom: {} iterations, preemption bound {:?}",
    iterations.load(Ordering::Relaxed),
    builder.preemption_bound
  );
}
