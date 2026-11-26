//! The atomics and cells the crate's concurrent code uses: std's, or loom's under `cfg(loom)`.
//! Everything that loom must see goes through here.

#[cfg(not(loom))]
pub(crate) use std::{
  sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  },
  thread,
};

#[cfg(loom)]
pub(crate) use loom::{
  sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  },
  thread,
};

/// Locks `mutex`, reading through a poison: every critical section of the crate leaves its state
/// whole before it can panic, so the state behind a poisoned lock is sound.
#[inline]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
  mutex
    .lock()
    .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The store-buffering pair of a wait, and nothing else.
///
/// A receiver going to sleep counts itself on a lane (a read-modify-write of the lane's count) and
/// then reads the lane's tail. A commit moves the tail (a read-modify-write) and then reads the
/// count. No wakeup is lost if one side's read observes the other side's write. A chain (a receive
/// that took items and left some) reads the count at the same point as a commit, after it took an
/// item that a commit published, so it observes every receiver that the commit observed.
///
/// - **In production** this rests on `SeqCst` accesses on both sides: every modification of a
///   count and of a tail is a `SeqCst` read-modify-write, and both reads are `SeqCst` loads.
///   Rust's atomics follow C++20's model, whose [atomics.order] puts every `SeqCst` operation in
///   one total order `S` consistent with each thread's program order, and lets a `SeqCst` load of
///   an object whose modifications are all `SeqCst` read only the last of them before it in `S`.
///   Of the two reads, the later in `S` follows the other side's write in `S`, and so reads it.
/// - **Under loom** this function is `fence(SeqCst)` at exactly the pair's two points: after the
///   receiver counts itself, and before a commit or a chain reads the count. loom 0.7 models
///   `SeqCst` accesses as `AcqRel` (its README, "Unsupported features"), under which both reads
///   may miss, a false alarm; it does model `SeqCst` fences. C++20 [atomics.fences] gives the same
///   guarantee from the fences: for a write A sequenced before a `SeqCst` fence X, and a `SeqCst`
///   fence Y sequenced before a read B of the same object, where X precedes Y in `S`, B observes A
///   or a later write. Whichever of the two fences comes first in `S`, the other side's read
///   observes this side's write.
///
/// Outside loom it compiles to nothing. No other ordering argument in the crate rests on it.
#[inline(always)]
pub(crate) fn pair_fence() {
  #[cfg(loom)]
  loom::sync::atomic::fence(Ordering::SeqCst);
}

pub(crate) use cell::UnsafeCell;

#[cfg(not(loom))]
mod cell {
  /// `core::cell::UnsafeCell` with loom's access API, so one body of code runs under both.
  pub(crate) struct UnsafeCell<T>(core::cell::UnsafeCell<T>);

  impl<T> UnsafeCell<T> {
    #[inline]
    pub(crate) const fn new(value: T) -> Self {
      Self(core::cell::UnsafeCell::new(value))
    }

    /// Calls `f` with a pointer for reading.
    #[inline]
    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
      f(self.0.get())
    }

    /// Calls `f` with a pointer for writing.
    #[inline]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
      f(self.0.get())
    }
  }
}

#[cfg(loom)]
mod cell {
  pub(crate) use loom::cell::UnsafeCell;
}

/// Waits out another thread's last few instructions: a backoff, or a yield to loom's scheduler.
pub(crate) struct Spin {
  #[cfg(not(loom))]
  backoff: crossbeam_utils::Backoff,
}

impl Spin {
  #[inline]
  pub(crate) fn new() -> Self {
    Self {
      #[cfg(not(loom))]
      backoff: crossbeam_utils::Backoff::new(),
    }
  }

  #[inline]
  pub(crate) fn wait(&self) {
    #[cfg(not(loom))]
    self.backoff.snooze();
    #[cfg(loom)]
    loom::thread::yield_now();
  }
}
