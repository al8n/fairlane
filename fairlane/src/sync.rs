//! The atomics and cells the crate's concurrent code uses: std's, or loom's under `cfg(loom)`.
//! Everything that loom must see goes through here.

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU64, Ordering};

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
