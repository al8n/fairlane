use core::time::Duration;
use std::time::Instant;

/// A reading of a monotonic clock: nanoseconds since that clock's own start.
///
/// Each clock starts at an instant of its own, so a moment of one clock means nothing beside a
/// moment of another. A policy compares only the moments its own clock gave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Moment(u64);

impl Moment {
  /// The moment `nanos` nanoseconds after its clock's start.
  #[inline]
  pub const fn from_nanos(nanos: u64) -> Self {
    Self(nanos)
  }

  /// Nanoseconds since its clock's start.
  #[inline]
  pub const fn as_nanos(self) -> u64 {
    self.0
  }

  /// The time from `earlier` to this moment, or zero where `earlier` is the later one.
  #[inline]
  pub const fn saturating_since(self, earlier: Moment) -> Duration {
    Duration::from_nanos(self.0.saturating_sub(earlier.0))
  }
}

/// A source of [`Moment`]s, never going back.
///
/// It is read on whichever thread commits or receives, so it must not depend on that thread.
pub trait Clock: Send + Sync + 'static {
  /// The present moment.
  fn now(&self) -> Moment;
}

/// The machine's monotonic clock, started when the value is made.
#[derive(Debug, Clone, Copy)]
pub struct Monotonic {
  start: Instant,
}

impl Monotonic {
  /// A clock whose moment 0 is now.
  #[inline]
  pub fn new() -> Self {
    Self {
      start: Instant::now(),
    }
  }
}

impl Default for Monotonic {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl Clock for Monotonic {
  #[inline]
  fn now(&self) -> Moment {
    Moment(saturating_nanos(self.start.elapsed()))
  }
}

/// Nanoseconds in `duration`, saturating at `u64::MAX` (about 584 years).
#[inline]
pub(crate) fn saturating_nanos(duration: Duration) -> u64 {
  u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(feature = "test-util")]
pub use manual::ManualClock;

#[cfg(feature = "test-util")]
mod manual {
  use core::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
  };
  use std::sync::Arc;

  use super::{Clock, Moment, saturating_nanos};

  /// A clock that a test sets. Its clones share one reading, so a test keeps one and hands
  /// another to the policy.
  #[derive(Debug, Clone, Default)]
  pub struct ManualClock {
    nanos: Arc<AtomicU64>,
  }

  impl ManualClock {
    /// A clock that reads `Moment::from_nanos(0)` until it is moved.
    #[inline]
    pub fn new() -> Self {
      Self::default()
    }

    /// Moves the clock on by `by`, saturating.
    pub fn advance(&self, by: Duration) {
      let by = saturating_nanos(by);
      let mut current = self.nanos.load(Ordering::SeqCst);
      while let Err(seen) = self.nanos.compare_exchange_weak(
        current,
        current.saturating_add(by),
        Ordering::SeqCst,
        Ordering::SeqCst,
      ) {
        current = seen;
      }
    }

    /// Sets the clock to `to`. A test that moves it back breaks [`Clock`]'s promise itself.
    #[inline]
    pub fn set(&self, to: Moment) {
      self.nanos.store(to.as_nanos(), Ordering::SeqCst);
    }
  }

  impl Clock for ManualClock {
    #[inline]
    fn now(&self) -> Moment {
      Moment::from_nanos(self.nanos.load(Ordering::SeqCst))
    }
  }
}

#[cfg(test)]
mod tests;
