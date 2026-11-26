//! A lane's waiting receivers, as a count and one event.
//!
//! A receiver going to sleep listens first, then counts itself, then looks again. A commit moves
//! the tail and then reads the count, and notifies only where it is not zero. Both the count's
//! write and the tail's move are sequentially consistent, so one of the two sees the other.
//! The count keeps `notify`, which takes event-listener's list lock every time, off a commit
//! that nobody waits for.

use event_listener::{Event, EventListener};

use crate::sync::{AtomicUsize, Ordering, pair_fence};

pub(crate) struct Arrivals {
  count: AtomicUsize,
  event: Event,
}

impl Arrivals {
  pub(crate) fn new() -> Self {
    Self {
      count: AtomicUsize::new(0),
      event: Event::new(),
    }
  }

  /// Listens, then counts: the receiver looks again after this.
  #[inline]
  pub(crate) fn listen(&self) -> EventListener {
    let listener = self.event.listen();
    self.count.fetch_add(1, Ordering::SeqCst);
    pair_fence();
    listener
  }

  /// The receiver stops waiting.
  #[inline]
  pub(crate) fn leave(&self) {
    self.count.fetch_sub(1, Ordering::SeqCst);
  }

  /// Wakes one waiting receiver, where any waits. One already notified and not yet awake counts
  /// as that one.
  #[inline]
  pub(crate) fn wake_one(&self) {
    pair_fence();
    if self.count.load(Ordering::SeqCst) != 0 {
      self.event.notify(1);
    }
  }

  /// Wakes every waiting receiver: at a close.
  #[inline]
  pub(crate) fn wake_all(&self) {
    self.event.notify(usize::MAX);
  }

  /// Receivers counted as waiting.
  #[inline]
  pub(crate) fn waiting(&self) -> usize {
    self.count.load(Ordering::SeqCst)
  }
}
