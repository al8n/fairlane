//! What the two halves share, and the constructors.

use core::panic::AssertUnwindSafe;

use event_listener::Event;
use hybrid_array::Array;

use crate::{
  arrivals::Arrivals,
  lane::{Lane, PerLane, lane_count},
  options::{Malformed, Options},
  policy::Policy,
  remaining::{Remaining, free},
  reserves::Reserves,
  ring::Ring,
  snapshot::{Reading, Snapshot},
  sync::{Arc, AtomicBool, AtomicUsize, Ordering},
};

/// One lane's whole state.
pub(crate) struct LaneState<T, S> {
  pub(crate) ring: Ring<T, S>,
  pub(crate) remaining: Remaining,
  pub(crate) reserves: Reserves,
  pub(crate) arrivals: Arrivals,
  pub(crate) capacity: usize,
}

impl<T, S> LaneState<T, S> {
  /// One unit of room back to the lane.
  #[inline]
  pub(crate) fn release(&self) {
    self.reserves.release(&self.remaining);
  }
}

pub(crate) struct Shared<T, L: Lane, Y: Policy<L>> {
  lanes: PerLane<LaneState<T, Y::Stamp>, L>,
  /// Each index's lane, read once at the construction: the receiving side never calls back into
  /// `Lane` to name an item's lane.
  names: PerLane<L, L>,
  policy: Y,
  senders: AtomicUsize,
  receivers: AtomicUsize,
  /// Set by the first `close`, so that exactly one call answers `true`.
  closing: AtomicBool,
  /// Woken once every lane is closed: `WhenClosed`.
  closed: Event,
}

impl<T, L: Lane, Y: Policy<L>> Shared<T, L, Y> {
  fn new(options: &Options<L>, policy: Y) -> Result<Self, Malformed> {
    options.check()?;
    let stamped = lane_count::<L>() >= 2;
    let lanes = Array::try_from_fn(|index| {
      let capacity = options.lane_at(index).capacity().get();
      Ok(LaneState {
        ring: Ring::new(capacity, stamped).ok_or(Malformed::Capacity(index))?,
        remaining: Remaining::new(capacity),
        reserves: Reserves::new(),
        arrivals: Arrivals::new(),
        capacity,
      })
    })?;
    Ok(Self {
      lanes: PerLane::from_array(lanes),
      names: PerLane::from_fn(|lane| lane),
      policy,
      senders: AtomicUsize::new(1),
      receivers: AtomicUsize::new(1),
      closing: AtomicBool::new(false),
      closed: Event::new(),
    })
  }

  #[inline]
  pub(crate) fn lane(&self, lane: L) -> &LaneState<T, Y::Stamp> {
    self.lanes.get(lane)
  }

  #[inline]
  pub(crate) fn lane_at(&self, index: usize) -> &LaneState<T, Y::Stamp> {
    self.lanes.at(index)
  }

  /// The lane at `index`, as read at the construction.
  #[inline]
  pub(crate) fn name(&self, index: usize) -> L {
    *self.names.at(index)
  }

  #[inline]
  pub(crate) fn policy(&self) -> &Y {
    &self.policy
  }

  /// Commits `item` through a permit of `lane`, or hands it back where the lane closed after the
  /// permit was granted; the permit's room is let go then.
  pub(crate) fn commit(&self, lane: L, item: T) -> Result<(), T> {
    let state = self.lane(lane);
    match state.ring.push(item, || self.policy.stamp()) {
      Ok(_) => {
        state.arrivals.wake_one();
        Ok(())
      }
      Err(item) => {
        state.release();
        Err(item)
      }
    }
  }

  /// Closes every lane, in index order: its room first, so that no reserve is granted, then its
  /// ring's mark, so that no permit commits. Every call marks every lane before it returns, so
  /// nothing commits anywhere once any `close` has returned. True for the one call that came
  /// first.
  pub(crate) fn close(&self) -> bool {
    let first = !self.closing.swap(true, Ordering::SeqCst);
    for state in self.lanes.as_slice() {
      state.reserves.close(&state.remaining);
      state.ring.close();
    }
    for state in self.lanes.as_slice() {
      state.arrivals.wake_all();
    }
    self.closed.notify(usize::MAX);
    first
  }

  /// Whether every lane is closed. A `close` marks the lanes in index order, so the last lane's
  /// mark is set only once every lane's is.
  #[inline]
  pub(crate) fn is_closed(&self) -> bool {
    self
      .lanes
      .as_slice()
      .last()
      .is_some_and(|state| state.ring.is_closed())
  }

  #[inline]
  pub(crate) fn closed_event(&self) -> &Event {
    &self.closed
  }

  pub(crate) fn snapshot(&self) -> Snapshot<L> {
    let readings = Array::from_fn(|index| {
      let state = self.lanes.at(index);
      let received = state.ring.received();
      let committed = state.ring.committed();
      let queued = usize::try_from(committed.saturating_sub(received)).unwrap_or(usize::MAX);
      let remaining = free(state.remaining.load());
      Reading {
        queued,
        reserved: state
          .capacity
          .saturating_sub(remaining)
          .saturating_sub(queued),
        remaining,
        senders_waiting: state.reserves.parked(),
        receivers_waiting: state.arrivals.waiting(),
        committed,
        received,
      }
    });
    Snapshot::new(PerLane::from_array(readings), self.is_closed())
  }

  pub(crate) fn add_sender(&self) {
    self.senders.fetch_add(1, Ordering::Relaxed);
  }

  /// The last sender gone closes the channel; what is queued can still be received.
  pub(crate) fn drop_sender(&self) {
    if self.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
      self.close();
    }
  }

  pub(crate) fn add_receiver(&self) {
    self.receivers.fetch_add(1, Ordering::Relaxed);
  }

  /// The last receiver gone closes the channel and lets go of every queued item, each dropped
  /// under its own catch, so that one destructor that panics does not leak the rest.
  pub(crate) fn drop_receiver(&self) {
    if self.receivers.fetch_sub(1, Ordering::AcqRel) != 1 {
      return;
    }
    self.close();
    for state in self.lanes.as_slice() {
      while let Some(popped) = state.ring.pop() {
        state.release();
        let item = popped.item;
        let _ = std::panic::catch_unwind(AssertUnwindSafe(move || drop(item)));
      }
    }
  }
}
