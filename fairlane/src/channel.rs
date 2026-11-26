//! What the two halves share, and the constructors.

use core::{num::NonZeroUsize, panic::AssertUnwindSafe};

use event_listener::Event;
use hybrid_array::Array;

use crate::{
  arrivals::Arrivals,
  choice::Choice,
  lane::{Lane, PerLane, Single, lane_count},
  options::{LaneOptions, Malformed, Options},
  policy::{DefaultPolicy, Policy},
  receiver::Receiver,
  remaining::{Remaining, free},
  reserves::Reserves,
  ring::Ring,
  sender::Sender,
  snapshot::{Reading, Snapshot},
  sync::{Arc, AtomicBool, AtomicUsize, Mutex, Ordering, lock},
};

/// A channel of several lanes, with the default policy: a count and a time, whichever comes first
/// ([`DefaultPolicy`]).
///
/// # Errors
///
/// [`Malformed`], by name: no lane; a [`Lane`] whose indices do not round-trip; an express lane
/// behind one that is not; a capacity that cannot be held as a ring.
///
/// ```
/// use core::num::NonZeroUsize;
///
/// use fairlane::{Lane, LaneOptions, Options};
///
/// # #[cfg(feature = "derive")] {
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Lane)]
/// enum Urgency {
///   Interactive,
///   Batch,
/// }
///
/// let options = Options::from_fn(|lane| match lane {
///   Urgency::Interactive => LaneOptions::new(NonZeroUsize::new(64).unwrap()),
///   Urgency::Batch => LaneOptions::new(NonZeroUsize::new(1024).unwrap()),
/// });
/// let (tx, rx) = fairlane::channel::<&str, Urgency>(options)?;
/// tx.send_blocking(Urgency::Batch, "later")?;
/// tx.send_blocking(Urgency::Interactive, "now")?;
/// assert_eq!(rx.recv_blocking()?.into_item(), "now");
/// assert_eq!(rx.recv_blocking()?.into_item(), "later");
/// # }
/// # Ok::<(), Box<dyn core::error::Error>>(())
/// ```
pub fn channel<T, L: Lane>(
  options: Options<L>,
) -> Result<Halves<T, L, DefaultPolicy<L>>, Malformed> {
  pair(&options, DefaultPolicy::default())
}

/// A channel of several lanes, with `policy` against starvation.
///
/// # Errors
///
/// As [`channel`].
pub fn channel_with<T, L: Lane, Y: Policy<L>>(
  options: Options<L>,
  policy: Y,
) -> Result<Halves<T, L, Y>, Malformed> {
  pair(&options, policy)
}

/// A bounded channel of one lane, with the default policy, which a channel of one lane never
/// consults.
///
/// # Errors
///
/// [`Malformed::Capacity`] where `capacity` cannot be held as a ring: its power of two, or the
/// ring's cells, do not fit the address space.
///
/// ```
/// use core::num::NonZeroUsize;
///
/// use fairlane::{Closed, Single};
///
/// let (tx, rx) = fairlane::bounded::<u32>(NonZeroUsize::new(8).unwrap())?;
/// tx.send_blocking(Single, 7)?;
/// assert_eq!(rx.recv_blocking()?.into_item(), 7);
/// # Ok::<(), Box<dyn core::error::Error>>(())
/// ```
pub fn bounded<T>(capacity: NonZeroUsize) -> Result<(Sender<T>, Receiver<T>), Malformed> {
  let options = Options::<Single>::from_fn(|Single| LaneOptions::new(capacity));
  pair(&options, DefaultPolicy::default())
}

/// A channel's two halves.
pub(crate) type Halves<T, L, Y> = (Sender<T, L, Y>, Receiver<T, L, Y>);

/// Both halves of a new channel.
pub(crate) fn pair<T, L: Lane, Y: Policy<L>>(
  options: &Options<L>,
  policy: Y,
) -> Result<Halves<T, L, Y>, Malformed> {
  let shared = Arc::new(Shared::new(options, policy)?);
  Ok((Sender::new(shared.clone()), Receiver::new(shared)))
}

/// One lane's whole state.
pub(crate) struct LaneState<T, S> {
  pub(crate) ring: Ring<T, S>,
  pub(crate) remaining: Remaining,
  pub(crate) reserves: Reserves,
  pub(crate) arrivals: Arrivals,
  pub(crate) capacity: usize,
  /// 1 while the choice lock holds this lane's oldest item, popped to read its stamp; 0 else.
  pub(crate) ahead: AtomicUsize,
}

impl<T, S: Copy> LaneState<T, S> {
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
  /// Each lane's express flag, as the options resolved it.
  express: PerLane<bool, L>,
  /// The receiving side's lock, for two lanes or more: the passed-over counts and the lookahead.
  /// Senders never take it.
  choice: Option<Mutex<Choice<T, L, Y::Stamp>>>,
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
    let lanes = lane_count::<L>() >= 2;
    let express = PerLane::from_array(Array::from_fn(|index| options.is_express_at(index)));
    let states = Array::try_from_fn(|index| {
      let capacity = options.lane_at(index).capacity().get();
      // A stamp matters only to the policy, which never sees an express lane, nor the one lane
      // of a channel of one.
      let stamped = lanes && !*express.at(index);
      Ok(LaneState {
        ring: Ring::new(capacity, stamped).ok_or(Malformed::Capacity(index))?,
        remaining: Remaining::new(capacity),
        reserves: Reserves::new(),
        arrivals: Arrivals::new(),
        capacity,
        ahead: AtomicUsize::new(0),
      })
    })?;
    Ok(Self {
      lanes: PerLane::from_array(states),
      names: PerLane::from_fn(|lane| lane),
      express,
      choice: lanes.then(|| Mutex::new(Choice::new())),
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

  /// Whether lane `index` is express.
  #[inline]
  pub(crate) fn is_express_at(&self, index: usize) -> bool {
    *self.express.at(index)
  }

  /// The choice lock, for a channel of two lanes or more.
  #[inline]
  pub(crate) fn choice(&self) -> Option<&Mutex<Choice<T, L, Y::Stamp>>> {
    self.choice.as_ref()
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
      // An item the choice lock holds ahead is still queued: popped from the ring, not taken.
      let ahead = state.ahead.load(Ordering::Relaxed) as u64;
      let received = state.ring.received().saturating_sub(ahead);
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
    let mut ahead = Vec::new();
    if let Some(choice) = &self.choice {
      let mut choice = lock(choice);
      for (index, slot) in choice.ahead.as_mut_slice().iter_mut().enumerate() {
        if let Some(popped) = slot.take() {
          self.lanes.at(index).ahead.store(0, Ordering::Relaxed);
          ahead.push((index, popped.item));
        }
      }
    }
    // Each item is dropped after the choice lock is let go, under its own catch.
    for (index, item) in ahead {
      self.lanes.at(index).release();
      let _ = std::panic::catch_unwind(AssertUnwindSafe(move || drop(item)));
    }
    for state in self.lanes.as_slice() {
      while let Some(popped) = state.ring.pop() {
        state.release();
        let item = popped.item;
        let _ = std::panic::catch_unwind(AssertUnwindSafe(move || drop(item)));
      }
    }
  }
}

#[cfg(test)]
mod tests;
