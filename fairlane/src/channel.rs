//! What the two halves share, and the constructors.

use core::{
  mem,
  num::NonZeroUsize,
  panic::AssertUnwindSafe,
  task::{Poll, Waker},
};

use hybrid_array::Array;

use crate::{
  arrivals::Arrivals,
  choice::Choice,
  lane::{Lane, PerLane, Single, index_of, lane_count},
  options::{LaneOptions, Malformed, Options},
  policy::{DefaultPolicy, Policy},
  receiver::Receiver,
  reentry,
  remaining::{Remaining, free},
  reserves::Reserves,
  ring::Ring,
  sender::Sender,
  snapshot::{Reading, Snapshot},
  sync::{Arc, AtomicBool, AtomicUsize, Mutex, Ordering, lock},
  waiters::{Key, State, Waiters},
  wake::{Wakes, wake, wake_all},
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

impl<T, S> LaneState<T, S> {
  /// One unit of room back to the lane. Answers the waker of a reserve it granted, for the caller
  /// to wake once its own state changes are done.
  #[inline]
  #[must_use = "the granted reserve's waker is to be woken"]
  pub(crate) fn release(&self) -> Option<Waker> {
    self.reserves.release(&self.remaining)
  }
}

/// A permit's unit while its commit runs: given back when dropped, as on a refused push or an
/// unwind out of the policy's `stamp`, and forgotten once the push claims its position, where the
/// unit becomes the item's room.
struct Refund<'a, T, S>(&'a LaneState<T, S>);

impl<T, S> Drop for Refund<'_, T, S> {
  fn drop(&mut self) {
    wake(self.0.release());
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
  /// Set by a `close` once it has marked every lane: `is_closed`.
  closed: AtomicBool,
  /// The `closed()` futures that wait, notified once the channel is closed.
  closers: Mutex<Waiters>,
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
      closed: AtomicBool::new(false),
      closers: Mutex::new(Waiters::new()),
    })
  }

  /// The index of `lane`: one call into [`Lane::index`], checked against `L::N`. A door resolves
  /// its lane once and carries the index to its commit or its refund, so a `Lane` whose answer
  /// changes cannot send a permit's item into a lane it holds no room in.
  #[inline]
  pub(crate) fn resolve(&self, lane: L) -> usize {
    index_of(lane)
  }

  #[inline]
  pub(crate) fn lane_at(&self, index: usize) -> &LaneState<T, Y::Stamp> {
    self.lanes.at(index)
  }

  /// The channel's address: the name the re-entry marker knows it by.
  #[inline]
  pub(crate) fn address(&self) -> usize {
    core::ptr::from_ref(self).addr()
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

  /// Commits `item` through a permit of lane `index`, or hands it back where the lane closed after
  /// the permit was granted; the permit's room is let go then, and also where the policy's `stamp`
  /// unwinds before the commit.
  pub(crate) fn commit(&self, index: usize, item: T) -> Result<(), T> {
    let state = self.lane_at(index);
    let refund = Refund(state);
    match state.ring.push(item, || self.policy.stamp()) {
      Ok(_) => {
        // The unit is the item's room now, and the receiver's waker runs after this.
        mem::forget(refund);
        wake(state.arrivals.wake_one());
        Ok(())
      }
      Err(item) => {
        drop(refund);
        Err(item)
      }
    }
  }

  /// Closes every lane, in index order: its room first, so that no reserve is granted, then its
  /// ring's mark, so that no permit commits. Every call marks every lane before it returns, so
  /// nothing commits anywhere once any `close` has returned. True for the one call that came
  /// first.
  ///
  /// Everything a close wakes, its reserves, its receivers and its `closed()` futures, is woken
  /// once every state change of the close is done: a waker is user code, and one that panics
  /// leaves no lane open.
  pub(crate) fn close(&self) -> bool {
    let mut wakers = Vec::new();
    let first = self.shut(&mut wakers);
    wake_all(wakers);
    first
  }

  /// Every state change of a close, and no waker run: each lane's room and its ring's mark, each
  /// lane's waiting receivers notified, the closed flag, and the `closed()` futures notified. The
  /// wakers go onto `wakers`.
  fn shut(&self, wakers: &mut Vec<Waker>) -> bool {
    let first = !self.closing.swap(true, Ordering::SeqCst);
    for state in self.lanes.as_slice() {
      state.reserves.close(&state.remaining, wakers);
      state.ring.close();
    }
    for state in self.lanes.as_slice() {
      state.arrivals.close(wakers);
    }
    // `Release`, after every mark: a thread that reads the flag set (`Acquire`) sees every lane
    // closed, so a send it makes afterwards reads its lane's mark.
    self.closed.store(true, Ordering::Release);
    lock(&self.closers).notify_all(wakers);
    first
  }

  /// Whether the channel is closed: a close has marked every lane.
  #[inline]
  pub(crate) fn is_closed(&self) -> bool {
    self.closed.load(Ordering::Acquire)
  }

  /// Polls a `closed()` future, whose place in the closers' line is `key`: ready once the channel
  /// is closed, else in line with `waker`.
  pub(crate) fn poll_closed(&self, key: &mut Option<Key>, waker: &Waker) -> Poll<()> {
    if key.is_none() && self.is_closed() {
      return Poll::Ready(());
    }
    let mut wakes = Wakes::new();
    let mut fresh: Option<Waker> = None;
    let polled = loop {
      {
        let mut closers = lock(&self.closers);
        match key {
          // The flag is read under the lock a close takes after it sets the flag: either this
          // read sees it, or the close finds this future in line.
          None if self.is_closed() => break Poll::Ready(()),
          None => {
            if let Some(new) = fresh.take() {
              *key = Some(closers.insert(new));
              break Poll::Pending;
            }
          }
          Some(place) => match closers.state(place) {
            State::Queued if closers.will_wake(place, waker) => break Poll::Pending,
            State::Queued => {
              if let Some(new) = fresh.take() {
                let replaced = closers.set_waker(place, new);
                drop(closers);
                wakes.discard(replaced);
                break Poll::Pending;
              }
            }
            State::Notified | State::Closed => {
              let place = key.take().expect("the future is in line");
              let (_, stored) = closers.remove(place);
              drop(closers);
              wakes.discard(stored);
              break Poll::Ready(());
            }
          },
        }
      }
      // A clone is user code: it is made with the lock let go, and the line read again after it.
      fresh = Some(waker.clone());
    };
    wakes.discard(fresh);
    wakes.finish();
    polled
  }

  /// A `closed()` future dropped: it leaves the closers' line. A close wakes every future in line,
  /// so there is no notification to pass on.
  pub(crate) fn leave_closed(&self, key: &mut Option<Key>) {
    if let Some(place) = key.take() {
      let (_, stored) = lock(&self.closers).remove(place);
      let mut wakes = Wakes::new();
      wakes.discard(stored);
      wakes.finish();
    }
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
  /// under its own catch, so that one destructor that panics does not leak the rest. What the
  /// close wakes is woken once the items are let go.
  pub(crate) fn drop_receiver(&self) {
    if self.receivers.fetch_sub(1, Ordering::AcqRel) != 1 {
      return;
    }
    let mut wakers = Vec::new();
    self.shut(&mut wakers);
    let mut ahead = Vec::new();
    if let Some(choice) = &self.choice {
      // A receive in progress holds a `Receiver`, so no policy can drop the last one from inside
      // a choice of this channel; the refusal stands guard all the same.
      reentry::refuse(self.address(), "dropped the last receiver");
      let mut choice = lock(choice);
      for (index, slot) in choice.ahead.iter_mut().enumerate() {
        if let Some(popped) = slot.take() {
          self.lanes.at(index).ahead.store(0, Ordering::Relaxed);
          ahead.push((index, popped.item));
        }
      }
    }
    // Each item is dropped after the choice lock is let go, under its own catch. The close has
    // emptied every line, so a room given back here grants no reserve.
    for (index, item) in ahead {
      wakers.extend(self.lanes.at(index).release());
      let _ = std::panic::catch_unwind(AssertUnwindSafe(move || drop(item)));
    }
    for state in self.lanes.as_slice() {
      while let Some(popped) = state.ring.pop() {
        wakers.extend(state.release());
        let item = popped.item;
        let _ = std::panic::catch_unwind(AssertUnwindSafe(move || drop(item)));
      }
    }
    wake_all(wakers);
  }
}

#[cfg(test)]
mod tests;
