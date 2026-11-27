//! What the two halves share, and the constructors.

use core::{
  any::Any,
  mem,
  num::NonZeroUsize,
  panic::AssertUnwindSafe,
  task::{Poll, Waker},
};

use hybrid_array::Array;

use crate::{
  arrivals::Arrivals,
  choice::Choice,
  lane::{Lane, PerLane, Single, index_of, lane_at, lane_count},
  options::{LaneOptions, Malformed, Options},
  policy::{DefaultPolicy, Policy},
  receiver::Receiver,
  remaining::{Remaining, free},
  reserves::Reserves,
  ring::{Refused, Ring},
  sender::Sender,
  snapshot::{Reading, Snapshot},
  sync::{Arc, AtomicBool, AtomicU64, AtomicUsize, Mutex, Ordering, lock},
  waiters::{self, Key, State, Waiters},
  wake::{Wakes, dispose, wake},
};

/// A channel of several lanes, with the default policy: a count and a time, whichever comes first
/// ([`DefaultPolicy`]).
///
/// # Errors
///
/// [`Malformed`], by name: no lane, or more than 256; a [`Lane`] whose indices do not round-trip;
/// an express lane behind one that is not; a capacity that cannot be held as a ring, laid out or
/// allocated.
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
  // The lane count first: the default policy keeps numbers per lane.
  Options::<L>::check_count()?;
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
/// ring's cells, do not fit the address space, or the ring's memory cannot be allocated.
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
  /// Whether the receiving side pops this lane's oldest item ahead, to read its stamp: a lane
  /// that is not express, of a channel of two lanes or more, under a policy whose stamp is not
  /// zero-sized.
  pub(crate) lookahead: bool,
  /// On a lane with a lookahead, every item taken out: the snapshot's `received`, stored by the
  /// holder of the choice lock and by the last receiver's drain. On a lane without one, that is
  /// the ring's head.
  pub(crate) taken: AtomicU64,
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
  /// One state per lane, by index, on the heap: a channel of many lanes is built without an array
  /// of lane states on the stack.
  lanes: Box<[LaneState<T, Y::Stamp>]>,
  /// Each index's lane, read once at the construction: the receiving side never calls back into
  /// `Lane` to name an item's lane. On the heap, built one lane at a time, as the lane states are:
  /// a large lane type makes no large table on the stack.
  names: Box<[L]>,
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
    let count = lane_count::<L>();
    let lanes = count >= 2;
    let express = PerLane::from_array(Array::from_fn(|index| options.is_express_at(index)));
    // A stamp matters only to the policy, which never sees an express lane, nor the one lane of a
    // channel of one.
    let stamped = |index: usize| lanes && !*express.at(index);
    let capacity = |index: usize| options.lane_at(index).capacity().get();
    // Every ring's layout is checked before the first ring is allocated, so a lane that cannot be
    // held is refused with nothing built.
    if let Some(index) =
      (0..count).find(|&index| !Ring::<T, Y::Stamp>::fits(capacity(index), stamped(index)))
    {
      return Err(Malformed::Capacity(index));
    }
    let mut states = Vec::with_capacity(count);
    for index in 0..count {
      states.push(LaneState {
        ring: Ring::new(capacity(index), stamped(index)).ok_or(Malformed::Capacity(index))?,
        remaining: Remaining::new(capacity(index)),
        reserves: Reserves::new(),
        arrivals: Arrivals::new(),
        capacity: capacity(index),
        lookahead: stamped(index) && size_of::<Y::Stamp>() != 0,
        taken: AtomicU64::new(0),
      });
    }
    let shared = Self {
      lanes: states.into_boxed_slice(),
      names: (0..count).map(lane_at::<L>).collect(),
      express,
      choice: lanes.then(|| Mutex::new(Choice::new())),
      policy,
      senders: AtomicUsize::new(1),
      receivers: AtomicUsize::new(1),
      closing: AtomicBool::new(false),
      closed: AtomicBool::new(false),
      closers: Mutex::new(Waiters::new()),
    };
    shared.warm();
    Ok(shared)
  }

  /// Takes every lock once, before the channel is shared, so that no later call is a lock's first
  /// use. What std allocates inside a lock, then or later, is std's own, outside the crate's
  /// allocation guarantee (the README, "Wakers, policies and items").
  fn warm(&self) {
    for state in self.lanes.iter() {
      state.reserves.warm();
      state.arrivals.warm();
    }
    drop(lock(&self.closers));
    if let Some(choice) = &self.choice {
      drop(lock(choice));
    }
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
    &self.lanes[index]
  }

  /// The channel's address: the name the re-entry marker knows it by.
  #[inline]
  pub(crate) fn address(&self) -> usize {
    core::ptr::from_ref(self).addr()
  }

  /// The lane at `index`, as read at the construction.
  #[inline]
  pub(crate) fn name(&self, index: usize) -> L {
    self.names[index]
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
  /// the permit was granted, or has spent its positions; the permit's room is let go then, and
  /// also where the policy's `stamp` unwinds before the commit.
  pub(crate) fn commit(&self, index: usize, item: T) -> Result<(), T> {
    let state = self.lane_at(index);
    // Declared after `item`, so that an unwind out of `stamp` gives the room back, and grants it,
    // before the item is dropped, as a refund after a commit does.
    let refund = Refund(state);
    // Taken here, before the item moves into the push, for the same reason. The push calls its
    // closure only where this found the ring taking stamps, so its fallback never runs.
    let stamp = state.ring.takes_stamp().then(|| self.policy.stamp());
    match state
      .ring
      .push(item, move || stamp.unwrap_or_else(|| self.policy.stamp()))
    {
      Ok(_) => {
        // The unit is the item's room now, and the receiver's waker runs after this.
        mem::forget(refund);
        wake(state.arrivals.wake_one());
        Ok(())
      }
      Err(Refused::Closed(item)) => {
        drop(refund);
        Err(item)
      }
      Err(Refused::Exhausted(item)) => {
        // Closed before the refund, so that the room lands on a closed lane and grants nobody.
        self.close_lane(index);
        drop(refund);
        Err(item)
      }
    }
  }

  /// Closes lane `index` alone, its ring's positions spent: its room, the reserves in its line and
  /// the receivers in its line, as a close does each lane. The ring is marked already, and the
  /// channel stays open. Every push that finds the lane so runs it, so the lane's waiters never
  /// wait on the thread that spent its last position.
  fn close_lane(&self, index: usize) {
    let state = self.lane_at(index);
    state.reserves.close(&state.remaining);
    state.ring.close();
    state.arrivals.close();
    let mut wakes = Wakes::new();
    state.reserves.dispatch(&mut wakes);
    state.arrivals.dispatch(&mut wakes);
    wakes.finish();
  }

  /// Closes every lane, in index order: its room first, so that no reserve is granted, then its
  /// ring's mark, so that no permit commits. Every call marks every lane before it returns, so
  /// nothing commits anywhere once any `close` has returned. True for the one call that came
  /// first.
  ///
  /// Everything a close wakes, its reserves, its receivers and its `closed()` futures, is woken
  /// once every state change of the close is done: a waker is user code, and one that panics
  /// leaves no lane open. The wakes are delivered from the waiters' own places, a chunk at a time,
  /// so fairlane allocates nothing after the first state change.
  pub(crate) fn close(&self) -> bool {
    let first = self.shut();
    let mut wakes = Wakes::new();
    self.dispatch_close(&mut wakes);
    wakes.finish();
    first
  }

  /// Every state change of a close, with no waker run and no allocation of fairlane's: each lane's
  /// room and its ring's mark, each lane's waiting receivers notified, the closed flag, and the
  /// `closed()` futures notified. Every wake is left owed in its waiter's place.
  fn shut(&self) -> bool {
    let first = !self.closing.swap(true, Ordering::SeqCst);
    for state in self.lanes.iter() {
      state.reserves.close(&state.remaining);
      state.ring.close();
    }
    for state in self.lanes.iter() {
      state.arrivals.close();
    }
    // `Release`, after every mark: a thread that reads the flag set (`Acquire`) sees every lane
    // closed, so a send it makes afterwards reads its lane's mark.
    self.closed.store(true, Ordering::Release);
    lock(&self.closers).notify_all();
    first
  }

  /// Wakes every waiter a close left owed: each lane's reserves and receivers, then the `closed()`
  /// futures.
  fn dispatch_close(&self, wakes: &mut Wakes) {
    for state in self.lanes.iter() {
      state.reserves.dispatch(wakes);
      state.arrivals.dispatch(wakes);
    }
    waiters::dispatch(&self.closers, wakes);
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
              if let Err(error) = closers.make_room() {
                drop(closers);
                drop(new);
                waiters::full(error);
              }
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
      let state = &self.lanes[index];
      // One reading each. An item the choice lock holds ahead is popped and not taken, so a lane
      // with a lookahead counts its takes apart: its head moves when an item moves into the
      // lookahead, before any receive takes it.
      let received = if state.lookahead {
        state.taken.load(Ordering::Relaxed)
      } else {
        state.ring.received()
      };
      let committed = state.ring.committed();
      // Derived from two readings: received first, so that the difference is never below what
      // was queued at the second; clamped to what the lane can hold.
      let queued = usize::try_from(committed.saturating_sub(received))
        .unwrap_or(usize::MAX)
        .min(state.capacity);
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
  ///
  /// The wakes this close resolves run before the destructors of the items it drops. A wake that a
  /// concurrent close took from its waiter's place first is that close's to deliver, and may run
  /// later; the item contract makes neither order one a destructor can wait on. A waker's panic is
  /// held until every item and every caught panic is disposed of, and raised then.
  pub(crate) fn drop_receiver(&self) {
    if self.receivers.fetch_sub(1, Ordering::AcqRel) != 1 {
      return;
    }
    self.shut();
    let mut wakes = Wakes::new();
    self.dispatch_close(&mut wakes);
    // Each item is dropped after the choice lock is let go, under its own catch. The close has
    // emptied every line, so a room given back here grants no reserve. The lookahead is drained
    // one lane at a time, the lock taken for each, so that nothing is collected.
    let mut payloads = Vec::new();
    if let Some(choice) = &self.choice {
      // No re-entry check: a receive inside this choice borrows a live receiver, so none is last.
      for (index, state) in self.lanes.iter().enumerate() {
        let popped = {
          let mut choice = lock(choice);
          let popped = choice.ahead[index].take();
          if let Some(popped) = &popped {
            state.taken.store(popped.ordinal + 1, Ordering::Relaxed);
          }
          popped
        };
        if let Some(popped) = popped {
          wakes.wake(state.release());
          contain(popped.item, &mut payloads);
        }
      }
    }
    for state in self.lanes.iter() {
      while let Some(popped) = state.ring.pop() {
        if state.lookahead {
          state.taken.store(popped.ordinal + 1, Ordering::Relaxed);
        }
        wakes.wake(state.release());
        contain(popped.item, &mut payloads);
      }
    }
    // The panics are disposed of once both drains are done: a payload whose own drop panics
    // then stops no drain.
    for payload in payloads {
      dispose(payload);
    }
    wakes.finish();
  }
}

/// Drops `item` under its own catch, keeping the panic of a destructor that panics in `payloads`,
/// to be disposed of once both drains are done. Where `payloads` has no room for it, the panic is
/// disposed of at once: the drain goes on either way, and allocates nothing it cannot do without.
fn contain<T>(item: T, payloads: &mut Vec<Box<dyn Any + Send>>) {
  if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(move || drop(item))) {
    if payloads.try_reserve(1).is_ok() {
      payloads.push(payload);
    } else {
      dispose(payload);
    }
  }
}

#[cfg(test)]
mod tests;
