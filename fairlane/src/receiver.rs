use core::{
  fmt,
  future::Future,
  num::NonZeroUsize,
  pin::Pin,
  task::{Context, Poll, Waker},
};

use hybrid_array::Array;

use crate::{
  arrivals::Receive,
  channel::Shared,
  choice::{Choice, choose, fits},
  error::Ended,
  lane::{Lane, PerLane, Single, lane_count},
  park::park_on,
  policy::{DefaultPolicy, LaneView, Policy},
  quota::Quota,
  reentry::{self, Choosing},
  snapshot::Snapshot,
  sync::{Arc, Mutex, Ordering, lock},
  waiters::Key,
  wake::{Staged, Wakes},
};

/// The receiving half of a channel. Clones share the channel: each item goes to one receive.
/// The last one dropped closes the channel and lets go of what is queued, on its own thread. It
/// runs the wakes its own close resolves before the destructors of the items it drops; a wake that
/// a concurrent close is delivering may run later. No destructor may rely on either order: it must
/// not block on anyone who waits on this channel (the crate's documentation, "Wakers, policies and
/// items").
///
/// The last one drops the queued items as a `Vec` does: if an item's destructor panics, the panic
/// unwinds out of the drop, and the rest are still dropped, and their room given back, while it
/// unwinds; a second destructor that panics then aborts the process.
pub struct Receiver<T, L: Lane = Single, Y: Policy<L> = DefaultPolicy<L>> {
  shared: Arc<Shared<T, L, Y>>,
}

/// One item as it left the channel.
pub struct Received<T, L: Lane> {
  item: T,
  lane: L,
  ordinal: u64,
}

impl<T, L: Lane> Received<T, L> {
  /// The item.
  #[inline]
  pub fn item(&self) -> &T {
    &self.item
  }

  /// The lane it was committed to.
  #[inline]
  pub fn lane(&self) -> L {
    self.lane
  }

  /// Its place among everything ever committed to its lane: 0 for the lane's first item, and no
  /// gap. Order is defined within a lane; across lanes, priority decides.
  ///
  /// A lane takes 2^63 − 1 items in its life, so an ordinal is below that. The send that finds
  /// every one spent closes that lane alone, and gets its item back.
  #[inline]
  pub fn ordinal(&self) -> u64 {
    self.ordinal
  }

  /// The item, by value.
  #[inline]
  pub fn into_item(self) -> T {
    self.item
  }
}

impl<T: fmt::Debug, L: Lane> fmt::Debug for Received<T, L> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Received")
      .field("item", &self.item)
      .field("lane", &self.lane)
      .field("ordinal", &self.ordinal)
      .finish()
  }
}

/// Why a batched receive took nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecvManyError {
  /// Every lane the quota admits is closed and empty.
  Ended,
  /// The quota admits no lane: its limit is 0, its range holds no lane, or every lane in its
  /// range has a limit of 0. Refused by name, before any wait.
  EmptyQuota,
}

impl From<Ended> for RecvManyError {
  #[inline]
  fn from(_: Ended) -> Self {
    Self::Ended
  }
}

impl fmt::Display for RecvManyError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Ended => fmt::Display::fmt(&Ended, f),
      Self::EmptyQuota => f.write_str("the quota admits no lane"),
    }
  }
}

impl core::error::Error for RecvManyError {}

/// What one receive found.
pub(crate) enum Took {
  /// It took this many items, at least one.
  Items(NonZeroUsize),
  /// Nothing now.
  Nothing,
  /// Every lane the quota admits is closed and empty.
  Ended,
}

/// Where a receive puts the items it takes.
///
/// An item leaves its lane only into room the sink has made for it, so that putting it there runs
/// no user code and cannot fail: between the pop and the sink, nothing can drop it.
pub(crate) trait Sink<T, L: Lane> {
  /// Makes room for one more item, before it is taken. False where there is none: the batch stops
  /// there, and keeps what it holds. `first` is true while the batch holds nothing.
  fn room(&mut self, first: bool) -> bool;

  /// Puts an item into the room [`room`](Self::room) made.
  fn put(&mut self, received: Received<T, L>);
}

/// A batch's `Vec`, grown one item at a time, as std grows it: reserving a whole quota up front
/// would reserve for items the batch never takes.
impl<T, L: Lane> Sink<T, L> for Vec<Received<T, L>> {
  fn room(&mut self, _: bool) -> bool {
    self.reserve(1);
    true
  }

  fn put(&mut self, received: Received<T, L>) {
    debug_assert!(self.len() < self.capacity(), "room was made first");
    self.push(received);
  }
}

/// A single receive's result.
impl<T, L: Lane> Sink<T, L> for Option<Received<T, L>> {
  fn room(&mut self, _: bool) -> bool {
    self.is_none()
  }

  fn put(&mut self, received: Received<T, L>) {
    *self = Some(received);
  }
}

impl<T, L: Lane, Y: Policy<L>> Shared<T, L, Y> {
  /// One receive under `quota`, without waiting: every item it takes goes to `sink` at once, before
  /// the next choice, so an unwind out of a policy never drops an item that was taken.
  ///
  /// The wakers of the reserves its units are granted to go to `granted`, and do not run here: the
  /// caller wakes them once the receive has left its lines and run the chain, so that a granted
  /// waker that blocks for a while holds no notification, and no chain, from another receiver.
  pub(crate) fn take(
    &self,
    quota: &Quota<L>,
    sink: &mut impl Sink<T, L>,
    granted: &mut Staged,
  ) -> Took {
    match self.choice() {
      None => self.take_one_lane(quota, sink, granted),
      Some(choice) => self.take_lanes(choice, quota, sink, granted),
    }
  }

  /// A channel of one lane: pops, and no lock.
  fn take_one_lane(
    &self,
    quota: &Quota<L>,
    sink: &mut impl Sink<T, L>,
    granted: &mut Staged,
  ) -> Took {
    let state = self.lane_at(0);
    let lane_limit = quota.limit_at(0);
    let mut held = 0;
    while fits(held, quota.limit(), lane_limit) {
      // A lane with no item asks the sink for no room: an empty or ended lane answers as such, and
      // grows nothing.
      if held == 0 && state.ring.is_empty() {
        break;
      }
      // Room for the item, and a place for the waker its room may be granted to, are both made
      // before the item leaves its lane: after the pop nothing can fail.
      if !sink.room(held == 0) {
        break;
      }
      granted.room();
      let Some(popped) = state.ring.pop() else {
        granted.unroom();
        break;
      };
      // The room goes back first, and the item to the sink. The wakers of the reserves the
      // batch's rooms are granted to are the caller's, to run once the receive has left its lines
      // and chained, so a waker that panics, or blocks, costs no item and holds back no room.
      granted.keep(state.release());
      sink.put(Received {
        item: popped.item,
        lane: self.name(0),
        ordinal: popped.ordinal,
      });
      held += 1;
    }
    match NonZeroUsize::new(held) {
      Some(held) => Took::Items(held),
      None if state.ring.is_ended() => Took::Ended,
      None => Took::Nothing,
    }
  }

  /// A channel of two lanes or more: one receiver chooses at a time, under the choice lock, for a
  /// whole batch. For each item:
  ///
  /// 1. the lanes the quota admits are read, LOWEST priority first: whether each holds an item,
  ///    how many, and the stamp of its oldest. Only the lock's holder pops, so a lane can gain
  ///    items and not lose any: a lane read as empty was empty from the lock's taking until its
  ///    read, and every lane above the chosen one was found empty after it was;
  /// 2. the first express lane that holds an item is chosen; else the lane the policy says is owed;
  ///    else the first lane that holds an item;
  /// 3. where taking it would break the chosen lane's limit, the batch STOPS: it never takes
  ///    another lane instead;
  /// 4. a lane that is not express is counted: its passed-over count to zero, and one more for
  ///    every other lane of the view.
  ///
  /// From the pop of an item to its delivery to the sink, no user code runs: its room is recorded,
  /// the counts move by index, and its lane's name comes from the table read at the construction.
  /// The policy and the `Lane` methods it calls run only before the pop, while every item already
  /// taken is in the sink and every item looked ahead at is in the choice state.
  ///
  /// A batch that holds as many items as its effective limit stops before it reads the lanes
  /// again: no policy call comes after its last item, so a receive of one item makes none after
  /// its take.
  ///
  /// The room of every item taken is given back after the lock is let go, even on an unwind out of
  /// the policy, so that no waker runs under the lock.
  fn take_lanes(
    &self,
    choice: &Mutex<Choice<T, L, Y::Stamp>>,
    quota: &Quota<L>,
    sink: &mut impl Sink<T, L>,
    granted: &mut Staged,
  ) -> Took {
    let count = lane_count::<L>();
    // Declared first, so that it is dropped last: unlocked and unmarked.
    let mut back = RoomBack {
      shared: self,
      units: PerLane::filled(0),
      granted,
    };
    reentry::refuse(self.address(), "received");
    let mut guard = lock(choice);
    // Dropped first, on every exit, an unwind out of the policy included.
    let _choosing = Choosing::enter(self.address());
    let choice = &mut *guard;
    let mut now: Option<Y::Stamp> = None;
    let mut held = 0;
    let mut limit = quota.limit();
    while held < limit {
      let mut waiting = PerLane::<bool, L>::filled(false);
      for index in (0..count).rev() {
        if !quota.admits_at(index) {
          continue;
        }
        let state = self.lane_at(index);
        let ahead = &mut choice.ahead[index];
        let mut holds = ahead.is_some() || !state.ring.is_empty();
        if holds && ahead.is_none() && state.lookahead {
          // The oldest stamp is read from the item itself: pop it ahead. Only this lock's holder
          // pops, so the ring keeps the item it showed.
          *ahead = state.ring.pop();
          holds = ahead.is_some();
        }
        waiting.as_mut_slice()[index] = holds;
      }
      let express =
        (0..count).find(|&index| waiting.as_slice()[index] && self.is_express_at(index));
      // The view: the admitted lanes that hold an item and are not express.
      let in_view = |index: usize| waiting.as_slice()[index] && !self.is_express_at(index);
      let chosen = match express {
        Some(index) => index,
        None => {
          let mut lanes = (0..count).filter(|&index| in_view(index));
          let Some(first) = lanes.next() else {
            break;
          };
          if lanes.next().is_none() {
            first
          } else {
            let now = *now.get_or_insert_with(|| self.policy().stamp());
            let mut lane_view = LaneView::new(now);
            for index in (0..count).filter(|&index| in_view(index)) {
              let state = self.lane_at(index);
              let ahead = choice.ahead[index].as_ref();
              let queued = state
                .ring
                .committed()
                .saturating_sub(state.ring.received())
                .saturating_add(u64::from(ahead.is_some()));
              // A lane in the view holds an item. A zero-sized stamp is not looked ahead at,
              // since it records nothing: its one value, `now`, is the oldest item's, as a
              // projection of the same policy through a larger stamp reads it.
              let oldest = if size_of::<Y::Stamp>() == 0 {
                Some(now)
              } else {
                ahead.and_then(|popped| popped.stamp)
              };
              lane_view = lane_view.with_index(
                index,
                usize::try_from(queued).unwrap_or(usize::MAX),
                oldest,
                *choice.passed_over.at(index),
              );
            }
            let owed = choose(&lane_view, self.policy()).map_or(first, Lane::index);
            if owed < count && in_view(owed) {
              owed
            } else {
              first
            }
          }
        }
      };

      let lane_limit = quota.limit_at(chosen);
      // Room for the item, and a place for the waker its room may be granted to, are both made
      // before the item leaves its lane: after the pop nothing can fail.
      if !fits(held, limit, lane_limit) || !sink.room(held == 0) {
        break;
      }
      back.granted.room();
      let state = self.lane_at(chosen);
      let popped = match choice.ahead[chosen].take() {
        Some(popped) => popped,
        None => match state.ring.pop() {
          Some(popped) => popped,
          None => {
            back.granted.unroom();
            break;
          }
        },
      };
      if state.lookahead {
        // Stored only by this lock's holder, in ordinal order: the snapshot's `received` never
        // goes backward.
        state.taken.store(popped.ordinal + 1, Ordering::Relaxed);
      }
      back.units.as_mut_slice()[chosen] += 1;
      held += 1;
      limit = limit.min(lane_limit);
      if express.is_none() {
        let passed_over = choice.passed_over.as_mut_slice();
        for index in (0..count).filter(|&index| index != chosen && in_view(index)) {
          passed_over[index] = passed_over[index].saturating_add(1);
        }
        passed_over[chosen] = 0;
      }
      sink.put(Received {
        item: popped.item,
        lane: self.name(chosen),
        ordinal: popped.ordinal,
      });
    }
    match NonZeroUsize::new(held) {
      Some(held) => Took::Items(held),
      None => {
        let ended = (0..count)
          .filter(|&index| quota.admits_at(index))
          .all(|index| choice.ahead[index].is_none() && self.lane_at(index).ring.is_ended());
        if ended { Took::Ended } else { Took::Nothing }
      }
    }
  }

  /// After a receive that took items: where items are left in a lane the quota admits, one more
  /// waiting receiver of that lane is woken. Every admitted lane is looked at, not only the one
  /// this receive woke on.
  ///
  /// - **What a lane holds is read where no item is in transit.** On a channel of two lanes or
  ///   more, every pop is made under the choice lock, and an item popped ahead stays in the
  ///   lookahead until a choice takes it. Read under that lock, a lane's ring and its lookahead
  ///   together show every item it holds: one moving from the ring into the lookahead cannot look
  ///   like neither. The lock is let go before any waker runs. A channel of one lane has no
  ///   lookahead, and its ring alone shows what it holds.
  /// - **The lock is taken only where a receiver waits** on a lane the quota admits.
  /// - **Every lane's notification is made before any waker runs,** each then under its own
  ///   catch. A waker that panics leaves no other lane's receiver asleep. One that blocks for a
  ///   while, as an unpark may, delays the wakes after it without losing them: their notifications
  ///   are already made.
  /// - **It runs before the take's granted wakers,** which wait for it ([`take`](Self::take)): a
  ///   granted waker that blocks for a while delays only the wakes staged after it in that take,
  ///   and leaves no receiver asleep.
  pub(crate) fn chain(&self, quota: &Quota<L>) {
    let count = lane_count::<L>();
    let waited =
      |index: usize| quota.admits_at(index) && self.lane_at(index).arrivals.waiting() != 0;
    if !(0..count).any(waited) {
      return;
    }
    let holds: PerLane<bool, L> = match self.choice() {
      Some(choice) => {
        let choice = lock(choice);
        PerLane::from_array(Array::from_fn(|index| {
          waited(index) && (choice.ahead[index].is_some() || !self.lane_at(index).ring.is_empty())
        }))
      }
      None => PerLane::from_array(Array::from_fn(|index| {
        waited(index) && !self.lane_at(index).ring.is_empty()
      })),
    };
    let mut wakers: PerLane<Option<Waker>, L> = PerLane::from_array(Array::from_fn(|_| None));
    for (index, waker) in wakers.as_mut_slice().iter_mut().enumerate() {
      if holds.as_slice()[index] {
        *waker = self.notify(index);
      }
    }
    let mut wakes = Wakes::new();
    for waker in wakers.as_mut_slice() {
      wakes.wake(waker.take());
    }
    wakes.finish();
  }
}

/// What a receive's poll owes the other receivers, should it unwind, wherever and whatever the
/// panic: a destination or a waiter line that cannot grow, a `Lane` or policy method, a waker's
/// clone. Dropped while armed, after the poll's own locks are let go:
///
/// - **The places the receive holds** are left as a dropped receive leaves them, each notification
///   passed on to the next receiver in its line. A notification held across the unwind would hold
///   its lane's line for as long as the future is kept, and wake nobody.
/// - **Where the receive spent its notifications** and its take then unwound, the chain runs, so
///   that the receivers it would have woken by taking are woken still.
///
/// So a poll that unwinds leaves the channel's lines as they were before the receive joined them.
/// A future polled again after it joins the lines afresh.
struct Unwind<'a, T, L: Lane, Y: Policy<L>> {
  arrival: &'a mut Arrival<L>,
  shared: &'a Shared<T, L, Y>,
  quota: &'a Quota<L>,
  spent: bool,
  armed: bool,
}

impl<T, L: Lane, Y: Policy<L>> Drop for Unwind<'_, T, L, Y> {
  fn drop(&mut self) {
    if !self.armed {
      return;
    }
    let mut wakes = Wakes::new();
    self.arrival.leave(self.shared, false, &mut wakes);
    wakes.finish();
    if self.spent {
      self.shared.chain(self.quota);
    }
  }
}

/// The room of the items one receive took, given back when it is dropped: after the choice lock.
///
/// Every unit is given back, and the wakers of the reserves the units are granted to are kept in
/// the caller's [`Staged`], each in a place made before its item was taken, so keeping it cannot
/// fail. None runs here: the caller wakes them once the receive has left its lines and chained, or,
/// on an unwind, the `Staged` does as it is dropped, after the receive's guard.
struct RoomBack<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  units: PerLane<usize, L>,
  granted: &'a mut Staged,
}

impl<T, L: Lane, Y: Policy<L>> Drop for RoomBack<'_, T, L, Y> {
  fn drop(&mut self) {
    for (index, units) in self.units.as_slice().iter().enumerate() {
      for _ in 0..*units {
        self.granted.keep(self.shared.lane_at(index).release());
      }
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> Receiver<T, L, Y> {
  #[inline]
  pub(crate) fn new(shared: Arc<Shared<T, L, Y>>) -> Self {
    Self { shared }
  }

  /// Waits for at least one item the quota admits, then appends a batch to `into`: the urgent
  /// lanes first, never waiting to fill it. The answer is how many it appended.
  ///
  /// A number converts into a quota of that many items of any lane.
  ///
  /// ```
  /// use fairlane::RecvManyError;
  ///
  /// async fn drain(rx: fairlane::Receiver<u64>) {
  ///   let mut batch = Vec::new();
  ///   loop {
  ///     match rx.recv_many(&mut batch, 32).await {
  ///       Ok(n) => batch.truncate(batch.len() - n.get()),
  ///       Err(RecvManyError::Ended) => break,
  ///       Err(RecvManyError::EmptyQuota) => unreachable!("32 admits every lane"),
  ///     }
  ///   }
  /// }
  /// ```
  ///
  /// # Errors
  ///
  /// [`RecvManyError::Ended`] once every lane the quota admits is closed and empty, and
  /// [`RecvManyError::EmptyQuota`] at once for a quota that admits no lane.
  ///
  /// # Polling when woken
  ///
  /// A receive that is woken must be polled promptly, or dropped (the crate's documentation,
  /// "Wakers, policies and items"). One left unpolled keeps its notice, and can delay other
  /// receives of the same lanes until it is polled or dropped; it loses no item and no unit.
  ///
  /// # Panics
  ///
  /// Where the policy panics, or, against its contract (the crate's documentation, "Wakers,
  /// policies and items"), the waker of another task that this receive wakes, the panic unwinds out
  /// of this call. Every item taken before it is already in `into`, and stays there for a caller
  /// that catches the unwind; no other item, and no room, is lost.
  #[inline]
  pub fn recv_many<'a>(
    &'a self,
    into: &'a mut Vec<Received<T, L>>,
    quota: impl Into<Quota<L>>,
  ) -> RecvMany<'a, T, L, Y> {
    RecvMany {
      shared: &self.shared,
      into,
      quota: quota.into(),
      wait: Arrival::new(),
    }
  }

  /// A batch, as [`recv_many`](Self::recv_many) takes it, without waiting: `Ok(0)` where nothing
  /// the quota admits is queued now.
  ///
  /// # Panics
  ///
  /// As [`recv_many`](Self::recv_many): the items taken before a panic stay in `into`.
  pub fn try_recv_many(
    &self,
    into: &mut Vec<Received<T, L>>,
    quota: impl Into<Quota<L>>,
  ) -> Result<usize, RecvManyError> {
    let quota = quota.into();
    if !quota.admits_any() {
      return Err(RecvManyError::EmptyQuota);
    }
    let mut granted = Staged::new();
    match self.shared.take(&quota, into, &mut granted) {
      Took::Items(held) => {
        self.shared.chain(&quota);
        granted.wake();
        Ok(held.get())
      }
      Took::Nothing => Ok(0),
      Took::Ended => Err(RecvManyError::Ended),
    }
  }

  /// [`recv_many`](Self::recv_many), parking this thread while it waits. It polls as soon as it is
  /// woken, so it keeps the receive's contract by itself.
  ///
  /// # Panics
  ///
  /// As [`recv_many`](Self::recv_many): the items taken before a panic stay in `into`.
  pub fn recv_many_blocking(
    &self,
    into: &mut Vec<Received<T, L>>,
    quota: impl Into<Quota<L>>,
  ) -> Result<NonZeroUsize, RecvManyError> {
    let quota = quota.into();
    match self.try_recv_many(into, &quota)? {
      0 => park_on(self.recv_many(into, quota)),
      held => NonZeroUsize::new(held).ok_or(RecvManyError::Ended),
    }
  }

  /// Waits for one item of any lane.
  ///
  /// ```
  /// # use core::num::NonZeroUsize;
  /// use fairlane::{Ended, Single};
  ///
  /// # let (tx, rx) = fairlane::bounded::<u8>(NonZeroUsize::new(4).unwrap())?;
  /// # tx.send_blocking(Single, 1)?;
  /// # drop(tx);
  /// loop {
  ///   match rx.recv_blocking() {
  ///     Ok(received) => assert_eq!(received.into_item(), 1),
  ///     Err(Ended) => break,
  ///   }
  /// }
  /// # Ok::<(), Box<dyn core::error::Error>>(())
  /// ```
  ///
  /// # Panics
  ///
  /// - A `Waker` must not panic, block on the channel or wait for another party's progress
  ///   through it, or call into a channel (the crate's documentation, "Wakers, policies and
  ///   items"): a channel has no executor, and every wake runs on the caller's thread. If one this
  ///   receive wakes panics anyway, after the item was taken, the panic unwinds out of this call
  ///   and the item is dropped with it; no room is lost, and every other wake still runs.
  ///   [`recv_many`](Self::recv_many) keeps such an item in its `into`.
  /// - Where the policy panics, the panic unwinds out of this call before any item is taken.
  ///
  /// # Polling when woken
  ///
  /// As [`recv_many`](Self::recv_many): a receive that is woken must be polled promptly, or
  /// dropped. One left unpolled can delay the others, and loses no item and no unit.
  #[inline]
  pub fn recv(&self) -> Recv<'_, T, L, Y> {
    Recv {
      shared: &self.shared,
      quota: Quota::up_to(1),
      wait: Arrival::new(),
    }
  }

  /// One item of any lane, where one is queued now.
  ///
  /// # Panics
  ///
  /// As [`recv`](Self::recv).
  pub fn try_recv(&self) -> Result<Option<Received<T, L>>, Ended> {
    let quota = Quota::up_to(1);
    let mut slot = None;
    let mut granted = Staged::new();
    match self.shared.take(&quota, &mut slot, &mut granted) {
      Took::Items(_) => {
        self.shared.chain(&quota);
        granted.wake();
        Ok(slot)
      }
      Took::Nothing => Ok(None),
      Took::Ended => Err(Ended),
    }
  }

  /// [`recv`](Self::recv), parking this thread while it waits. It polls as soon as it is woken, so
  /// it keeps the receive's contract by itself.
  ///
  /// # Panics
  ///
  /// As [`recv`](Self::recv).
  pub fn recv_blocking(&self) -> Result<Received<T, L>, Ended> {
    match self.try_recv()? {
      Some(received) => Ok(received),
      None => park_on(self.recv()),
    }
  }

  /// Closes the channel, as [`Sender::close`](crate::Sender::close) does. What is queued can
  /// still be received.
  #[inline]
  pub fn close(&self) -> bool {
    self.shared.close()
  }

  /// Whether the channel is closed.
  #[inline]
  pub fn is_closed(&self) -> bool {
    self.shared.is_closed()
  }

  /// The channel's policy. Its numbers are live: the next choice reads them.
  #[inline]
  pub fn policy(&self) -> &Y {
    self.shared.policy()
  }

  /// A reading of the channel's queues.
  #[inline]
  pub fn snapshot(&self) -> Snapshot<L> {
    self.shared.snapshot()
  }
}

impl<T, L: Lane, Y: Policy<L>> Clone for Receiver<T, L, Y> {
  #[inline]
  fn clone(&self) -> Self {
    self.shared.add_receiver();
    Self {
      shared: self.shared.clone(),
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for Receiver<T, L, Y> {
  /// The last one closes the channel and drops what is queued, as a `Vec` drops its items: if a
  /// destructor panics, the rest are still dropped, and their room given back, while it unwinds,
  /// and a second panic aborts the process.
  #[inline]
  fn drop(&mut self) {
    self.shared.drop_receiver();
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for Receiver<T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Receiver")
      .field("closed", &self.is_closed())
      .finish_non_exhaustive()
  }
}

/// A receive's wait: its place in the line of each lane its quota admits.
///
/// - **To sleep** it joins the line of each lane, then looks again. Each place goes into the
///   future the moment it exists, so that a panic on a later lane, a waker clone's or a line's that
///   cannot grow, leaves the earlier places to the poll's [`Unwind`].
/// - **On waking** it leaves every line, spending its notifications, and takes.
/// - **Wherever its poll unwinds,** the [`Unwind`] leaves every line the receive is in, passing its
///   notifications on, and runs the chain where the receive had spent them and its take unwound. A
///   future polled again after joins every line afresh, before it can wait.
/// - **Dropped while it waits,** it leaves every line and gives again the notice it held. Every
///   line is left first, each under its own lock; then the wakers it let go run, each under its
///   own catch. So a waker that panics leaves no place behind, and the notice of a place left
///   behind would be held, by a receive no one polls, for ever.
pub(crate) struct Arrival<L: Lane> {
  /// Its place in each lane it sleeps on.
  keys: PerLane<Option<Key>, L>,
  /// Its notification identity while it is in the lines, shared by its places.
  receive: Option<Arc<Receive>>,
  /// Whether it may hold a place: it began to join the lines.
  asleep: bool,
  /// Whether it is in the line of every lane its quota admits.
  registered: bool,
}

impl<L: Lane> Arrival<L> {
  fn new() -> Self {
    Self {
      keys: PerLane::from_array(Array::from_fn(|_| None)),
      receive: None,
      asleep: false,
      registered: false,
    }
  }

  /// Joins the line of every lane `quota` admits that it is not in yet.
  fn sleep<T, Y: Policy<L>>(&mut self, shared: &Shared<T, L, Y>, quota: &Quota<L>, waker: &Waker) {
    // A fresh identity each time it joins the lines, made before its first place.
    let receive = self
      .receive
      .get_or_insert_with(|| Arc::new(Receive::new()))
      .clone();
    self.asleep = true;
    for (index, key) in self.keys.as_mut_slice().iter_mut().enumerate() {
      if quota.admits_at(index) && key.is_none() {
        let fresh = waker.clone();
        *key = Some(
          shared
            .lane_at(index)
            .arrivals
            .listen(fresh, receive.clone()),
        );
      }
    }
    self.registered = true;
  }

  /// Whether a lane's notification has come.
  fn woken<T, Y: Policy<L>>(
    &self,
    shared: &Shared<T, L, Y>,
    waker: &Waker,
    wakes: &mut Wakes,
  ) -> bool {
    self.keys.as_slice().iter().enumerate().any(|(index, key)| {
      key
        .as_ref()
        .is_some_and(|key| shared.lane_at(index).arrivals.woken(key, waker, wakes))
    })
  }

  /// Leaves every line: spending the notifications it holds (`spend`), or passing them on.
  ///
  /// Every line is left before any waker it lets go runs: a waker that blocks then holds up no
  /// lane of this receive's, and a notification can no longer come to a place it is leaving.
  fn leave<T, Y: Policy<L>>(&mut self, shared: &Shared<T, L, Y>, spend: bool, wakes: &mut Wakes) {
    if !self.asleep {
      return;
    }
    self.asleep = false;
    self.registered = false;
    // First the identity, so that no line gives the receive a notice while it leaves.
    let held = self.receive.take().and_then(|receive| receive.leave());
    let mut stored: PerLane<Option<Waker>, L> = PerLane::from_array(Array::from_fn(|_| None));
    for (index, key) in self.keys.as_mut_slice().iter_mut().enumerate() {
      if let Some(key) = key.take() {
        stored.as_mut_slice()[index] = shared.lane_at(index).arrivals.leave(key);
      }
    }
    // A notice not spent is given again in its lane, once every line is left.
    let passed = held.filter(|_| !spend).and_then(|lane| shared.notify(lane));
    for waker in stored.as_mut_slice() {
      wakes.discard(waker.take());
    }
    wakes.wake(passed);
  }

  /// Polls one receive to its end.
  fn poll<T, Y: Policy<L>>(
    &mut self,
    shared: &Shared<T, L, Y>,
    quota: &Quota<L>,
    cx: &mut Context<'_>,
    sink: &mut impl Sink<T, L>,
  ) -> Poll<Result<NonZeroUsize, RecvManyError>> {
    if !quota.admits_any() {
      return Poll::Ready(Err(RecvManyError::EmptyQuota));
    }
    // Declared first, so that on an unwind it is dropped last: the granted reserves are woken once
    // the guard below has left the lines and chained.
    let mut granted = Staged::new();
    let mut wakes = Wakes::new();
    // Declared after `wakes`, so that an unwind leaves the lines before the wakers `wakes` holds
    // are dropped.
    let mut unwind = Unwind {
      arrival: self,
      shared,
      quota,
      spent: false,
      armed: true,
    };
    let polled = loop {
      let took = shared.take(quota, sink, &mut granted);
      unwind.spent = false;
      let arrival = &mut *unwind.arrival;
      match took {
        Took::Items(held) => {
          arrival.leave(shared, true, &mut wakes);
          shared.chain(quota);
          break Poll::Ready(Ok(held));
        }
        Took::Ended => {
          arrival.leave(shared, true, &mut wakes);
          break Poll::Ready(Err(RecvManyError::Ended));
        }
        Took::Nothing => {}
      }
      if !arrival.registered {
        arrival.sleep(shared, quota, cx.waker());
        continue;
      }
      if !arrival.woken(shared, cx.waker(), &mut wakes) {
        break Poll::Pending;
      }
      arrival.leave(shared, true, &mut wakes);
      unwind.spent = true;
    };
    unwind.armed = false;
    drop(unwind);
    // After the receive has left its lines and chained.
    granted.wake();
    wakes.finish();
    polled
  }

  /// The receive is dropped: it leaves every line, passing on what it was notified of.
  fn drop_wait<T, Y: Policy<L>>(&mut self, shared: &Shared<T, L, Y>) {
    let mut wakes = Wakes::new();
    self.leave(shared, false, &mut wakes);
    wakes.finish();
  }
}

/// The future of [`Receiver::recv_many`].
#[must_use = "a future does nothing unless polled"]
pub struct RecvMany<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  into: &'a mut Vec<Received<T, L>>,
  quota: Quota<L>,
  wait: Arrival<L>,
}

/// No field is pinned: the future moves items out, never keeps one in place.
impl<T, L: Lane, Y: Policy<L>> Unpin for RecvMany<'_, T, L, Y> {}

impl<T, L: Lane, Y: Policy<L>> Future for RecvMany<'_, T, L, Y> {
  type Output = Result<NonZeroUsize, RecvManyError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    this
      .wait
      .poll(this.shared, &this.quota, cx, &mut *this.into)
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for RecvMany<'_, T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    self.wait.drop_wait(self.shared);
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for RecvMany<'_, T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("RecvMany")
      .field("quota", &self.quota)
      .finish_non_exhaustive()
  }
}

/// The future of [`Receiver::recv`].
#[must_use = "a future does nothing unless polled"]
pub struct Recv<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  quota: Quota<L>,
  wait: Arrival<L>,
}

/// No field is pinned.
impl<T, L: Lane, Y: Policy<L>> Unpin for Recv<'_, T, L, Y> {}

impl<T, L: Lane, Y: Policy<L>> Future for Recv<'_, T, L, Y> {
  type Output = Result<Received<T, L>, Ended>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    let mut slot = None;
    match this.wait.poll(this.shared, &this.quota, cx, &mut slot) {
      Poll::Pending => Poll::Pending,
      Poll::Ready(Ok(_)) => match slot {
        Some(received) => Poll::Ready(Ok(received)),
        None => Poll::Pending,
      },
      Poll::Ready(Err(_)) => Poll::Ready(Err(Ended)),
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for Recv<'_, T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    self.wait.drop_wait(self.shared);
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for Recv<'_, T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Recv").finish_non_exhaustive()
  }
}
