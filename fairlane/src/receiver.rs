use core::{
  fmt,
  future::Future,
  num::NonZeroUsize,
  pin::Pin,
  task::{Context, Poll},
};

use event_listener::{EventListener, Listener};
use hybrid_array::Array;

use crate::{
  channel::Shared,
  choice::{Choice, choose, count as count_choice, fits},
  error::Ended,
  lane::{Lane, PerLane, Single, lane_count},
  park::park_on,
  policy::{DefaultPolicy, LaneView, Policy},
  quota::Quota,
  snapshot::Snapshot,
  sync::{Arc, Mutex, Ordering, lock},
};

/// The receiving half of a channel. Clones share the channel: each item goes to one receive.
/// The last one dropped closes the channel and lets go of what is queued.
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

impl<T, L: Lane, Y: Policy<L>> Shared<T, L, Y> {
  /// One receive under `quota`, without waiting: every item it takes goes to `sink` at once, before
  /// the next choice, so an unwind out of a policy never drops an item that was taken.
  pub(crate) fn take(&self, quota: &Quota<L>, sink: &mut impl FnMut(Received<T, L>)) -> Took {
    match self.choice() {
      None => self.take_one_lane(quota, sink),
      Some(choice) => self.take_lanes(choice, quota, sink),
    }
  }

  /// A channel of one lane: pops, and no lock.
  fn take_one_lane(&self, quota: &Quota<L>, sink: &mut impl FnMut(Received<T, L>)) -> Took {
    let state = self.lane_at(0);
    let lane_limit = quota.limit_at(0);
    let mut held = 0;
    while fits(held, quota.limit(), lane_limit) {
      let Some(popped) = state.ring.pop() else {
        break;
      };
      state.release();
      sink(Received {
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
  /// The room of every item taken is given back after the lock is let go, even on an unwind out of
  /// the policy, so that no waker runs under the lock.
  fn take_lanes(
    &self,
    choice: &Mutex<Choice<T, L, Y::Stamp>>,
    quota: &Quota<L>,
    sink: &mut impl FnMut(Received<T, L>),
  ) -> Took {
    let count = lane_count::<L>();
    // Declared before the guard, so that it is dropped after it.
    let mut back = RoomBack {
      shared: self,
      units: PerLane::filled(0),
    };
    let mut guard = lock(choice);
    let choice = &mut *guard;
    let stamps_matter = core::mem::size_of::<Y::Stamp>() != 0;
    let mut now: Option<Y::Stamp> = None;
    let mut held = 0;
    let mut limit = quota.limit();
    loop {
      let mut waiting = PerLane::<bool, L>::filled(false);
      for index in (0..count).rev() {
        if !quota.admits_at(index) {
          continue;
        }
        let state = self.lane_at(index);
        let ahead = &mut choice.ahead.as_mut_slice()[index];
        let mut holds = ahead.is_some() || !state.ring.is_empty();
        if holds && ahead.is_none() && stamps_matter && !self.is_express_at(index) {
          // The oldest stamp is read from the item itself: pop it ahead. Only this lock's holder
          // pops, so the ring keeps the item it showed.
          *ahead = state.ring.pop();
          holds = ahead.is_some();
          if holds {
            state.ahead.store(1, Ordering::Relaxed);
          }
        }
        waiting.as_mut_slice()[index] = holds;
      }
      let express =
        (0..count).find(|&index| waiting.as_slice()[index] && self.is_express_at(index));
      let mut view = None;
      let chosen = match express {
        Some(index) => index,
        None => {
          // The view: the admitted lanes that hold an item and are not express.
          let in_view = |index: usize| waiting.as_slice()[index] && !self.is_express_at(index);
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
              let ahead = choice.ahead.as_slice()[index].as_ref();
              let queued = state
                .ring
                .committed()
                .saturating_sub(state.ring.received())
                .saturating_add(u64::from(ahead.is_some()));
              lane_view = lane_view.with_lane(
                self.name(index),
                usize::try_from(queued).unwrap_or(usize::MAX),
                ahead.and_then(|popped| popped.stamp),
                *choice.passed_over.at(index),
              );
            }
            let owed = choose(&lane_view, self.policy()).map_or(first, Lane::index);
            view = Some(lane_view);
            if owed < count && in_view(owed) {
              owed
            } else {
              first
            }
          }
        }
      };

      let lane_limit = quota.limit_at(chosen);
      if !fits(held, limit, lane_limit) {
        break;
      }
      let state = self.lane_at(chosen);
      let popped = match choice.ahead.as_mut_slice()[chosen].take() {
        Some(popped) => {
          state.ahead.store(0, Ordering::Relaxed);
          popped
        }
        None => match state.ring.pop() {
          Some(popped) => popped,
          None => break,
        },
      };
      held += 1;
      limit = limit.min(lane_limit);
      if express.is_none() {
        match &view {
          Some(view) => count_choice(&mut choice.passed_over, self.name(chosen), view),
          None => choice.passed_over.as_mut_slice()[chosen] = 0,
        }
      }
      back.units.as_mut_slice()[chosen] += 1;
      sink(Received {
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
          .all(|index| {
            choice.ahead.as_slice()[index].is_none() && self.lane_at(index).ring.is_ended()
          });
        if ended { Took::Ended } else { Took::Nothing }
      }
    }
  }

  /// After a receive that took items: where items are left in a lane the quota admits, one more
  /// waiting receiver of that lane is woken. Every admitted lane is looked at, not only the one
  /// this receive woke on.
  pub(crate) fn chain(&self, quota: &Quota<L>) {
    for index in 0..lane_count::<L>() {
      let state = self.lane_at(index);
      let holds = state.ahead.load(Ordering::Relaxed) != 0 || !state.ring.is_empty();
      if quota.admits_at(index) && holds {
        state.arrivals.wake_one();
      }
    }
  }
}

/// The room of the items one receive took, given back when it is dropped: after the choice lock.
struct RoomBack<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  units: PerLane<usize, L>,
}

impl<T, L: Lane, Y: Policy<L>> Drop for RoomBack<'_, T, L, Y> {
  fn drop(&mut self) {
    for (index, units) in self.units.as_slice().iter().enumerate() {
      for _ in 0..*units {
        self.shared.lane_at(index).release();
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
  pub fn try_recv_many(
    &self,
    into: &mut Vec<Received<T, L>>,
    quota: impl Into<Quota<L>>,
  ) -> Result<usize, RecvManyError> {
    let quota = quota.into();
    if !quota.admits_any() {
      return Err(RecvManyError::EmptyQuota);
    }
    match self
      .shared
      .take(&quota, &mut |received| into.push(received))
    {
      Took::Items(held) => {
        self.shared.chain(&quota);
        Ok(held.get())
      }
      Took::Nothing => Ok(0),
      Took::Ended => Err(RecvManyError::Ended),
    }
  }

  /// [`recv_many`](Self::recv_many), parking this thread while it waits.
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
  #[inline]
  pub fn recv(&self) -> Recv<'_, T, L, Y> {
    Recv {
      shared: &self.shared,
      quota: Quota::up_to(1),
      wait: Arrival::new(),
    }
  }

  /// One item of any lane, where one is queued now.
  pub fn try_recv(&self) -> Result<Option<Received<T, L>>, Ended> {
    let quota = Quota::up_to(1);
    let mut slot = None;
    match self
      .shared
      .take(&quota, &mut |received| slot = Some(received))
    {
      Took::Items(_) => {
        self.shared.chain(&quota);
        Ok(slot)
      }
      Took::Nothing => Ok(None),
      Took::Ended => Err(Ended),
    }
  }

  /// [`recv`](Self::recv), parking this thread while it waits.
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

/// A receive's wait: a listener on each lane its quota admits, and its place in their counts.
///
/// To sleep it listens on each lane, then counts itself on each, then looks again. On waking it
/// leaves every count, discards every listener and starts over. Dropped while it waits, it leaves
/// the counts and drops its listeners, so a notification it held goes to the next receiver.
pub(crate) struct Arrival<L: Lane> {
  /// One listener per lane it sleeps on; `None` where it does not, or where the listener is spent.
  listeners: PerLane<Option<EventListener>, L>,
  /// The lanes whose count holds it.
  counted: PerLane<bool, L>,
  asleep: bool,
}

impl<L: Lane> Arrival<L> {
  fn new() -> Self {
    Self {
      listeners: PerLane::from_array(Array::from_fn(|_| None)),
      counted: PerLane::filled(false),
      asleep: false,
    }
  }

  /// Listens, then counts, on every lane `quota` admits.
  fn sleep<T, Y: Policy<L>>(&mut self, shared: &Shared<T, L, Y>, quota: &Quota<L>) {
    for index in 0..self.counted.as_slice().len() {
      if quota.admits_at(index) {
        self.listeners.as_mut_slice()[index] = Some(shared.lane_at(index).arrivals.listen());
        self.counted.as_mut_slice()[index] = true;
      }
    }
    self.asleep = true;
  }

  /// Whether a listener has been notified. Each notified listener is spent.
  fn poll_woken(&mut self, cx: &mut Context<'_>) -> bool {
    let mut woken = false;
    for listener in self.listeners.as_mut_slice() {
      if let Some(waiting) = listener {
        if Pin::new(waiting).poll(cx).is_ready() {
          *listener = None;
          woken = true;
        }
      }
    }
    woken
  }

  /// Leaves every count, and lets every listener go: discarded when the receive wakes or ends,
  /// so that it causes no wake of its own; dropped when the receive is dropped, so that a
  /// notification it held passes to the next listener. A notification discarded here is
  /// repaired by the chain of the receive that took the items.
  fn leave<T, Y: Policy<L>>(&mut self, shared: &Shared<T, L, Y>, discard: bool) {
    if !self.asleep {
      return;
    }
    self.asleep = false;
    for index in 0..self.counted.as_slice().len() {
      if core::mem::take(&mut self.counted.as_mut_slice()[index]) {
        shared.lane_at(index).arrivals.leave();
      }
      if let Some(listener) = self.listeners.as_mut_slice()[index].take() {
        if discard {
          listener.discard();
        } else {
          drop(listener);
        }
      }
    }
  }

  /// Polls one receive to its end.
  fn poll<T, Y: Policy<L>>(
    &mut self,
    shared: &Shared<T, L, Y>,
    quota: &Quota<L>,
    cx: &mut Context<'_>,
    sink: &mut impl FnMut(Received<T, L>),
  ) -> Poll<Result<NonZeroUsize, RecvManyError>> {
    if !quota.admits_any() {
      return Poll::Ready(Err(RecvManyError::EmptyQuota));
    }
    loop {
      match shared.take(quota, sink) {
        Took::Items(held) => {
          self.leave(shared, true);
          shared.chain(quota);
          return Poll::Ready(Ok(held));
        }
        Took::Ended => {
          self.leave(shared, true);
          return Poll::Ready(Err(RecvManyError::Ended));
        }
        Took::Nothing => {}
      }
      if !self.asleep {
        self.sleep(shared, quota);
        continue;
      }
      if !self.poll_woken(cx) {
        return Poll::Pending;
      }
      self.leave(shared, true);
    }
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
    let into = &mut *this.into;
    this
      .wait
      .poll(this.shared, &this.quota, cx, &mut |received| {
        into.push(received)
      })
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for RecvMany<'_, T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    self.wait.leave(self.shared, false);
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
    match this
      .wait
      .poll(this.shared, &this.quota, cx, &mut |received| {
        slot = Some(received)
      }) {
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
    self.wait.leave(self.shared, false);
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for Recv<'_, T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Recv").finish_non_exhaustive()
  }
}
