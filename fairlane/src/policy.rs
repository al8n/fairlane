use core::{
  fmt,
  num::NonZeroU32,
  sync::atomic::{AtomicU32, AtomicU64, Ordering},
  time::Duration,
};

use hybrid_array::Array;

use crate::{
  clock::{Clock, Moment, Monotonic, saturating_nanos},
  lane::{Lane, PerLane},
};

/// The default [`PassLimit`]: a lane is owed once it has been passed over 8 times.
pub const DEFAULT_PASS_LIMIT: NonZeroU32 = NonZeroU32::MIN.saturating_add(7);

/// The default [`Elapsed`]: a lane is owed once its oldest item has waited 2 seconds.
pub const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(2);

/// Says which waiting lane is owed service, against starvation.
///
/// The channel serves its lanes in priority order, lane 0 first, unless the policy says a lane is
/// owed. Express lanes are outside the policy: they never appear in its view.
///
/// # Contract
///
/// `stamp` and `owed`, and the clock they read, are pure and never panic. They must not block, and
/// must not call any channel operation, on any channel, from this thread or any other.
///
/// - `owed`, and the `stamp` a receive takes, run while the channel holds its choice lock; the
///   `stamp` of a send runs while the send holds room it has not committed.
/// - A call into a channel there may deadlock, as any callback run under a lock may: a receive of
///   this channel waits for the lock its own thread holds, a reserve that must wait waits for a
///   receive, and two channels whose policies receive from each other wait for each other.
/// - Where it is cheap to tell, such a call is refused by name instead: a receive of this channel,
///   or a reserve of it that must wait, made on this thread inside its choice, panics with the
///   cause. The rest are not checked, and are as forbidden.
///
/// A policy that breaks this contract by panicking loses no room: the receive or the send unwinds.
/// What a receive had taken is the caller's; the item a send was given by value may be dropped by
/// the unwind, after its room is given back.
///
/// The lanes of one view are read at different instants, one after another, so a view is not one
/// atomic reading of the channel.
pub trait Policy<L: Lane>: Send + Sync + 'static {
  /// What is recorded with each item at its commit. `()` records nothing and costs nothing.
  ///
  /// It should be small, a few words: a receive's view of the lanes holds one reading per lane,
  /// each with a stamp, on the stack.
  type Stamp: Copy + Send + 'static;

  /// A stamp for an item being committed now. The receiving side also takes one per receive, as
  /// the view's [`now`](LaneView::now).
  fn stamp(&self) -> Self::Stamp;

  /// The lane that is owed service, or `None` to serve in priority order. A lane that is not in
  /// the view is ignored.
  fn owed(&self, view: &LaneView<L, Self::Stamp>) -> Option<L>;
}

/// What a policy may read: the lanes one receive admits that are neither express nor empty.
///
/// The channel builds one for each choice. A policy's tests, and any queue that is not a channel,
/// build one by hand.
pub struct LaneView<L: Lane, S> {
  readings: PerLane<Option<Reading<S>>, L>,
  now: S,
}

#[derive(Clone, Copy)]
struct Reading<S> {
  queued: usize,
  oldest: Option<S>,
  passed_over: u64,
}

impl<L: Lane, S: Copy> LaneView<L, S> {
  /// A view with no lane yet, read at `now`.
  #[inline]
  pub fn new(now: S) -> Self {
    Self {
      readings: PerLane::filled(None),
      now,
    }
  }

  /// The same view with `lane` in it: `queued` items, the oldest stamped `oldest`, passed over
  /// `passed_over` times since its last item was taken. A lane given twice keeps the second.
  #[must_use]
  #[inline]
  pub fn with_lane(mut self, lane: L, queued: usize, oldest: Option<S>, passed_over: u64) -> Self {
    *self.readings.get_mut(lane) = Some(Reading {
      queued,
      oldest,
      passed_over,
    });
    self
  }

  /// [`with_lane`](Self::with_lane), by the lane's index: the channel builds its views without
  /// calling into `Lane`.
  #[must_use]
  #[inline]
  pub(crate) fn with_index(
    mut self,
    index: usize,
    queued: usize,
    oldest: Option<S>,
    passed_over: u64,
  ) -> Self {
    self.readings.as_mut_slice()[index] = Some(Reading {
      queued,
      oldest,
      passed_over,
    });
    self
  }

  /// The lanes in the view, by priority.
  pub fn lanes(&self) -> impl Iterator<Item = L> + '_ {
    self
      .readings
      .as_slice()
      .iter()
      .enumerate()
      .filter(|(_, reading)| reading.is_some())
      .filter_map(|(index, _)| L::from_index(index))
  }

  /// How many items `lane` holds; 0 for a lane not in the view.
  #[inline]
  pub fn queued(&self, lane: L) -> usize {
    self.reading(lane).map_or(0, |reading| reading.queued)
  }

  /// The stamp of `lane`'s oldest item, where the view has one. The channel's views have one for
  /// every lane in them; for a zero-sized stamp it is the one value there is, whether the policy is
  /// asked directly or through [`FirstOf`] or `Either`.
  #[inline]
  pub fn oldest(&self, lane: L) -> Option<S> {
    self.reading(lane).and_then(|reading| reading.oldest)
  }

  /// How many items of other lanes were taken since `lane`'s last one; 0 for a lane not in the
  /// view.
  ///
  /// A `u64`: each take moves a count by at most one, so a count reaches `u64::MAX` only after
  /// 2⁶⁴ − 1 takes while its lane waits, over five centuries at one take a nanosecond, and every
  /// threshold a [`PassLimit`] can hold, a `u32`, is passed with room to spare. The channel's count
  /// stops at `u64::MAX` rather than wrap.
  #[inline]
  pub fn passed_over(&self, lane: L) -> u64 {
    self.reading(lane).map_or(0, |reading| reading.passed_over)
  }

  /// The stamp taken for this receive.
  #[inline]
  pub fn now(&self) -> S {
    self.now
  }

  /// The same view with every stamp mapped through `f`: how a policy of two parts hands each
  /// part its own stamps.
  pub fn map<U: Copy>(&self, f: impl Fn(S) -> U) -> LaneView<L, U> {
    let readings = Array::from_fn(|index| {
      self.readings.at(index).map(|reading| Reading {
        queued: reading.queued,
        oldest: reading.oldest.map(&f),
        passed_over: reading.passed_over,
      })
    });
    LaneView {
      readings: PerLane::from_array(readings),
      now: f(self.now),
    }
  }

  /// Whether `lane` is in the view. Never panics, whatever `lane.index()` answers.
  #[inline]
  pub(crate) fn contains(&self, lane: L) -> bool {
    self.reading(lane).is_some()
  }

  #[inline]
  fn reading(&self, lane: L) -> Option<&Reading<S>> {
    self
      .readings
      .as_slice()
      .get(lane.index())
      .and_then(Option::as_ref)
  }
}

impl<L: Lane, S: Copy> Clone for LaneView<L, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      readings: self.readings.clone(),
      now: self.now,
    }
  }
}

impl<L: Lane, S: Copy + fmt::Debug> fmt::Debug for LaneView<L, S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    struct Lanes<'a, L: Lane, S>(&'a LaneView<L, S>);
    impl<L: Lane, S: Copy + fmt::Debug> fmt::Debug for Lanes<'_, L, S> {
      fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for lane in self.0.lanes() {
          map.entry(
            &lane.as_str(),
            &format_args!(
              "queued {}, oldest {:?}, passed over {}",
              self.0.queued(lane),
              self.0.oldest(lane),
              self.0.passed_over(lane)
            ),
          );
        }
        map.finish()
      }
    }
    f.debug_struct("LaneView")
      .field("now", &self.now)
      .field("lanes", &Lanes(self))
      .finish()
  }
}

/// No lane is ever owed: strict priority. Its stamp is `()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Strict;

impl<L: Lane> Policy<L> for Strict {
  type Stamp = ();

  #[inline]
  fn stamp(&self) {}

  #[inline]
  fn owed(&self, _: &LaneView<L, ()>) -> Option<L> {
    None
  }
}

/// A lane is owed once it has been passed over `limit` times. A lane with no limit may starve.
/// Its stamp is `()`.
///
/// Where several lanes are owed, the one owed longest goes first: the largest count past its
/// limit, compared in full, so a lane at a limit of `u32::MAX` passed over once more is owed
/// longer than a lane just at its own limit. A tie goes to the higher lane.
///
/// The limits are live: [`set_limit`](Self::set_limit) takes effect at the next choice.
pub struct PassLimit<L: Lane> {
  /// One limit per lane; 0 is no limit.
  limits: PerLane<AtomicU32, L>,
}

impl<L: Lane> PassLimit<L> {
  /// `limit` for every lane.
  pub fn new(limit: NonZeroU32) -> Self {
    Self {
      limits: PerLane::from_array(Array::from_fn(|_| AtomicU32::new(limit.get()))),
    }
  }

  /// The same policy with `lane`'s limit replaced; `None` lets the lane starve.
  #[must_use]
  #[inline]
  pub fn with_lane(self, lane: L, limit: Option<NonZeroU32>) -> Self {
    self.set_limit(lane, limit);
    self
  }

  /// `lane`'s limit, where it has one.
  #[inline]
  pub fn limit(&self, lane: L) -> Option<NonZeroU32> {
    NonZeroU32::new(self.limits.get(lane).load(Ordering::Relaxed))
  }

  /// Replaces `lane`'s limit, live: the next choice reads it.
  #[inline]
  pub fn set_limit(&self, lane: L, limit: Option<NonZeroU32>) {
    self
      .limits
      .get(lane)
      .store(limit.map_or(0, NonZeroU32::get), Ordering::Relaxed);
  }
}

impl<L: Lane> Default for PassLimit<L> {
  /// [`DEFAULT_PASS_LIMIT`] for every lane.
  #[inline]
  fn default() -> Self {
    Self::new(DEFAULT_PASS_LIMIT)
  }
}

impl<L: Lane> fmt::Debug for PassLimit<L> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let limits = PerLane::<Option<NonZeroU32>, L>::from_array(Array::from_fn(|index| {
      NonZeroU32::new(self.limits.at(index).load(Ordering::Relaxed))
    }));
    f.debug_struct("PassLimit")
      .field("limits", &limits)
      .finish()
  }
}

impl<L: Lane> Policy<L> for PassLimit<L> {
  type Stamp = ();

  #[inline]
  fn stamp(&self) {}

  fn owed(&self, view: &LaneView<L, ()>) -> Option<L> {
    let mut owed: Option<(L, u64)> = None;
    for lane in view.lanes() {
      let Some(limit) = self.limit(lane) else {
        continue;
      };
      let Some(excess) = view.passed_over(lane).checked_sub(u64::from(limit.get())) else {
        continue;
      };
      if owed.is_none_or(|(_, longest)| excess > longest) {
        owed = Some((lane, excess));
      }
    }
    owed.map(|(lane, _)| lane)
  }
}

/// A lane is owed once its oldest item has waited longer than `max_wait`, by the policy's own
/// clock. A lane with no `max_wait` may starve. Its stamp is the [`Moment`] of the commit.
///
/// Where several lanes are owed, the most overdue goes first. A tie goes to the higher lane.
///
/// A stamp is taken just before its commit, so a sender descheduled between the two commits an
/// item that is older than its place in the lane. The wait is a bound: once the oldest item has
/// waited longer than `max_wait`, the next receive that reads the lane serves it, or serves a lane
/// more overdue.
///
/// The waits are live: [`set_max_wait`](Self::set_max_wait) takes effect at the next choice. A
/// choice reads each wait from one atomic, without blocking, so the settings of a lane are seen in
/// the order they were made. [`max_wait`](Self::max_wait) reads the setting itself, under a lock
/// its setters share.
///
/// A wait is kept in whole nanoseconds, and reads back exactly as set up to `u64::MAX` of them,
/// about 584 years. A longer one is kept, and read back, as `u64::MAX` nanoseconds: no wait a
/// [`Moment`] can measure exceeds either, so the lane is owed exactly when it would be.
pub struct Elapsed<L: Lane, C = Monotonic> {
  max_waits: PerLane<MaxWait, L>,
  clock: C,
}

/// One lane's longest wait.
struct MaxWait {
  /// What a choice reads: the wait in nanoseconds, [`NO_WAIT`] for none. One atomic, stored only
  /// under `set`'s lock, so its stores follow the settings' order and a reader's loads keep it.
  threshold: AtomicU64,
  /// The setting as made, apart from the largest wait, for `max_wait`.
  set: std::sync::Mutex<Option<u64>>,
}

/// A lane with no wait. It schedules as a wait of `u64::MAX` nanoseconds does: no wait a
/// [`Moment`] can measure exceeds it.
const NO_WAIT: u64 = u64::MAX;

impl MaxWait {
  fn new(max_wait: Option<Duration>) -> Self {
    let nanos = max_wait.map(saturating_nanos);
    let wait = Self {
      threshold: AtomicU64::new(nanos.unwrap_or(NO_WAIT)),
      set: std::sync::Mutex::new(nanos),
    };
    // Locked once here: std's `Mutex` may allocate its native mutex at its first lock.
    drop(wait.lock());
    wait
  }

  fn lock(&self) -> std::sync::MutexGuard<'_, Option<u64>> {
    self
      .set
      .lock()
      .unwrap_or_else(std::sync::PoisonError::into_inner)
  }

  /// Replaces the setting, and publishes its threshold while the lock is held.
  fn set(&self, max_wait: Option<Duration>) {
    let nanos = max_wait.map(saturating_nanos);
    let mut set = self.lock();
    *set = nanos;
    self
      .threshold
      .store(nanos.unwrap_or(NO_WAIT), Ordering::Relaxed);
  }

  /// The setting, in nanoseconds.
  fn get(&self) -> Option<u64> {
    *self.lock()
  }

  /// The threshold a choice compares with. Never blocks.
  fn threshold(&self) -> u64 {
    self.threshold.load(Ordering::Relaxed)
  }
}

impl<L: Lane> Elapsed<L> {
  /// `max_wait` for every lane, on a [`Monotonic`] clock started now.
  #[inline]
  pub fn new(max_wait: Duration) -> Self {
    Self::with_clock(max_wait, Monotonic::new())
  }
}

impl<L: Lane, C> Elapsed<L, C> {
  /// `max_wait` for every lane, on `clock`.
  pub fn with_clock(max_wait: Duration, clock: C) -> Self {
    Self {
      max_waits: PerLane::from_array(Array::from_fn(|_| MaxWait::new(Some(max_wait)))),
      clock,
    }
  }

  /// The same policy with `lane`'s wait replaced; `None` lets the lane starve.
  #[must_use]
  #[inline]
  pub fn with_lane(self, lane: L, max_wait: Option<Duration>) -> Self {
    self.set_max_wait(lane, max_wait);
    self
  }

  /// `lane`'s longest wait, where it has one: as it was set, up to `u64::MAX` nanoseconds.
  #[inline]
  pub fn max_wait(&self, lane: L) -> Option<Duration> {
    self.max_waits.get(lane).get().map(Duration::from_nanos)
  }

  /// Replaces `lane`'s longest wait, live: the next choice reads it.
  #[inline]
  pub fn set_max_wait(&self, lane: L, max_wait: Option<Duration>) {
    self.max_waits.get(lane).set(max_wait);
  }
}

impl<L: Lane> Default for Elapsed<L> {
  /// [`DEFAULT_MAX_WAIT`] for every lane, on a [`Monotonic`] clock started now.
  #[inline]
  fn default() -> Self {
    Self::new(DEFAULT_MAX_WAIT)
  }
}

impl<L: Lane, C: fmt::Debug> fmt::Debug for Elapsed<L, C> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let waits = PerLane::<Option<Duration>, L>::from_array(Array::from_fn(|index| {
      self.max_waits.at(index).get().map(Duration::from_nanos)
    }));
    f.debug_struct("Elapsed")
      .field("max_waits", &waits)
      .field("clock", &self.clock)
      .finish()
  }
}

impl<L: Lane, C: Clock> Policy<L> for Elapsed<L, C> {
  type Stamp = Moment;

  #[inline]
  fn stamp(&self) -> Moment {
    self.clock.now()
  }

  fn owed(&self, view: &LaneView<L, Moment>) -> Option<L> {
    let now = view.now();
    let mut owed: Option<(L, u64)> = None;
    for lane in view.lanes() {
      let max_wait = self.max_waits.get(lane).threshold();
      if max_wait == NO_WAIT {
        continue;
      }
      let Some(oldest) = view.oldest(lane) else {
        continue;
      };
      let waited = saturating_nanos(now.saturating_since(oldest));
      if waited <= max_wait {
        continue;
      }
      let overdue = waited - max_wait;
      if owed.is_none_or(|(_, most)| overdue > most) {
        owed = Some((lane, overdue));
      }
    }
    owed.map(|(lane, _)| lane)
  }
}

/// Owed when either policy is: both apply, and whichever names a lane first wins. `first` is
/// asked first, so where both name a lane, `first`'s is taken.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FirstOf<A, B> {
  first: A,
  second: B,
}

impl<A, B> FirstOf<A, B> {
  /// Asks `first`, then `second`.
  #[inline]
  pub const fn new(first: A, second: B) -> Self {
    Self { first, second }
  }

  /// The policy asked first: its numbers are live through it.
  #[inline]
  pub const fn first(&self) -> &A {
    &self.first
  }

  /// The policy asked second.
  #[inline]
  pub const fn second(&self) -> &B {
    &self.second
  }
}

/// [`FirstOf`]'s stamp: one of each.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Stamps<A, B> {
  first: A,
  second: B,
}

impl<A: Copy, B: Copy> Stamps<A, B> {
  /// The stamp of the policy asked first.
  #[inline]
  pub const fn first(&self) -> A {
    self.first
  }

  /// The stamp of the policy asked second.
  #[inline]
  pub const fn second(&self) -> B {
    self.second
  }
}

impl<L: Lane, A: Policy<L>, B: Policy<L>> Policy<L> for FirstOf<A, B> {
  type Stamp = Stamps<A::Stamp, B::Stamp>;

  #[inline]
  fn stamp(&self) -> Self::Stamp {
    Stamps {
      first: self.first.stamp(),
      second: self.second.stamp(),
    }
  }

  /// A lane `first` names outside the view is ignored, as [`choose`](crate::choice::choose)
  /// ignores it, and `second` is asked instead. So is one `second` names: the answer is a lane of
  /// the view, or none.
  fn owed(&self, view: &LaneView<L, Self::Stamp>) -> Option<L> {
    let in_view = |lane: &L| view.contains(*lane);
    self
      .first
      .owed(&view.map(|stamps| stamps.first))
      .filter(in_view)
      .or_else(|| {
        self
          .second
          .owed(&view.map(|stamps| stamps.second))
          .filter(in_view)
      })
  }
}

/// The default policy, a double safeguard: a count and a time, whichever comes first.
///
/// Its `Default` is [`DEFAULT_PASS_LIMIT`] and [`DEFAULT_MAX_WAIT`] for every lane. Both numbers
/// are live through [`first`](FirstOf::first) and [`second`](FirstOf::second).
pub type DefaultPolicy<L> = FirstOf<PassLimit<L>, Elapsed<L>>;

#[cfg(feature = "either")]
mod either_impl;

#[cfg(test)]
mod tests;
