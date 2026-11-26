use core::ops::{Bound, RangeBounds};

use crate::lane::{Lane, PerLane, lane_count};

/// What one batched receive may take.
///
/// A quota has a limit for the whole batch, a cap per lane, and the range of lanes it admits.
/// A cap per lane bounds the whole batch, not only that lane's share of it: a batch that holds an
/// item of a lane holds at most that lane's cap in all. A batch that reaches a cap STOPS; it never
/// passes the lane over to take another.
///
/// A number converts into a quota, so `rx.recv_many(&mut batch, 32)` takes up to 32 items of any
/// lane.
///
/// ```
/// # use fairlane::{Quota, Single};
/// let quota = Quota::<Single>::up_to(32).at_most(Single, 8);
/// assert_eq!(quota.limit(), 32);
/// assert_eq!(quota.at_most_for(Single), 8);
/// assert!(quota.admits(Single));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quota<P: Lane> {
  limit: usize,
  /// One cap per lane; `usize::MAX` is none beyond the limit.
  at_most: PerLane<usize, P>,
  /// The admitted lanes: the indices `start..end`.
  start: usize,
  end: usize,
}

impl<P: Lane> Quota<P> {
  /// Up to `limit` items in all, from every lane.
  #[inline]
  pub fn up_to(limit: usize) -> Self {
    Self {
      limit,
      at_most: PerLane::filled(usize::MAX),
      start: 0,
      end: lane_count::<P>(),
    }
  }

  /// The same quota, where a batch that holds an item of `lane` holds at most `n` items in all.
  /// `n = 0` admits no item of `lane`.
  #[must_use]
  #[inline]
  pub fn at_most(mut self, lane: P, n: usize) -> Self {
    *self.at_most.get_mut(lane) = n;
    self
  }

  /// The same quota, admitting only the lanes in `range`, by their order: `..=Urgency::Interactive`
  /// admits the interactive lane and every lane before it. It replaces any earlier range.
  #[must_use]
  pub fn lanes(mut self, range: impl RangeBounds<P>) -> Self {
    self.start = match range.start_bound() {
      Bound::Included(lane) => lane.index(),
      Bound::Excluded(lane) => lane.index().saturating_add(1),
      Bound::Unbounded => 0,
    };
    self.end = match range.end_bound() {
      Bound::Included(lane) => lane.index().saturating_add(1),
      Bound::Excluded(lane) => lane.index(),
      Bound::Unbounded => lane_count::<P>(),
    }
    .min(lane_count::<P>());
    self
  }

  /// How many items one receive takes at most, in all.
  #[inline]
  pub fn limit(&self) -> usize {
    self.limit
  }

  /// How many items a batch that holds an item of `lane` holds at most: that lane's cap, within
  /// the limit.
  #[inline]
  pub fn at_most_for(&self, lane: P) -> usize {
    self.at_most_at(lane.index())
  }

  /// Whether a receive under this quota may take an item of `lane`: the lane is in the quota's
  /// range, and its [`at_most_for`](Self::at_most_for) is not zero.
  #[inline]
  pub fn admits(&self, lane: P) -> bool {
    self.admits_at(lane.index())
  }

  /// [`at_most_for`](Self::at_most_for), by index. An index past the lanes has no cap.
  #[inline]
  pub(crate) fn at_most_at(&self, index: usize) -> usize {
    self
      .at_most
      .as_slice()
      .get(index)
      .map_or(0, |at_most| (*at_most).min(self.limit))
  }

  /// [`admits`](Self::admits), by index.
  #[inline]
  pub(crate) fn admits_at(&self, index: usize) -> bool {
    (self.start..self.end).contains(&index) && self.at_most_at(index) > 0
  }

  /// Whether the quota admits any lane at all.
  #[inline]
  pub(crate) fn admits_any(&self) -> bool {
    (self.start..self.end).any(|index| self.admits_at(index))
  }
}

impl<P: Lane> From<usize> for Quota<P> {
  /// [`Quota::up_to`].
  #[inline]
  fn from(limit: usize) -> Self {
    Self::up_to(limit)
  }
}

impl<P: Lane> From<&Quota<P>> for Quota<P> {
  #[inline]
  fn from(quota: &Quota<P>) -> Self {
    quota.clone()
  }
}

#[cfg(test)]
mod tests;
