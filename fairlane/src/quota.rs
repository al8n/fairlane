use core::ops::{Bound, RangeBounds};

use crate::lane::{Lane, PerLane, lane_count};

/// What one batched receive may take.
///
/// A quota has a limit for the whole batch, a limit per lane, and the range of lanes it admits.
/// A lane's limit bounds the whole batch, not only that lane's share of it: a batch that holds an
/// item of a lane holds at most that lane's limit in all. A batch that reaches a lane's limit
/// STOPS; it never passes the lane over to take another.
///
/// A number converts into a quota, so `rx.recv_many(&mut batch, 32)` takes up to 32 items of any
/// lane.
///
/// ```
/// # use fairlane::{Quota, Single};
/// let quota = Quota::<Single>::up_to(32).limit_with(Single, 8);
/// assert_eq!(quota.limit(), 32);
/// assert_eq!(quota.limit_for(Single), 8);
/// assert!(quota.admits(Single));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quota<L: Lane> {
  limit: usize,
  /// One limit per lane; `usize::MAX` is none beyond the batch's limit.
  limits: PerLane<usize, L>,
  /// The admitted lanes: the indices `start..end`.
  start: usize,
  end: usize,
}

impl<L: Lane> Quota<L> {
  /// Up to `limit` items in all, from every lane.
  #[inline]
  pub fn up_to(limit: usize) -> Self {
    Self {
      limit,
      limits: PerLane::filled(usize::MAX),
      start: 0,
      end: lane_count::<L>(),
    }
  }

  /// The same quota, where a batch that holds an item of `lane` holds at most `n` items in all.
  /// `n = 0` admits no item of `lane`.
  #[must_use]
  #[inline]
  pub fn limit_with(mut self, lane: L, n: usize) -> Self {
    *self.limits.get_mut(lane) = n;
    self
  }

  /// The same quota, admitting only the lanes in `range`, by their order: `..=Urgency::Interactive`
  /// admits the interactive lane and every lane before it. It replaces any earlier range.
  #[must_use]
  pub fn lanes(mut self, range: impl RangeBounds<L>) -> Self {
    self.start = match range.start_bound() {
      Bound::Included(lane) => lane.index(),
      Bound::Excluded(lane) => lane.index().saturating_add(1),
      Bound::Unbounded => 0,
    };
    self.end = match range.end_bound() {
      Bound::Included(lane) => lane.index().saturating_add(1),
      Bound::Excluded(lane) => lane.index(),
      Bound::Unbounded => lane_count::<L>(),
    }
    .min(lane_count::<L>());
    self
  }

  /// How many items one receive takes at most, in all.
  #[inline]
  pub fn limit(&self) -> usize {
    self.limit
  }

  /// How many items a batch that holds an item of `lane` holds at most: that lane's limit, within
  /// the batch's [`limit`](Self::limit).
  #[inline]
  pub fn limit_for(&self, lane: L) -> usize {
    self.limit_at(lane.index())
  }

  /// Whether a receive under this quota may take an item of `lane`: the lane is in the quota's
  /// range, and its [`limit_for`](Self::limit_for) is not zero.
  #[inline]
  pub fn admits(&self, lane: L) -> bool {
    self.admits_at(lane.index())
  }

  /// [`limit_for`](Self::limit_for), by index. An index past the lanes answers 0: it admits
  /// nothing.
  #[inline]
  pub(crate) fn limit_at(&self, index: usize) -> usize {
    self
      .limits
      .as_slice()
      .get(index)
      .map_or(0, |limit| (*limit).min(self.limit))
  }

  /// [`admits`](Self::admits), by index.
  #[inline]
  pub(crate) fn admits_at(&self, index: usize) -> bool {
    (self.start..self.end).contains(&index) && self.limit_at(index) > 0
  }

  /// Whether the quota admits any lane at all.
  #[inline]
  pub(crate) fn admits_any(&self) -> bool {
    (self.start..self.end).any(|index| self.admits_at(index))
  }
}

impl<L: Lane> From<usize> for Quota<L> {
  /// [`Quota::up_to`].
  #[inline]
  fn from(limit: usize) -> Self {
    Self::up_to(limit)
  }
}

impl<L: Lane> From<&Quota<L>> for Quota<L> {
  #[inline]
  fn from(quota: &Quota<L>) -> Self {
    quota.clone()
  }
}

#[cfg(test)]
mod tests;
