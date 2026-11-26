use core::fmt;

use crate::lane::{Lane, PerLane};

/// A reading of a channel's queues, lane by lane.
///
/// - [`committed`](Self::committed), [`received`](Self::received),
///   [`remaining`](Self::remaining), [`senders_waiting`](Self::senders_waiting) and
///   [`receivers_waiting`](Self::receivers_waiting) are each one reading, exact at the instant it
///   was taken. `committed` and `received` never go backward from one snapshot to the next.
/// - [`queued`](Self::queued) and [`reserved`](Self::reserved) are derived from several of those
///   readings, taken one after another while the lane may move, so each is an estimate: between 0
///   and the lane's capacity, and exact while the lane is still.
/// - The lanes are read one after another too, so a snapshot is not one instant of the channel.
#[derive(Clone, PartialEq, Eq)]
pub struct Snapshot<L: Lane> {
  lanes: PerLane<Reading, L>,
  closed: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Reading {
  pub(crate) queued: usize,
  pub(crate) reserved: usize,
  pub(crate) remaining: usize,
  pub(crate) senders_waiting: usize,
  pub(crate) receivers_waiting: usize,
  pub(crate) committed: u64,
  pub(crate) received: u64,
}

impl<L: Lane> Snapshot<L> {
  #[inline]
  pub(crate) fn new(lanes: PerLane<Reading, L>, closed: bool) -> Self {
    Self { lanes, closed }
  }

  /// Items committed to `lane` and not yet received: `committed` less `received`, at most the
  /// lane's capacity. Derived.
  #[inline]
  pub fn queued(&self, lane: L) -> usize {
    self.lanes.get(lane).queued
  }

  /// Room held in `lane` by permits, and by reserves granted and not yet resumed: its capacity,
  /// less what remains, less what is queued. Derived.
  #[inline]
  pub fn reserved(&self, lane: L) -> usize {
    self.lanes.get(lane).reserved
  }

  /// `lane`'s remaining capacity: what a reserve could take now.
  #[inline]
  pub fn remaining(&self, lane: L) -> usize {
    self.lanes.get(lane).remaining
  }

  /// Reserves of `lane` that wait.
  #[inline]
  pub fn senders_waiting(&self, lane: L) -> usize {
    self.lanes.get(lane).senders_waiting
  }

  /// Receives that wait, counted on each lane they admit.
  #[inline]
  pub fn receivers_waiting(&self, lane: L) -> usize {
    self.lanes.get(lane).receivers_waiting
  }

  /// Every item ever committed to `lane`: the next item's ordinal.
  #[inline]
  pub fn committed(&self, lane: L) -> u64 {
    self.lanes.get(lane).committed
  }

  /// Every item ever taken out of `lane`, received or let go at a disconnect.
  #[inline]
  pub fn received(&self, lane: L) -> u64 {
    self.lanes.get(lane).received
  }

  /// Whether the channel is closed: nothing more is taken in.
  #[inline]
  pub fn is_closed(&self) -> bool {
    self.closed
  }
}

impl<L: Lane> fmt::Debug for Snapshot<L> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Snapshot")
      .field("lanes", &self.lanes)
      .field("closed", &self.closed)
      .finish()
  }
}
