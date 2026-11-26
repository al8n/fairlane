use core::{fmt, num::NonZeroUsize};

use crate::lane::{Lane, PerLane, lane_count};

/// One lane's options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LaneOptions {
  capacity: NonZeroUsize,
  express: Option<bool>,
}

impl LaneOptions {
  /// A lane that holds at most `capacity` items, queued and reserved together.
  ///
  /// The capacity has no default: it is the lane's bound, and a required decision. Whether the
  /// lane is express follows the lane's own declaration ([`Lane::is_express`]).
  #[inline]
  pub const fn new(capacity: NonZeroUsize) -> Self {
    Self {
      capacity,
      express: None,
    }
  }

  /// The lane's capacity: its queued items and its permits out, together, never exceed it.
  #[inline]
  pub const fn capacity(&self) -> NonZeroUsize {
    self.capacity
  }

  /// Whether the lane is express, where these options say so; `None` follows the lane's own
  /// declaration.
  #[inline]
  pub const fn express(&self) -> Option<bool> {
    self.express
  }

  /// The lane is express, or is not, whatever its declaration says.
  #[must_use]
  #[inline]
  pub const fn with_express(mut self, express: bool) -> Self {
    self.express = Some(express);
    self
  }
}

/// A channel's options: one [`LaneOptions`] for each lane.
///
/// It has no `Default`, because a capacity is a required decision. [`from_fn`](Self::from_fn) is
/// the one constructor, so a lane cannot silently take another lane's numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options<L: Lane> {
  lanes: PerLane<LaneOptions, L>,
}

impl<L: Lane> Options<L> {
  /// Every lane's options, from `f`.
  ///
  /// # Panics
  ///
  /// As [`PerLane::from_fn`]: only for a hand-written [`Lane`] that breaks its contract.
  #[inline]
  pub fn from_fn(f: impl FnMut(L) -> LaneOptions) -> Self {
    Self {
      lanes: PerLane::from_fn(f),
    }
  }

  /// The options of `lane`.
  #[inline]
  pub fn lane(&self, lane: L) -> &LaneOptions {
    self.lanes.get(lane)
  }

  /// The same options with `lane`'s replaced.
  #[must_use]
  #[inline]
  pub fn with_lane(mut self, lane: L, options: LaneOptions) -> Self {
    *self.lanes.get_mut(lane) = options;
    self
  }

  /// The options of lane `index`.
  #[inline]
  pub(crate) fn lane_at(&self, index: usize) -> &LaneOptions {
    self.lanes.at(index)
  }

  /// Whether lane `index` is express: its options' word, else its declaration's.
  #[inline]
  pub(crate) fn is_express_at(&self, index: usize) -> bool {
    let options = self.lanes.at(index);
    options
      .express
      .unwrap_or_else(|| L::from_index(index).is_some_and(Lane::is_express))
  }

  /// Refuses, by name, options no channel can be made from: no lane; a vocabulary whose indices
  /// do not round-trip; an express lane behind a lane that is not; a capacity above
  /// [`MAX_CAPACITY`]. A capacity the ring cannot allocate for its item type is refused where the
  /// ring is made.
  pub(crate) fn check(&self) -> Result<(), Malformed> {
    let count = lane_count::<L>();
    if count == 0 {
      return Err(Malformed::NoLanes);
    }
    for index in 0..count {
      if L::from_index(index).is_none_or(|lane| lane.index() != index) {
        return Err(Malformed::Indices(index));
      }
    }
    if L::from_index(count).is_some() {
      return Err(Malformed::Indices(count));
    }
    for index in 1..count {
      if self.is_express_at(index) && !self.is_express_at(index - 1) {
        return Err(Malformed::ExpressBehind(index));
      }
    }
    for index in 0..count {
      if self.lanes.at(index).capacity.get() > MAX_CAPACITY {
        return Err(Malformed::Capacity(index));
      }
    }
    Ok(())
  }
}

/// The largest capacity a lane can have: its remaining capacity shares one word with two flags.
pub(crate) const MAX_CAPACITY: usize = usize::MAX >> 2;

/// Options no channel can be made from, refused by name. Each payload is the index of the first
/// lane at fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Malformed {
  /// The vocabulary has no lane.
  NoLanes,
  /// `Lane::from_index` and `Lane::index` do not round-trip at this index, or `from_index` is
  /// `Some` at the lane count.
  Indices(usize),
  /// This lane is express, and the lane before it is not.
  ExpressBehind(usize),
  /// This lane's capacity cannot be held as a ring.
  Capacity(usize),
}

impl fmt::Display for Malformed {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::NoLanes => f.write_str("the lane vocabulary has no lane"),
      Self::Indices(index) => write!(
        f,
        "`Lane::from_index` and `Lane::index` do not round-trip at index {index}"
      ),
      Self::ExpressBehind(index) => {
        write!(f, "lane {index} is express behind a lane that is not")
      }
      Self::Capacity(index) => write!(f, "lane {index}'s capacity cannot be held as a ring"),
    }
  }
}

impl core::error::Error for Malformed {}

#[cfg(feature = "serde")]
mod serde_impls;

#[cfg(test)]
mod tests;
