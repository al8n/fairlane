//! The lane choice, as pure functions over a [`LaneView`].
//!
//! The channel calls them for each item it takes, under its choice lock. Any other queue that
//! serves lanes may call them too, so that one rule decides everywhere: build a view of the lanes
//! that wait, [`choose`] one, and [`count`] the choice.

use crate::{
  lane::{Lane, PerLane, lane_count},
  policy::{LaneView, Policy},
  ring::Popped,
};

/// The lane `policy` says is owed, where the view holds it; else the view's first lane, by
/// priority. `None` for a view with no lane.
///
/// The view holds the lanes that may be served now: admitted, not express, and not empty.
/// Express lanes are served before this choice is made.
#[inline]
pub fn choose<L: Lane, Y: Policy<L>>(view: &LaneView<L, Y::Stamp>, policy: &Y) -> Option<L> {
  policy
    .owed(view)
    .filter(|&lane| view.contains(lane))
    .or_else(|| view.lanes().next())
}

/// After an item of `served` was taken: `served`'s passed-over count is zero, and every other lane
/// of the view has one more. A lane outside the view keeps its count.
#[inline]
pub fn count<L: Lane, S: Copy>(
  passed_over: &mut PerLane<u32, L>,
  served: L,
  view: &LaneView<L, S>,
) {
  for lane in view.lanes() {
    if lane != served {
      let count = passed_over.get_mut(lane);
      *count = count.saturating_add(1);
    }
  }
  *passed_over.get_mut(served) = 0;
}

/// Whether a batch that holds `held` items, under an effective `limit`, may take one more of a
/// lane whose limit is `lane_limit`. The effective limit is the quota's, lowered by the limit of
/// every lane the batch already holds an item of.
#[inline]
pub(crate) fn fits(held: usize, limit: usize, lane_limit: usize) -> bool {
  held < limit.min(lane_limit)
}

/// What the receiving side's lock guards, on a channel of two lanes or more.
///
/// - **The passed-over counts**, which the lane choice and the policy read and change together:
///   two receivers that both read them unlocked could both serve one owed lane.
/// - **The lookahead:** at most one item per lane, popped from its ring so that its stamp can be
///   read, and still queued until a choice takes it. Its room is not given back before then.
///
/// While the lock is held only its holder pops, so a lane can gain items and cannot lose any.
///
/// The lookahead is on the heap, one slot per lane by index, so that the shared state holds no
/// item inline: a channel of large items is built without moving them through the stack.
pub(crate) struct Choice<T, L: Lane, S> {
  pub(crate) passed_over: PerLane<u32, L>,
  pub(crate) ahead: Box<[Option<Popped<T, S>>]>,
}

impl<T, L: Lane, S> Choice<T, L, S> {
  pub(crate) fn new() -> Self {
    Self {
      passed_over: PerLane::filled(0),
      ahead: (0..lane_count::<L>()).map(|_| None).collect(),
    }
  }
}

#[cfg(test)]
mod tests;
