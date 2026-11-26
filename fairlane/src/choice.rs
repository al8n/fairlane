//! The lane choice, as pure functions over a [`LaneView`].
//!
//! The channel calls them for each item it takes, under its choice lock. Any other queue that
//! serves lanes may call them too, so that one rule decides everywhere: build a view of the lanes
//! that wait, [`choose`] one, and [`count`] the choice.

use crate::{
  lane::{Lane, PerLane},
  policy::{LaneView, Policy},
};

/// The lane `policy` says is owed, where the view holds it; else the view's first lane, by
/// priority. `None` for a view with no lane.
///
/// The view holds the lanes that may be served now: admitted, not express, and not empty.
/// Express lanes are served before this choice is made.
#[inline]
pub fn choose<P: Lane, Y: Policy<P>>(view: &LaneView<P, Y::Stamp>, policy: &Y) -> Option<P> {
  policy
    .owed(view)
    .filter(|&lane| view.contains(lane))
    .or_else(|| view.lanes().next())
}

/// After an item of `served` was taken: `served`'s passed-over count is zero, and every other lane
/// of the view has one more. A lane outside the view keeps its count.
#[inline]
pub fn count<P: Lane, S: Copy>(
  passed_over: &mut PerLane<u32, P>,
  served: P,
  view: &LaneView<P, S>,
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
/// lane whose cap is `at_most`. The effective limit is the quota's, lowered by the cap of every
/// lane the batch already holds an item of.
#[inline]
pub(crate) fn fits(held: usize, limit: usize, at_most: usize) -> bool {
  held < limit.min(at_most)
}

#[cfg(test)]
mod tests;
