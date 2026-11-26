use either::Either;

use super::{LaneView, Policy};
use crate::lane::Lane;

/// One policy of two, chosen at run time: from a deployment's configuration, for example. Unlike
/// [`FirstOf`](super::FirstOf), only the chosen side applies.
impl<P: Lane, A: Policy<P>, B: Policy<P>> Policy<P> for Either<A, B> {
  type Stamp = Either<A::Stamp, B::Stamp>;

  #[inline]
  fn stamp(&self) -> Self::Stamp {
    match self {
      Either::Left(left) => Either::Left(left.stamp()),
      Either::Right(right) => Either::Right(right.stamp()),
    }
  }

  /// A stamp of the other side cannot come from this policy, whose side is fixed for its life; one
  /// would be read as `now`, which is never owed.
  fn owed(&self, view: &LaneView<P, Self::Stamp>) -> Option<P> {
    match self {
      Either::Left(left) => {
        let now = view.now().left().unwrap_or_else(|| left.stamp());
        left.owed(&view.map(|stamp| stamp.left().unwrap_or(now)))
      }
      Either::Right(right) => {
        let now = view.now().right().unwrap_or_else(|| right.stamp());
        right.owed(&view.map(|stamp| stamp.right().unwrap_or(now)))
      }
    }
  }
}
