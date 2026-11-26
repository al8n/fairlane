// The README's examples use `#[derive(Lane)]`, so it is the crate's page with `derive` on, as on
// docs.rs.
#![cfg_attr(feature = "derive", doc = include_str!("../README.md"))]
#![cfg_attr(
  not(feature = "derive"),
  doc = "A bounded multi-lane MPMC channel: a bound per lane, an anti-starvation policy, batched \
         receives, and both awaited and blocking doors. Built with the `derive` feature, as on \
         docs.rs, this page is the README, with its examples."
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![deny(unsafe_code)]
#![cfg_attr(not(test), allow(dead_code))]

#[cfg(all(loom, not(feature = "loom")))]
compile_error!("`--cfg loom` builds fairlane on loom, which needs fairlane's `loom` feature");

#[cfg(not(target_has_atomic = "64"))]
compile_error!(
  "fairlane needs 64-bit atomics: a ring position, which is an item's ordinal, is an `AtomicU64`"
);

#[cfg(feature = "test-util")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub use clock::ManualClock;
pub use clock::{Clock, Moment, Monotonic};
pub use error::{Closed, Ended, SendError, TryReserveError, TrySendError};
pub use lane::{Lane, PerLane, Single};
pub use options::{LaneOptions, Malformed, Options};
pub use policy::{
  DEFAULT_MAX_WAIT, DEFAULT_PASS_LIMIT, DefaultPolicy, Elapsed, FirstOf, LaneView, PassLimit,
  Policy, Stamps, Strict,
};
pub use quota::Quota;

/// `#[derive(Lane)]` for a fieldless enum: the lanes in declaration order, index 0 first.
///
/// ```
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, fairlane::Lane)]
/// enum Urgency {
///   #[fairlane(express)]
///   Wake,
///   Interactive,
///   #[fairlane(name = "bulk")]
///   Batch,
/// }
///
/// use fairlane::Lane as _;
/// assert_eq!(Urgency::Batch.index(), 2);
/// assert_eq!(Urgency::Batch.as_str(), "bulk");
/// assert!(Urgency::Wake.is_express());
/// ```
///
/// The words, all in one `#[fairlane(…)]` attribute:
///
/// - on a variant, `express`: the lane is express unless its options say otherwise. Express lanes
///   come first;
/// - on a variant, `name = "…"`: the lane's [`as_str`](Lane::as_str), where the snake-case name of
///   the variant will not do;
/// - on the enum, `crate = "…"`: the path to this crate, for a crate that re-exports it.
///
/// It refuses, each by name, at compile time: an item that is not an enum; generic parameters; an
/// enum with no variant; a variant with fields; a variant with a discriminant; an express lane
/// behind one that is not; two lanes of one name; an unknown word; a word given twice.
#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use fairlane_derive::Lane;

pub mod choice;

mod clock;
mod error;
mod lane;
mod options;
mod park;
mod policy;
mod quota;
mod remaining;
mod reserves;
#[allow(unsafe_code)]
mod ring;
mod sync;

#[cfg(test)]
mod model;
#[cfg(all(test, loom))]
mod models;
#[cfg(test)]
mod testing;

/// Paths the derive's output names. Not public API.
#[doc(hidden)]
pub mod __private {
  pub use typenum;
}
