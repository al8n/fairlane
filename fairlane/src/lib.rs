#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![deny(unsafe_code)]
#![cfg_attr(not(test), allow(dead_code))]

#[cfg(not(target_has_atomic = "64"))]
compile_error!(
  "fairlane needs 64-bit atomics: a ring position, which is an item's ordinal, is an `AtomicU64`"
);

pub use lane::{Lane, PerLane, Single};
pub use options::{LaneOptions, Malformed, Options};

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

mod lane;
mod options;

/// Paths the derive's output names. Not public API.
#[doc(hidden)]
pub mod __private {
  pub use typenum;
}
