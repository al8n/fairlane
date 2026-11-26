use core::{fmt, marker::PhantomData};

use hybrid_array::{Array, ArraySize};
use typenum::Unsigned;

/// A closed vocabulary of lanes: one value of the type is one lane.
///
/// A lane is a priority class with its own queue and its own bound. Index 0 is served first.
/// `#[derive(Lane)]` (the `derive` feature) writes every item for a fieldless enum, numbering the
/// lanes in declaration order.
///
/// # Contract
///
/// A hand-written implementation must keep all of these:
///
/// - every value's [`index`](Lane::index) is below `N`, and two different values have two
///   different indices;
/// - [`from_index`](Lane::from_index) is `Some(lane)` with `lane.index() == i` for every `i`
///   below `N`, and `None` from `N` on;
/// - every method is pure and never panics. The receiving side calls them while it holds the
///   channel's choice lock.
///
/// [`channel`](crate::channel) checks the round trip and refuses a vocabulary that breaks it with
/// [`Malformed::Indices`](crate::Malformed::Indices).
pub trait Lane: Copy + Eq + fmt::Debug + Send + Sync + 'static {
  /// How many lanes there are, as a type. The derive writes it; a hand-written implementation
  /// names a `typenum` constant, such as `typenum::U2`.
  type N: ArraySize;

  /// The lane's place in the order: 0 is served first.
  fn index(self) -> usize;

  /// The lane at `index`, or `None` from `N` on.
  fn from_index(index: usize) -> Option<Self>;

  /// The lane's name: a label, and the key a configuration file uses for it.
  fn as_str(self) -> &'static str;

  /// Whether the lane is express unless its options say otherwise.
  ///
  /// An express lane is served before every other lane and outside the policy. Express lanes come
  /// first in the order.
  #[inline]
  fn is_express(self) -> bool {
    false
  }
}

/// The vocabulary of one lane: a plain bounded channel that keeps permits and batched receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Single;

impl Lane for Single {
  type N = typenum::U1;

  #[inline]
  fn index(self) -> usize {
    0
  }

  #[inline]
  fn from_index(index: usize) -> Option<Self> {
    (index == 0).then_some(Self)
  }

  #[inline]
  fn as_str(self) -> &'static str {
    "single"
  }
}

/// One value for each lane of `P`.
///
/// It is the public shape of every per-lane table, so no `typenum` type appears in a signature
/// that uses it.
pub struct PerLane<X, P: Lane> {
  values: Array<X, P::N>,
  _lane: PhantomData<P>,
}

impl<X, P: Lane> PerLane<X, P> {
  /// A value for every lane, from `f`.
  ///
  /// The derive's users build one with an exhaustive `match` over the lane, so a lane that was
  /// forgotten is a compile error.
  ///
  /// # Panics
  ///
  /// Only for a hand-written [`Lane`] whose `from_index` is not `Some` for an index below `N`,
  /// which breaks the trait's contract.
  pub fn from_fn(mut f: impl FnMut(P) -> X) -> Self {
    Self::from_array(Array::from_fn(|index| match P::from_index(index) {
      Some(lane) => f(lane),
      None => broken_lane::<P>(index),
    }))
  }

  /// The value of `lane`.
  ///
  /// # Panics
  ///
  /// Only for a hand-written [`Lane`] whose `index` is not below `N`.
  #[inline]
  pub fn get(&self, lane: P) -> &X {
    &self.values[lane.index()]
  }

  /// The value of `lane`, mutably.
  ///
  /// # Panics
  ///
  /// As [`get`](Self::get).
  #[inline]
  pub fn get_mut(&mut self, lane: P) -> &mut X {
    &mut self.values[lane.index()]
  }

  /// Every lane with its value, in the lanes' order.
  pub fn iter(&self) -> impl Iterator<Item = (P, &X)> + '_ {
    self
      .values
      .iter()
      .enumerate()
      .filter_map(|(index, value)| P::from_index(index).map(|lane| (lane, value)))
  }

  #[inline]
  pub(crate) const fn from_array(values: Array<X, P::N>) -> Self {
    Self {
      values,
      _lane: PhantomData,
    }
  }

  /// The same value for every lane, without calling into `P`.
  #[inline]
  pub(crate) fn filled(value: X) -> Self
  where
    X: Clone,
  {
    Self::from_array(Array::from_fn(|_| value.clone()))
  }

  /// The values by index.
  #[inline]
  pub(crate) fn as_slice(&self) -> &[X] {
    self.values.as_slice()
  }

  /// The value at `index`.
  #[inline]
  pub(crate) fn at(&self, index: usize) -> &X {
    &self.values[index]
  }
}

/// How many lanes `P` has.
#[inline]
pub(crate) const fn lane_count<P: Lane>() -> usize {
  <P::N as Unsigned>::USIZE
}

#[cold]
#[inline(never)]
fn broken_lane<P: Lane>(index: usize) -> ! {
  panic!(
    "`{}::from_index({index})` is `None` below its `Lane::N`, which breaks `Lane`'s contract",
    core::any::type_name::<P>()
  )
}

impl<X: Clone, P: Lane> Clone for PerLane<X, P> {
  #[inline]
  fn clone(&self) -> Self {
    Self::from_array(self.values.clone())
  }
}

impl<X: Default, P: Lane> Default for PerLane<X, P> {
  #[inline]
  fn default() -> Self {
    Self::from_array(Array::default())
  }
}

impl<X: PartialEq, P: Lane> PartialEq for PerLane<X, P> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.values == other.values
  }
}

impl<X: Eq, P: Lane> Eq for PerLane<X, P> {}

impl<X: core::hash::Hash, P: Lane> core::hash::Hash for PerLane<X, P> {
  #[inline]
  fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
    self.values.hash(state);
  }
}

impl<X: fmt::Debug, P: Lane> fmt::Debug for PerLane<X, P> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_map()
      .entries(self.iter().map(|(lane, value)| (lane.as_str(), value)))
      .finish()
  }
}

#[cfg(feature = "serde")]
mod serde_impls;

#[cfg(test)]
mod tests;
