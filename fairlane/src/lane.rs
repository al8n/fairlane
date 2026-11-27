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
/// - every method is pure and never panics. A policy may call them while the channel's choice
///   lock is held, and the [`Policy`](crate::Policy) contract applies to them there.
///
/// A door resolves its lane's index once, so a `Lane` that breaks the contract can make the
/// channel answer wrongly, but cannot make it write to a lane it holds no room in.
///
/// [`channel`](crate::channel) checks the round trip and refuses a vocabulary that breaks it with
/// [`Malformed::Indices`](crate::Malformed::Indices).
pub trait Lane: Copy + Eq + fmt::Debug + Send + Sync + 'static {
  /// How many lanes there are, as a type: from 1 to 256. The derive writes it; a hand-written
  /// implementation names a `typenum` constant, such as `typenum::U2`.
  ///
  /// A channel holds at most 256 lanes: every receive, and every snapshot, keeps a reading per
  /// lane on the stack, and 256 keeps those small. The derive refuses more at compile time, and
  /// [`channel`](crate::channel) refuses a hand-written vocabulary of more with
  /// [`Malformed::TooManyLanes`](crate::Malformed::TooManyLanes).
  type N: ArraySize;

  /// The lane's place in the order: 0 is served first.
  fn index(self) -> usize;

  /// The lane at `index`, or `None` from `N` on.
  fn from_index(index: usize) -> Option<Self>;

  /// The lane's name: a label, and the key a configuration file uses for it. Each lane's is its
  /// own: a table of a vocabulary with two lanes of one name is refused when it is serialized.
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

/// One value for each lane of `L`.
///
/// It is the public shape of every per-lane table, so no `typenum` type appears in a signature
/// that uses it.
pub struct PerLane<T, L: Lane> {
  values: Array<T, L::N>,
  _lane: PhantomData<L>,
}

impl<T, L: Lane> PerLane<T, L> {
  /// A value for every lane, from `f`.
  ///
  /// The derive's users build one with an exhaustive `match` over the lane, so a lane that was
  /// forgotten is a compile error.
  ///
  /// # Panics
  ///
  /// Only for a hand-written [`Lane`] whose `from_index` is not `Some` for an index below `N`,
  /// which breaks the trait's contract.
  pub fn from_fn(mut f: impl FnMut(L) -> T) -> Self {
    Self::from_array(Array::from_fn(|index| match L::from_index(index) {
      Some(lane) => f(lane),
      None => broken_lane::<L>(index),
    }))
  }

  /// The value of `lane`.
  ///
  /// # Panics
  ///
  /// Only for a hand-written [`Lane`] whose `index` is not below `N`.
  #[inline]
  pub fn get(&self, lane: L) -> &T {
    &self.values[lane.index()]
  }

  /// The value of `lane`, mutably.
  ///
  /// # Panics
  ///
  /// As [`get`](Self::get).
  #[inline]
  pub fn get_mut(&mut self, lane: L) -> &mut T {
    &mut self.values[lane.index()]
  }

  /// Every lane with its value, in the lanes' order.
  pub fn iter(&self) -> impl Iterator<Item = (L, &T)> + '_ {
    self
      .values
      .iter()
      .enumerate()
      .filter_map(|(index, value)| L::from_index(index).map(|lane| (lane, value)))
  }

  #[inline]
  pub(crate) const fn from_array(values: Array<T, L::N>) -> Self {
    Self {
      values,
      _lane: PhantomData,
    }
  }

  /// The same value for every lane, without calling into `L`.
  #[inline]
  pub(crate) fn filled(value: T) -> Self
  where
    T: Clone,
  {
    Self::from_array(Array::from_fn(|_| value.clone()))
  }

  /// The values by index.
  #[inline]
  pub(crate) fn as_slice(&self) -> &[T] {
    self.values.as_slice()
  }

  /// The value at `index`.
  #[inline]
  pub(crate) fn at(&self, index: usize) -> &T {
    &self.values[index]
  }

  /// The values by index, mutably.
  #[inline]
  pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
    self.values.as_mut_slice()
  }
}

/// How many lanes `L` has.
#[inline]
pub(crate) const fn lane_count<L: Lane>() -> usize {
  <L::N as Unsigned>::USIZE
}

/// The lane at `index`, which is below `L::N`.
///
/// # Panics
///
/// Only for a hand-written [`Lane`] whose `from_index` is not `Some` there.
#[inline]
pub(crate) fn lane_at<L: Lane>(index: usize) -> L {
  L::from_index(index).unwrap_or_else(|| broken_lane::<L>(index))
}

/// `lane`'s index, checked against `L::N`: the one call into `Lane::index` a door makes, so that
/// everything the door does after it names one lane.
///
/// # Panics
///
/// Only for a hand-written [`Lane`] whose `index` is not below `N`.
#[inline]
pub(crate) fn index_of<L: Lane>(lane: L) -> usize {
  let index = lane.index();
  if index < lane_count::<L>() {
    index
  } else {
    broken_index::<L>(index)
  }
}

#[cold]
#[inline(never)]
fn broken_lane<L: Lane>(index: usize) -> ! {
  panic!(
    "`{}::from_index({index})` is `None` below its `Lane::N`, which breaks `Lane`'s contract",
    core::any::type_name::<L>()
  )
}

#[cold]
#[inline(never)]
fn broken_index<L: Lane>(index: usize) -> ! {
  panic!(
    "`{}::index()` answered {index}, not below its `Lane::N` ({}), which breaks `Lane`'s contract",
    core::any::type_name::<L>(),
    lane_count::<L>()
  )
}

impl<T: Clone, L: Lane> Clone for PerLane<T, L> {
  #[inline]
  fn clone(&self) -> Self {
    Self::from_array(self.values.clone())
  }
}

impl<T: Default, L: Lane> Default for PerLane<T, L> {
  #[inline]
  fn default() -> Self {
    Self::from_array(Array::default())
  }
}

impl<T: PartialEq, L: Lane> PartialEq for PerLane<T, L> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.values == other.values
  }
}

impl<T: Eq, L: Lane> Eq for PerLane<T, L> {}

impl<T: core::hash::Hash, L: Lane> core::hash::Hash for PerLane<T, L> {
  #[inline]
  fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
    self.values.hash(state);
  }
}

impl<T: fmt::Debug, L: Lane> fmt::Debug for PerLane<T, L> {
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
