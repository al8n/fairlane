//! Lane vocabularies shared by the unit tests.

use crate::lane::Lane;

/// Three lanes, none express by declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Three {
  A,
  B,
  C,
}

impl Three {
  pub(crate) const ALL: [Self; 3] = [Self::A, Self::B, Self::C];
}

impl Lane for Three {
  type N = typenum::U3;

  fn index(self) -> usize {
    self as usize
  }

  fn from_index(index: usize) -> Option<Self> {
    Self::ALL.get(index).copied()
  }

  fn as_str(self) -> &'static str {
    match self {
      Self::A => "a",
      Self::B => "b",
      Self::C => "c",
    }
  }
}

/// SplitMix64: a seeded, replayable source of test choices. A failing seed is printed and run
/// again by itself.
pub(crate) struct Rng(u64);

impl Rng {
  pub(crate) fn new(seed: u64) -> Self {
    Self(seed)
  }

  pub(crate) fn next(&mut self) -> u64 {
    self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = self.0;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
  }

  /// A number in `0..n`; `n` must not be 0.
  pub(crate) fn below(&mut self, n: usize) -> usize {
    (self.next() % n as u64) as usize
  }

  /// True one time in `n`.
  pub(crate) fn one_in(&mut self, n: usize) -> bool {
    self.below(n) == 0
  }
}
