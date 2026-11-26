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
