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

/// A waker that counts its wakes: a test polls a future only once its waker was woken, as an
/// executor would.
pub(crate) struct Probe(std::sync::atomic::AtomicUsize);

impl std::task::Wake for Probe {
  fn wake(self: std::sync::Arc<Self>) {
    self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
  }

  fn wake_by_ref(self: &std::sync::Arc<Self>) {
    self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
  }
}

impl Probe {
  /// A fresh probe and its waker.
  pub(crate) fn new() -> (std::sync::Arc<Self>, std::task::Waker) {
    let probe = std::sync::Arc::new(Self(std::sync::atomic::AtomicUsize::new(0)));
    let waker = std::task::Waker::from(probe.clone());
    (probe, waker)
  }

  /// Whether the waker was woken since the last call.
  pub(crate) fn take(&self) -> bool {
    self.0.swap(0, std::sync::atomic::Ordering::SeqCst) > 0
  }
}

/// Polls `future` once with `waker`.
pub(crate) fn poll_with<F: core::future::Future + Unpin>(
  future: &mut F,
  waker: &std::task::Waker,
) -> core::task::Poll<F::Output> {
  core::pin::Pin::new(future).poll(&mut core::task::Context::from_waker(waker))
}
