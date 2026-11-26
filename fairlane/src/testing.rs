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

pub(crate) use faulty::Faults;

/// A waker whose `clone`, `wake` and `drop` panic while a switch is on: the user code a waker is,
/// at its worst. `std::task::Wake` gives no hook on `clone`, so the waker is built on a raw vtable.
#[allow(unsafe_code)]
mod faulty {
  use std::{
    sync::{
      Arc, Mutex,
      atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    },
    task::{RawWaker, RawWakerVTable, Waker},
  };

  /// The switches and the count of wakes behind one faulty waker and all its clones.
  #[derive(Default)]
  pub(crate) struct Faults {
    pub(crate) clone_panics: AtomicBool,
    pub(crate) wake_panics: AtomicBool,
    pub(crate) drop_panics: AtomicBool,
    pub(crate) wakes: AtomicUsize,
    /// Run once, by the next clone: user code that does what it likes inside a waker's `clone`.
    #[allow(clippy::type_complexity)]
    pub(crate) on_clone: Mutex<Option<Box<dyn FnOnce() + Send>>>,
  }

  impl Faults {
    /// A waker over these switches.
    pub(crate) fn waker(self: &Arc<Self>) -> Waker {
      let data = Arc::into_raw(self.clone());
      // SAFETY: every function of `VTABLE` takes `data` as an `Arc<Faults>` made by
      // `Arc::into_raw`, each waker holding one strong count: a clone adds one, and a wake or a
      // drop releases its own.
      unsafe { Waker::from_raw(raw(data)) }
    }

    /// Whether the waker was woken since the last call.
    pub(crate) fn take(&self) -> bool {
      self.wakes.swap(0, SeqCst) > 0
    }
  }

  const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

  fn raw(data: *const Faults) -> RawWaker {
    RawWaker::new(data.cast(), &VTABLE)
  }

  unsafe fn clone(data: *const ()) -> RawWaker {
    let faults = data.cast::<Faults>();
    // SAFETY: `data` is a live `Arc<Faults>` of this vtable, borrowed.
    let hook = unsafe { (*faults).on_clone.lock().unwrap().take() };
    if let Some(hook) = hook {
      hook();
    }
    // SAFETY: as above.
    if unsafe { (*faults).clone_panics.load(SeqCst) } {
      panic!("a waker's clone panics");
    }
    // SAFETY: as above; the new waker owns the count added here.
    unsafe { Arc::increment_strong_count(faults) };
    raw(faults)
  }

  unsafe fn wake(data: *const ()) {
    // SAFETY: this waker's own count, released here.
    let faults = unsafe { Arc::from_raw(data.cast::<Faults>()) };
    faults.wakes.fetch_add(1, SeqCst);
    let panics = faults.wake_panics.load(SeqCst);
    core::mem::drop(faults);
    if panics {
      panic!("a waker's wake panics");
    }
  }

  unsafe fn wake_by_ref(data: *const ()) {
    // SAFETY: a live `Arc<Faults>` of this vtable, borrowed.
    let faults = unsafe { &*data.cast::<Faults>() };
    faults.wakes.fetch_add(1, SeqCst);
    if faults.wake_panics.load(SeqCst) {
      panic!("a waker's wake panics");
    }
  }

  unsafe fn drop(data: *const ()) {
    // SAFETY: this waker's own count, released here.
    let faults = unsafe { Arc::from_raw(data.cast::<Faults>()) };
    let panics = faults.drop_panics.load(SeqCst);
    core::mem::drop(faults);
    if panics {
      panic!("a waker's drop panics");
    }
  }
}
