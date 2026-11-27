//! Allocation refusal at construction: a table or a ring the constructor cannot allocate is refused
//! as `Malformed::Capacity`, without aborting. During a call fairlane allocates as std does.
//!
//! A global allocator refuses the one request it is armed for. The binary has no test harness, so
//! no other thread allocates while it is armed, and the requests are counted exactly.

use std::{
  alloc::{GlobalAlloc, Layout, System},
  num::NonZeroUsize,
  sync::atomic::{AtomicUsize, Ordering::SeqCst},
};

use fairlane::{Lane, LaneOptions, Malformed, Options, Single, Strict, channel_with};

/// The requests seen since the allocator was last armed.
static SEEN: AtomicUsize = AtomicUsize::new(0);
/// The request to refuse, counted from 0; `usize::MAX` refuses none.
static REFUSE: AtomicUsize = AtomicUsize::new(usize::MAX);

/// The system allocator, refusing the request it is armed for.
struct Refusing;

impl Refusing {
  fn refuses(&self) -> bool {
    SEEN.fetch_add(1, SeqCst) == REFUSE.load(SeqCst)
  }
}

// SAFETY: every request is the system allocator's, or refused with a null pointer, which the
// trait allows.
unsafe impl GlobalAlloc for Refusing {
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    if self.refuses() {
      return core::ptr::null_mut();
    }
    // SAFETY: the caller's contract, passed on.
    unsafe { System.alloc(layout) }
  }

  unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
    if self.refuses() {
      return core::ptr::null_mut();
    }
    // SAFETY: the caller's contract, passed on.
    unsafe { System.alloc_zeroed(layout) }
  }

  unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    if self.refuses() {
      return core::ptr::null_mut();
    }
    // SAFETY: the caller's contract, passed on.
    unsafe { System.realloc(ptr, layout, new_size) }
  }

  unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
    // SAFETY: the caller's contract, passed on.
    unsafe { System.dealloc(ptr, layout) }
  }
}

#[global_allocator]
static ALLOCATOR: Refusing = Refusing;

/// Runs `f` with request `nth` from now refused, or none for `usize::MAX`, and answers what it
/// returned and how many requests it made.
fn refusing<R>(nth: usize, f: impl FnOnce() -> R) -> (R, usize) {
  SEEN.store(0, SeqCst);
  REFUSE.store(nth, SeqCst);
  let out = f();
  REFUSE.store(usize::MAX, SeqCst);
  (out, SEEN.load(SeqCst))
}

/// Three lanes, written by hand: the binary builds with no feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Three {
  A,
  B,
  C,
}

impl Lane for Three {
  type N = typenum::U3;

  fn index(self) -> usize {
    self as usize
  }

  fn from_index(index: usize) -> Option<Self> {
    [Self::A, Self::B, Self::C].get(index).copied()
  }

  fn as_str(self) -> &'static str {
    match self {
      Self::A => "a",
      Self::B => "b",
      Self::C => "c",
    }
  }
}

/// Each table the constructor allocates, refused, answers `Malformed::Capacity` by its lane.
///
/// With `Strict`, whose stamp is `()`, no ring has stamps. The constructor's own requests come
/// first, in order: the lane states, each lane's ring, the names, and, for several lanes, the
/// lookahead. Whatever follows is std's or the channel's `Arc`, and is not refused here.
fn construction_refuses_each_table_by_lane() {
  let three = Options::<Three>::from_fn(|_| LaneOptions::new(NonZeroUsize::MIN));
  // The lane states, the rings of A, B and C, the names and the lookahead.
  let expected = [0, 0, 1, 2, 0, 0];
  for (nth, lane) in expected.into_iter().enumerate() {
    let options = three.clone();
    let (built, _) = refusing(nth, move || {
      channel_with::<u8, Three, Strict>(options, Strict).err()
    });
    assert_eq!(
      built,
      Some(Malformed::Capacity(lane)),
      "three lanes, request {nth}"
    );
  }
  let (built, requests) = refusing(usize::MAX, move || {
    channel_with::<u8, Three, Strict>(three, Strict).is_ok()
  });
  assert!(built, "unrefused, the channel is built");
  assert!(
    requests > expected.len(),
    "the `Arc` comes after the tables"
  );

  let one = Options::<Single>::from_fn(|_| LaneOptions::new(NonZeroUsize::MIN));
  // The lane states and the ring. The names of a zero-sized lane need no memory, and one lane has
  // no lookahead.
  for nth in 0..2 {
    let options = one.clone();
    let (built, _) = refusing(nth, move || {
      channel_with::<u8, Single, Strict>(options, Strict).err()
    });
    assert_eq!(
      built,
      Some(Malformed::Capacity(0)),
      "one lane, request {nth}"
    );
  }
}

fn main() {
  construction_refuses_each_table_by_lane();
  println!("alloc_refusal: every construction refusal answered, none aborted");
}
