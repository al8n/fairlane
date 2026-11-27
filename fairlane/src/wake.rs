//! Running wakers, which are user code, so that one that panics stops nothing else.
//!
//! A waker's `wake`, `clone` and `drop` may each panic. The crate never runs one under its locks,
//! and makes the state changes a waker stands for before it runs the waker. [`Wakes`] then runs
//! each waker's wake or drop under a catch of its own, so that a panic in one leaves no other waker
//! unrun. The first panic is raised again once every waker has run, or disposed of where the
//! thread is already unwinding: a second panic there would abort the process.

use core::{any::Any, panic::AssertUnwindSafe, task::Waker};
use std::{
  panic::{catch_unwind, resume_unwind},
  thread,
};

/// Wakers being run: woken or dropped, each under its own catch.
///
/// Dropped without [`finish`](Self::finish), on an unwind for instance, it disposes of the panic
/// it holds.
#[must_use = "`finish` raises a waker's panic again"]
pub(crate) struct Wakes {
  /// The first panic a waker raised.
  panic: Option<Box<dyn Any + Send>>,
}

impl Wakes {
  #[inline]
  pub(crate) const fn new() -> Self {
    Self { panic: None }
  }

  /// Wakes `waker`, where there is one.
  #[inline]
  pub(crate) fn wake(&mut self, waker: Option<Waker>) {
    if let Some(waker) = waker {
      self.run(move || waker.wake());
    }
  }

  /// Drops `waker`, where there is one.
  #[inline]
  pub(crate) fn discard(&mut self, waker: Option<Waker>) {
    if let Some(waker) = waker {
      self.run(move || drop(waker));
    }
  }

  fn run(&mut self, f: impl FnOnce()) {
    if let Err(panic) = catch_unwind(AssertUnwindSafe(f)) {
      match self.panic {
        None => self.panic = Some(panic),
        Some(_) => dispose(panic),
      }
    }
  }

  /// Raises the first panic again, unless the thread is already unwinding.
  pub(crate) fn finish(mut self) {
    if let Some(panic) = self.panic.take() {
      if thread::panicking() {
        dispose(panic);
      } else {
        resume_unwind(panic);
      }
    }
  }
}

impl Drop for Wakes {
  fn drop(&mut self) {
    if let Some(panic) = self.panic.take() {
      dispose(panic);
    }
  }
}

/// Drops a caught panic's payload, and each payload its drop panics with in turn, each under a
/// catch of its own, until one drop ends without a panic.
///
/// A payload whose drop panics hands over a payload of its own, which may own something only its
/// drop gives back: another channel's permit, for instance. So none is forgotten, however long
/// the chain. A chain that never ends is a destructor that never returns, like a loop in a `Drop`:
/// the user's code, and its to end.
pub(crate) fn dispose(payload: Box<dyn Any + Send>) {
  let mut payload = payload;
  while let Err(again) = catch_unwind(AssertUnwindSafe(move || drop(payload))) {
    payload = again;
  }
}

/// Wakes `waker`, as [`Wakes`] does.
#[inline]
pub(crate) fn wake(waker: Option<Waker>) {
  if waker.is_some() {
    let mut wakes = Wakes::new();
    wakes.wake(waker);
    wakes.finish();
  }
}

/// How many wakers [`Staged`] keeps without allocating: a single receive needs one, and a batch of
/// up to this many items allocates nothing for the reserves it grants room to.
const STAGED_INLINE: usize = 8;

/// The wakers of the reserves a batch grants room to, kept until the batch has taken every item
/// and given back every unit, and woken then.
///
/// A place for each is made before the item that may produce it leaves its lane, so that keeping
/// the waker once the item is out never needs memory and cannot fail. The first places are inline;
/// the rest are in a `Vec`, grown one place at a time, beforehand, and only where a batch takes
/// more items than the inline places.
pub(crate) struct Staged {
  inline: [Option<Waker>; STAGED_INLINE],
  more: Vec<Waker>,
  /// Places made.
  made: usize,
  /// Places used: one per item taken, whether it produced a waker or not.
  used: usize,
}

impl Staged {
  #[inline]
  pub(crate) const fn new() -> Self {
    Self {
      inline: [const { None }; STAGED_INLINE],
      more: Vec::new(),
      made: 0,
      used: 0,
    }
  }

  /// Makes a place for the waker one more item may produce, before the item is taken. False where
  /// none can be made: the batch stops there, and keeps what it holds.
  pub(crate) fn room(&mut self) -> bool {
    if self.made >= STAGED_INLINE {
      let needed = self.made + 1 - STAGED_INLINE;
      if self.more.capacity() < needed && self.more.try_reserve(needed - self.more.len()).is_err() {
        return false;
      }
    }
    self.made += 1;
    true
  }

  /// Keeps the waker an item taken produced, where it produced one, in the place made for it.
  pub(crate) fn keep(&mut self, waker: Option<Waker>) {
    debug_assert!(self.used < self.made, "a place was made first");
    let place = self.used;
    self.used += 1;
    if let Some(waker) = waker {
      match self.inline.get_mut(place) {
        Some(slot) => *slot = Some(waker),
        None => {
          debug_assert!(self.more.len() < self.more.capacity());
          self.more.push(waker);
        }
      }
    }
  }

  /// Wakes every waker kept, each under its own catch, and raises the first panic again once all
  /// have run, unless the thread is already unwinding.
  pub(crate) fn wake(&mut self) {
    let mut wakes = Wakes::new();
    for slot in &mut self.inline {
      wakes.wake(slot.take());
    }
    for waker in self.more.drain(..) {
      wakes.wake(Some(waker));
    }
    wakes.finish();
  }
}

/// Wakers a reserve no longer needs, kept until the reserve owns its verdict.
///
/// A reserve's poll can end with a waker it cloned and did not store, or one its refresh replaced.
/// Their drops are user code, so they are run only after the caller holds its `Permit`: a panic in
/// one then unwinds through the permit, which gives its room back, and no granted unit is lost.
pub(crate) struct Spare {
  wakers: [Option<Waker>; 2],
}

impl Spare {
  #[inline]
  pub(crate) const fn new() -> Self {
    Self {
      wakers: [None, None],
    }
  }

  /// Keeps `waker` to be dropped with the rest. A reserve's poll keeps at most two: the clone it
  /// made, and the waker that clone replaced.
  #[inline]
  pub(crate) fn keep(&mut self, waker: Option<Waker>) {
    let Some(waker) = waker else {
      return;
    };
    match self.wakers.iter_mut().find(|slot| slot.is_none()) {
      Some(slot) => *slot = Some(waker),
      None => {
        debug_assert!(false, "a reserve's poll keeps at most two wakers");
        let mut wakes = Wakes::new();
        wakes.discard(Some(waker));
        wakes.finish();
      }
    }
  }
}

impl Drop for Spare {
  fn drop(&mut self) {
    let mut wakes = Wakes::new();
    for waker in &mut self.wakers {
      wakes.discard(waker.take());
    }
    wakes.finish();
  }
}
