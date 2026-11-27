//! Running wakers, which are user code, so that one that panics stops nothing else.
//!
//! A waker's `wake`, `clone` and `drop` may each panic. The crate never runs one under its locks,
//! and makes the state changes a waker stands for before it runs the waker. [`Wakes`] then runs
//! each waker's wake or drop under a catch of its own, so that a panic in one leaves no other waker
//! unrun. The first panic is raised again once every waker has run, or disposed of where the
//! thread is already unwinding: a second panic there would abort the process.

use core::{any::Any, mem, panic::AssertUnwindSafe, task::Waker};
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

/// How many payloads one disposal drops in turn, each caught from the drop of the one before.
///
/// A payload whose drop panics hands over a payload of its own, which may own something that only
/// its drop gives back: another channel's permit, for instance. So each is dropped in turn. A
/// destructor that panics anew at every drop would make the chain endless; past this many links
/// the last payload is forgotten, a leak, so that a disposal always ends.
const DISPOSALS: usize = 8;

/// Drops a caught panic's payload, and each payload its drop panics with in turn, each under a
/// catch of its own, up to [`DISPOSALS`] of them.
pub(crate) fn dispose(payload: Box<dyn Any + Send>) {
  let mut payload = payload;
  for _ in 0..DISPOSALS {
    match catch_unwind(AssertUnwindSafe(move || drop(payload))) {
      Ok(()) => return,
      Err(again) => payload = again,
    }
  }
  mem::forget(payload);
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

/// Wakes every waker of `wakers`, as [`Wakes`] does.
pub(crate) fn wake_all(wakers: Vec<Waker>) {
  let mut wakes = Wakes::new();
  for waker in wakers {
    wakes.wake(Some(waker));
  }
  wakes.finish();
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
