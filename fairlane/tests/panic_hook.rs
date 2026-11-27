//! No panic of fairlane's runs while it holds a lock of its own.
//!
//! A panic hook runs before the unwind lets any guard go, so a hook that waits on a thread that
//! receives from the channel would wait for ever behind a lock the panicking call still holds. The
//! binary has no test harness: the hook is process-wide, and no other test's panic may reach it.

use std::{
  num::NonZeroUsize,
  panic::{self, AssertUnwindSafe},
  sync::{
    Mutex,
    atomic::{AtomicBool, Ordering::SeqCst},
    mpsc,
  },
  thread,
  time::Duration,
};

use fairlane::{Lane, LaneOptions, Options, channel};

/// Whether the lanes' `index` answers have drifted.
static DRIFTED: AtomicBool = AtomicBool::new(false);

/// Two lanes whose `index` drifts out of range once `DRIFTED` is set, which breaks `Lane`'s
/// contract: the channel may answer wrongly, but raises no panic of its own under its locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Drifting {
  A,
  B,
}

impl Lane for Drifting {
  type N = typenum::U2;

  fn index(self) -> usize {
    self as usize + if DRIFTED.load(SeqCst) { 2 } else { 0 }
  }

  fn from_index(index: usize) -> Option<Self> {
    [Self::A, Self::B].get(index).copied()
  }

  fn as_str(self) -> &'static str {
    match self {
      Self::A => "a",
      Self::B => "b",
    }
  }
}

/// A receive of two lanes, under the default policy, with lanes whose indices have drifted: the
/// policy is asked under the choice lock. Should any panic of fairlane's run there, the hook's own
/// receive, made on another thread, would wait on that lock.
fn a_panic_hook_never_waits_on_the_choice_lock() {
  let options = Options::<Drifting>::from_fn(|_| LaneOptions::new(NonZeroUsize::MIN));
  let (tx, rx) = channel::<u32, Drifting>(options).unwrap();
  tx.try_send(Drifting::A, 1).unwrap();
  tx.try_send(Drifting::B, 2).unwrap();
  let hook_rx = Mutex::new(Some(rx.clone()));
  let seen: &'static Mutex<Option<bool>> = Box::leak(Box::new(Mutex::new(None)));
  panic::set_hook(Box::new(move |_| {
    let Some(rx) = hook_rx.lock().unwrap().take() else {
      return;
    };
    let (done, finished) = mpsc::channel();
    thread::spawn(move || {
      let _ = rx.try_recv();
      let _ = done.send(());
    });
    let returned = finished.recv_timeout(Duration::from_secs(2)).is_ok();
    *seen.lock().unwrap() = Some(returned);
  }));
  DRIFTED.store(true, SeqCst);
  let received = panic::catch_unwind(AssertUnwindSafe(|| rx.try_recv()));
  DRIFTED.store(false, SeqCst);
  drop(panic::take_hook());
  match *seen.lock().unwrap() {
    None => assert!(
      matches!(received, Ok(Ok(Some(_)))),
      "no panic, and an item is received"
    ),
    Some(false) => {
      panic!("a panic of fairlane's ran under the choice lock: the hook's receive waited")
    }
    Some(true) => panic!("a panic of fairlane's ran, though no lock was held"),
  }
}

fn main() {
  a_panic_hook_never_waits_on_the_choice_lock();
  println!("panic_hook: no panic of fairlane's ran under its locks");
}
