//! Refusing, by name, a policy that calls back into the channel it serves.
//!
//! A receive of a channel of two lanes or more holds the channel's choice lock while it asks the
//! policy. A policy, or a `Lane` method or a clock it calls, that receives from that channel on the
//! same thread would wait for ever on the lock its own thread holds, and every other receiver with
//! it. One that reserves room it must wait for would wait for a receive that can never come.
//!
//! So each thread keeps the channels whose choice lock it holds, and the doors that would wait
//! refuse to run inside them, with a panic that names the cause: as a `RefCell` refuses a second
//! borrow. The marker is a guard, undone on every exit from the choice, an unwind included, so a
//! thread that caught a policy's panic keeps receiving.

use core::cell::RefCell;

use crate::sync::thread_local;

thread_local! {
  /// The channels whose choice lock this thread holds, by address: more than one only where a
  /// policy of one channel receives from another.
  static CHOOSING: RefCell<Vec<usize>> = RefCell::new(Vec::new());
}

/// This thread holds the choice lock of the channel at an address, until the guard is dropped.
#[must_use = "the marker lasts as long as the guard"]
pub(crate) struct Choosing(usize);

impl Choosing {
  /// Marks the channel at `channel` as chosen for on this thread. The list keeps its capacity, so
  /// a receive allocates nothing after the thread's first.
  pub(crate) fn enter(channel: usize) -> Self {
    let _ = CHOOSING.try_with(|held| held.borrow_mut().push(channel));
    Self(channel)
  }
}

impl Drop for Choosing {
  fn drop(&mut self) {
    let _ = CHOOSING.try_with(|held| {
      let mut held = held.borrow_mut();
      if let Some(at) = held.iter().rposition(|&channel| channel == self.0) {
        held.remove(at);
      }
    });
  }
}

/// Refuses `door` on the channel at `channel` where this thread is inside that channel's choice.
///
/// # Panics
///
/// Exactly there: the call would wait for ever.
#[inline]
pub(crate) fn refuse(channel: usize, door: &str) {
  let inside = CHOOSING
    .try_with(|held| held.borrow().contains(&channel))
    .unwrap_or(false);
  if inside {
    reentered(door);
  }
}

#[cold]
#[inline(never)]
fn reentered(door: &str) -> ! {
  panic!(
    "a `Policy`, or code it calls, {door} on the channel it serves, from inside that channel's \
     lane choice: it would wait for ever on the choice lock this thread holds"
  )
}
