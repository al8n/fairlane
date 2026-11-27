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

/// How many channels' choices one thread can be marked inside at once. More than one only where a
/// policy of one channel receives from another, which the contract forbids; past this depth the
/// check, which is best effort, does not mark.
const DEPTH: usize = 8;

/// The channels whose choice lock this thread holds, by address. A fixed table with no
/// destructor, so fairlane allocates nothing for it; the thread-local storage itself is std's.
#[derive(Clone, Copy)]
struct Held {
  channels: [usize; DEPTH],
  len: usize,
}

impl Held {
  const EMPTY: Self = Self {
    channels: [0; DEPTH],
    len: 0,
  };

  fn contains(&self, channel: usize) -> bool {
    self.channels[..self.len].contains(&channel)
  }
}

// Under loom, loom's thread-local, one per modelled thread; its macro takes no `const`
// initializer.
#[cfg(not(loom))]
std::thread_local! {
  static CHOOSING: RefCell<Held> = const { RefCell::new(Held::EMPTY) };
}
#[cfg(loom)]
loom::thread_local! {
  static CHOOSING: RefCell<Held> = RefCell::new(Held::EMPTY);
}

/// This thread holds the choice lock of the channel at an address, until the guard is dropped.
#[must_use = "the marker lasts as long as the guard"]
pub(crate) struct Choosing {
  channel: usize,
  /// Whether the table had room for the mark.
  marked: bool,
}

impl Choosing {
  /// Marks the channel at `channel` as chosen for on this thread. Fairlane allocates nothing here.
  pub(crate) fn enter(channel: usize) -> Self {
    let marked = CHOOSING
      .try_with(|held| {
        let mut held = held.borrow_mut();
        let len = held.len;
        if len == DEPTH {
          return false;
        }
        held.channels[len] = channel;
        held.len = len + 1;
        true
      })
      .unwrap_or(false);
    Self { channel, marked }
  }
}

impl Drop for Choosing {
  fn drop(&mut self) {
    if !self.marked {
      return;
    }
    let _ = CHOOSING.try_with(|held| {
      let mut held = held.borrow_mut();
      let len = held.len;
      if let Some(at) = held.channels[..len]
        .iter()
        .rposition(|&channel| channel == self.channel)
      {
        held.channels.copy_within(at + 1..len, at);
        held.len = len - 1;
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
    .try_with(|held| held.borrow().contains(channel))
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

#[cfg(test)]
mod tests;
