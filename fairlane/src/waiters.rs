//! A queue of waiters, oldest first, behind its owner's lock.
//!
//! - **Each waiter has a place,** named by a [`Key`] its one future holds, and a [`State`] that only
//!   the holder of the owner's lock changes. A waiter learns what happened to it by reading its own
//!   state, so a wake carries nothing.
//! - **Its waker is stored with it.** A notice to one waiter takes the waker out, for the caller to
//!   wake after the lock. A broadcast changes every waiter's state and leaves each waker in its
//!   place, on the owed list; [`dispatch`] then wakes them a fixed number at a time, each chunk
//!   after the lock. So a broadcast needs no memory beyond the places themselves. Nothing here
//!   clones, wakes or drops a waker under the lock.
//! - **The places live in one vector,** reused. The queued ones form a list through it, oldest
//!   first, and the owed ones another, so a waiter that leaves leaves its list at once. The vector
//!   keeps the most places ever taken at once: as many as there were waiters at one time. It grows
//!   through [`Waiters::make_room`], before a waker is handed over.

use core::task::Waker;

use crate::{
  sync::{Mutex, lock},
  wake::Wakes,
};

/// No place.
const NIL: usize = usize::MAX;

/// How many owed wakers [`dispatch`] takes out under the lock at a time.
const CHUNK: usize = 32;

/// Where a waiter is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
  /// In line.
  Queued,
  /// Taken out of the line by its owner's notice: for a reserve, granted a unit.
  Notified,
  /// Taken out of the line by a close.
  Closed,
}

/// A waiter's place. Its one holder gives it back to [`Waiters::remove`]; it is neither `Clone`
/// nor `Copy`, so a place has one owner and is freed once.
#[derive(Debug)]
pub(crate) struct Key {
  index: usize,
  /// The queue the key belongs to, checked in debug builds.
  #[cfg(debug_assertions)]
  queue: usize,
}

struct Entry {
  state: State,
  /// `Some` while queued, and while a broadcast's wake is owed.
  waker: Option<Waker>,
  /// Whether it is on the owed list: a broadcast changed its state and left its waker here.
  owed: bool,
  /// The neighbours in its list: the line while queued, the owed list while owed.
  prev: usize,
  next: usize,
}

enum Slot {
  /// Free, with the next free place.
  Vacant(usize),
  Taken(Entry),
}

/// The ends of a list through the places.
#[derive(Clone, Copy)]
struct List {
  head: usize,
  tail: usize,
}

impl List {
  const EMPTY: Self = Self {
    head: NIL,
    tail: NIL,
  };
}

/// Which list a place is on.
#[derive(Clone, Copy)]
enum Which {
  Line,
  Owed,
}

pub(crate) struct Waiters {
  slots: Vec<Slot>,
  /// The first free place.
  vacant: usize,
  /// The queued places, oldest first.
  line: List,
  /// The places a broadcast left a waker in, to be woken.
  owed: List,
  queued: usize,
  notified: usize,
  #[cfg(debug_assertions)]
  id: usize,
}

impl Waiters {
  pub(crate) fn new() -> Self {
    Self {
      slots: Vec::new(),
      vacant: NIL,
      line: List::EMPTY,
      owed: List::EMPTY,
      queued: 0,
      notified: 0,
      #[cfg(debug_assertions)]
      id: next_id(),
    }
  }

  /// Waiters in line.
  #[inline]
  pub(crate) fn queued(&self) -> usize {
    self.queued
  }

  /// The places the vector holds room for, taken or not.
  #[cfg(test)]
  pub(crate) fn room(&self) -> usize {
    self.slots.capacity()
  }

  /// Waiters notified, and not yet removed by their holders.
  #[cfg(test)]
  #[inline]
  pub(crate) fn notified(&self) -> usize {
    self.notified
  }

  /// Makes room for one more waiter, where no freed place is left, before a waker is handed over:
  /// the waker is never in a place that is still being made. It allocates as std does.
  pub(crate) fn make_room(&mut self) {
    if self.vacant == NIL {
      self.slots.reserve(1);
    }
  }

  /// Puts a waiter at the back of the line, with its waker, in the room
  /// [`make_room`](Self::make_room) made: it allocates nothing and cannot fail.
  pub(crate) fn insert(&mut self, waker: Waker) -> Key {
    debug_assert!(
      self.vacant != NIL || self.slots.len() < self.slots.capacity(),
      "room was made first"
    );
    let entry = Entry {
      state: State::Queued,
      waker: Some(waker),
      owed: false,
      prev: NIL,
      next: NIL,
    };
    let index = if self.vacant == NIL {
      self.slots.push(Slot::Taken(entry));
      self.slots.len() - 1
    } else {
      let index = self.vacant;
      self.vacant = match self.slots[index] {
        Slot::Vacant(next) => next,
        Slot::Taken(_) => unreachable!("the free list names a taken place"),
      };
      self.slots[index] = Slot::Taken(entry);
      index
    };
    self.link_back(Which::Line, index);
    self.queued += 1;
    Key {
      index,
      #[cfg(debug_assertions)]
      queue: self.id,
    }
  }

  /// Where the waiter of `key` is.
  #[inline]
  pub(crate) fn state(&self, key: &Key) -> State {
    self.entry(key).state
  }

  /// Whether the waker stored for `key` wakes the same task as `waker`. No user code runs.
  #[inline]
  pub(crate) fn will_wake(&self, key: &Key, waker: &Waker) -> bool {
    self
      .entry(key)
      .waker
      .as_ref()
      .is_some_and(|stored| stored.will_wake(waker))
  }

  /// Stores `waker` for the queued waiter of `key`, and answers the one it replaces, for the caller
  /// to drop after the lock.
  #[inline]
  pub(crate) fn set_waker(&mut self, key: &Key, waker: Waker) -> Option<Waker> {
    let entry = self.entry_mut(key);
    debug_assert_eq!(entry.state, State::Queued);
    entry.waker.replace(waker)
  }

  /// Notifies the oldest waiter in line: out of the line, `Notified`, and its waker taken out to be
  /// woken. `None` where nobody is in line.
  pub(crate) fn notify_oldest(&mut self) -> Option<Option<Waker>> {
    let index = self.line.head;
    if index == NIL {
      return None;
    }
    self.unlink(Which::Line, index);
    self.queued -= 1;
    self.notified += 1;
    let entry = self.entry_at_mut(index);
    entry.state = State::Notified;
    Some(entry.waker.take())
  }

  /// Notifies every waiter in line, and leaves each waker owed, for [`dispatch`] to wake.
  pub(crate) fn notify_all(&mut self) {
    self.notified += self.queued;
    self.broadcast(State::Notified);
  }

  /// Closes every waiter, queued or notified, and leaves the wakers of those in line owed, for
  /// [`dispatch`] to wake. Answers how many had been notified: for a reserve line, the grants not
  /// taken up.
  pub(crate) fn close_all(&mut self) -> usize {
    let notified = self.notified;
    for slot in &mut self.slots {
      if let Slot::Taken(entry) = slot {
        if entry.state == State::Notified {
          entry.state = State::Closed;
        }
      }
    }
    self.notified = 0;
    self.broadcast(State::Closed);
    notified
  }

  /// Moves every waiter in line to `state` and onto the owed list, its waker left in place.
  fn broadcast(&mut self, state: State) {
    let mut at = self.line.head;
    while at != NIL {
      let entry = self.entry_at_mut(at);
      entry.state = state;
      entry.owed = true;
      at = entry.next;
    }
    let line = core::mem::replace(&mut self.line, List::EMPTY);
    if line.head != NIL {
      match self.owed.tail {
        NIL => self.owed.head = line.head,
        tail => {
          self.entry_at_mut(tail).next = line.head;
          self.entry_at_mut(line.head).prev = tail;
        }
      }
      self.owed.tail = line.tail;
    }
    self.queued = 0;
  }

  /// Takes the wakers of up to `out.len()` owed places into `out`, and answers how many.
  pub(crate) fn take_owed(&mut self, out: &mut [Option<Waker>]) -> usize {
    let mut taken = 0;
    while taken < out.len() && self.owed.head != NIL {
      let index = self.owed.head;
      self.unlink(Which::Owed, index);
      let entry = self.entry_at_mut(index);
      entry.owed = false;
      out[taken] = entry.waker.take();
      taken += 1;
    }
    taken
  }

  /// Frees the place of `key`, whatever its state, and answers the state it had and the waker it
  /// still held, for the caller to drop after the lock. An owed waker is the holder's own, polled
  /// before the broadcast's wake reached it: it needs no wake.
  pub(crate) fn remove(&mut self, key: Key) -> (State, Option<Waker>) {
    let (state, owed) = {
      let entry = self.entry(&key);
      (entry.state, entry.owed)
    };
    if owed {
      self.unlink(Which::Owed, key.index);
    } else if state == State::Queued {
      self.unlink(Which::Line, key.index);
      self.queued -= 1;
    }
    if state == State::Notified {
      self.notified -= 1;
    }
    let slot = core::mem::replace(&mut self.slots[key.index], Slot::Vacant(self.vacant));
    self.vacant = key.index;
    match slot {
      Slot::Taken(entry) => (state, entry.waker),
      Slot::Vacant(_) => unreachable!("a key names a taken place"),
    }
  }

  fn list(&mut self, which: Which) -> &mut List {
    match which {
      Which::Line => &mut self.line,
      Which::Owed => &mut self.owed,
    }
  }

  /// Puts the place at `index` at the back of a list.
  fn link_back(&mut self, which: Which, index: usize) {
    let tail = self.list(which).tail;
    {
      let entry = self.entry_at_mut(index);
      entry.prev = tail;
      entry.next = NIL;
    }
    match tail {
      NIL => self.list(which).head = index,
      tail => self.entry_at_mut(tail).next = index,
    }
    self.list(which).tail = index;
  }

  /// Takes the place at `index` out of a list.
  fn unlink(&mut self, which: Which, index: usize) {
    let (prev, next) = {
      let entry = self.entry_at(index);
      (entry.prev, entry.next)
    };
    match prev {
      NIL => self.list(which).head = next,
      prev => self.entry_at_mut(prev).next = next,
    }
    match next {
      NIL => self.list(which).tail = prev,
      next => self.entry_at_mut(next).prev = prev,
    }
    let entry = self.entry_at_mut(index);
    entry.prev = NIL;
    entry.next = NIL;
  }

  #[inline]
  fn entry(&self, key: &Key) -> &Entry {
    #[cfg(debug_assertions)]
    debug_assert_eq!(key.queue, self.id, "a key used on another queue");
    self.entry_at(key.index)
  }

  #[inline]
  fn entry_mut(&mut self, key: &Key) -> &mut Entry {
    #[cfg(debug_assertions)]
    debug_assert_eq!(key.queue, self.id, "a key used on another queue");
    self.entry_at_mut(key.index)
  }

  #[inline]
  fn entry_at(&self, index: usize) -> &Entry {
    match &self.slots[index] {
      Slot::Taken(entry) => entry,
      Slot::Vacant(_) => unreachable!("a key names a taken place"),
    }
  }

  #[inline]
  fn entry_at_mut(&mut self, index: usize) -> &mut Entry {
    match &mut self.slots[index] {
      Slot::Taken(entry) => entry,
      Slot::Vacant(_) => unreachable!("a key names a taken place"),
    }
  }
}

/// Wakes every waker a broadcast left owed in `line`, [`CHUNK`] at a time: each chunk is taken out
/// under the lock into a buffer on the stack, and woken after it. Fairlane allocates nothing here,
/// so no allocation of its own stands between the broadcast's state changes and their wakes.
pub(crate) fn dispatch(line: &Mutex<Waiters>, wakes: &mut Wakes) {
  loop {
    let mut chunk: [Option<Waker>; CHUNK] = [const { None }; CHUNK];
    let taken = lock(line).take_owed(&mut chunk);
    for waker in &mut chunk[..taken] {
      wakes.wake(waker.take());
    }
    if taken < CHUNK {
      return;
    }
  }
}

/// A fresh queue's identity, for the debug check that a key goes back to its own queue. A plain
/// std atomic: it is not part of any model.
#[cfg(debug_assertions)]
fn next_id() -> usize {
  static NEXT: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);
  NEXT.fetch_add(1, core::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests;
