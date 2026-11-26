//! A queue of waiters, oldest first, behind its owner's lock.
//!
//! - **Each waiter has a place,** named by a [`Key`] its one future holds, and a [`State`] that only
//!   the holder of the owner's lock changes. A waiter learns what happened to it by reading its own
//!   state, so a wake carries nothing.
//! - **Its waker is stored with it,** and taken out when the waiter is notified or closed, to be
//!   woken after the lock is let go. Nothing here clones, wakes or drops a waker: the caller does,
//!   outside the lock.
//! - **The places live in one vector,** reused. The queued ones form a list through it, oldest
//!   first, so a waiter that leaves from the middle of the line leaves at once. The vector keeps
//!   the most places ever taken at once: as many as there were waiters at one time.

use core::task::Waker;

/// No place.
const NIL: usize = usize::MAX;

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
  /// `Some` while queued.
  waker: Option<Waker>,
  /// The neighbours in line, while queued.
  prev: usize,
  next: usize,
}

enum Slot {
  /// Free, with the next free place.
  Vacant(usize),
  Taken(Entry),
}

pub(crate) struct Waiters {
  slots: Vec<Slot>,
  /// The first free place.
  vacant: usize,
  /// The oldest and the newest queued places.
  head: usize,
  tail: usize,
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
      head: NIL,
      tail: NIL,
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

  /// Waiters notified, and not yet removed by their holders.
  #[inline]
  pub(crate) fn notified(&self) -> usize {
    self.notified
  }

  /// Puts a waiter at the back of the line, with its waker.
  pub(crate) fn insert(&mut self, waker: Waker) -> Key {
    let entry = Entry {
      state: State::Queued,
      waker: Some(waker),
      prev: self.tail,
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
    match self.tail {
      NIL => self.head = index,
      tail => self.entry_at_mut(tail).next = index,
    }
    self.tail = index;
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
    let index = self.head;
    if index == NIL {
      return None;
    }
    self.unlink(index);
    self.notified += 1;
    let entry = self.entry_at_mut(index);
    entry.state = State::Notified;
    Some(entry.waker.take())
  }

  /// Notifies every waiter in line, oldest first, pushing their wakers onto `wakers`.
  pub(crate) fn notify_all(&mut self, wakers: &mut Vec<Waker>) {
    while let Some(waker) = self.notify_oldest() {
      wakers.extend(waker);
    }
  }

  /// Closes every waiter, queued or notified, pushing the wakers of those in line onto `wakers`.
  /// Answers how many had been notified: for a reserve line, the grants not taken up.
  pub(crate) fn close_all(&mut self, wakers: &mut Vec<Waker>) -> usize {
    let notified = self.notified;
    for slot in &mut self.slots {
      if let Slot::Taken(entry) = slot {
        if entry.state != State::Closed {
          entry.state = State::Closed;
          wakers.extend(entry.waker.take());
        }
      }
    }
    self.head = NIL;
    self.tail = NIL;
    self.queued = 0;
    self.notified = 0;
    notified
  }

  /// Frees the place of `key`, whatever its state, and answers the state it had and the waker it
  /// still held, for the caller to drop after the lock.
  pub(crate) fn remove(&mut self, key: Key) -> (State, Option<Waker>) {
    let state = self.entry(&key).state;
    match state {
      State::Queued => self.unlink(key.index),
      State::Notified => self.notified -= 1,
      State::Closed => {}
    }
    let slot = core::mem::replace(&mut self.slots[key.index], Slot::Vacant(self.vacant));
    self.vacant = key.index;
    match slot {
      Slot::Taken(entry) => (state, entry.waker),
      Slot::Vacant(_) => unreachable!("a key names a taken place"),
    }
  }

  /// Takes the queued place at `index` out of the line.
  fn unlink(&mut self, index: usize) {
    let (prev, next) = {
      let entry = self.entry_at(index);
      (entry.prev, entry.next)
    };
    match prev {
      NIL => self.head = next,
      prev => self.entry_at_mut(prev).next = next,
    }
    match next {
      NIL => self.tail = prev,
      next => self.entry_at_mut(next).prev = prev,
    }
    let entry = self.entry_at_mut(index);
    entry.prev = NIL;
    entry.next = NIL;
    self.queued -= 1;
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

/// A fresh queue's identity, for the debug check that a key goes back to its own queue. A plain
/// std atomic: it is not part of any model.
#[cfg(debug_assertions)]
fn next_id() -> usize {
  static NEXT: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(0);
  NEXT.fetch_add(1, core::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests;
