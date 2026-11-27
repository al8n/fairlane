//! A lane's remaining capacity, and the two flags that decide who may take it, in one word.
//!
//! The word is `free << 2 | WAITING | CLOSED`.
//!
//! - **`take`** takes one unit only where neither flag is set: while a reserve waits, every unit
//!   that comes back is granted to the oldest waiter, so a newcomer never passes it.
//! - **`give`** adds one unit only where `WAITING` is clear. With `WAITING` set it answers
//!   `Grant`: the unit goes to a waiter, under the reserve lock, and `free` does not move.
//!
//! The flags change only under the lane's reserve lock. `WAITING` is set only where `free` is 0,
//! and `free` grows only where `WAITING` is clear, so `WAITING` implies `free == 0`.

use crate::sync::{AtomicUsize, Ordering};

pub(crate) const WAITING: usize = 0b01;
pub(crate) const CLOSED: usize = 0b10;
const UNIT: usize = 0b100;

/// Why `take` could not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
  /// No unit is free, or a reserve waits for one.
  Busy,
  /// The lane is closed.
  Closed,
}

/// What `give` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Give {
  /// The unit is free again: the new word.
  Added(usize),
  /// A reserve waits: the unit is to be granted to it, under the reserve lock.
  Grant,
}

/// The word after one unit is taken, or why it cannot be.
#[inline]
pub(crate) const fn take(word: usize) -> Result<usize, Refusal> {
  if word & CLOSED != 0 {
    Err(Refusal::Closed)
  } else if word & WAITING != 0 || word < UNIT {
    Err(Refusal::Busy)
  } else {
    Ok(word - UNIT)
  }
}

/// The word after one unit is given back, or `Grant` where a reserve waits.
#[inline]
pub(crate) const fn give(word: usize) -> Give {
  if word & WAITING != 0 {
    Give::Grant
  } else {
    Give::Added(word + UNIT)
  }
}

/// The free units a word holds.
#[inline]
pub(crate) const fn free(word: usize) -> usize {
  word >> 2
}

// The reserve line's transitions, under its lock. Each is a function of the word and the number of
// reserves in line; the line applies it with a compare-and-swap, since a `take` or a `give` can move
// `free` while `WAITING` is clear. Together they keep, at every exit from the lock and while the
// lane is open: `WAITING` exactly while a reserve is in line, and `WAITING` only where `free` is 0.

/// What a reserve that found no unit it could take does, under the line lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Join {
  /// The lane is closed.
  Closed,
  /// A unit came back meanwhile and nobody is in line: it is taken. The new word.
  Take(usize),
  /// It joins the line. The new word, with `WAITING`.
  Queue(usize),
}

#[inline]
pub(crate) const fn join(word: usize) -> Join {
  if word & CLOSED != 0 {
    Join::Closed
  } else if word & WAITING == 0 && word >= UNIT {
    Join::Take(word - UNIT)
  } else {
    Join::Queue(word | WAITING)
  }
}

/// Where a unit that came back goes, under the line lock, with `queued` reserves in line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Back {
  /// To the oldest reserve in line. The new word: `WAITING` cleared where it was the last.
  Grant(usize),
  /// To `free`, nobody being in line. The new word.
  Add(usize),
}

#[inline]
pub(crate) const fn back(word: usize, queued: usize) -> Back {
  if queued > 0 {
    Back::Grant(if queued == 1 { word & !WAITING } else { word })
  } else {
    Back::Add((word & !WAITING) + UNIT)
  }
}

/// The word after a reserve in line leaves it, `queued` being left in line after it.
#[inline]
pub(crate) const fn left(word: usize, queued: usize) -> usize {
  if queued == 0 { word & !WAITING } else { word }
}

/// The word a close leaves: the `granted` units not taken up settled back into `free`, `WAITING`
/// cleared, `CLOSED` set.
#[inline]
pub(crate) const fn closed(word: usize, granted: usize) -> usize {
  self::word(free(word) + granted, CLOSED)
}

/// A word of `free` units and the given flags.
#[inline]
pub(crate) const fn word(free: usize, flags: usize) -> usize {
  free << 2 | flags
}

/// The atomic word itself. `capacity` is at most `usize::MAX >> 2`, which `Options::check` holds.
pub(crate) struct Remaining(AtomicUsize);

impl Remaining {
  #[inline]
  pub(crate) fn new(capacity: usize) -> Self {
    Self(AtomicUsize::new(word(capacity, 0)))
  }

  #[inline]
  pub(crate) fn load(&self) -> usize {
    self.0.load(Ordering::SeqCst)
  }

  /// Takes one unit where `take` allows it.
  pub(crate) fn try_take(&self) -> Result<(), Refusal> {
    let mut current = self.load();
    loop {
      let next = take(current)?;
      match self
        .0
        .compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst)
      {
        Ok(_) => return Ok(()),
        Err(seen) => current = seen,
      }
    }
  }

  /// Gives one unit back where `give` allows it; `Err(())` where a reserve waits and the unit is
  /// to be granted instead.
  pub(crate) fn try_give(&self) -> Result<(), ()> {
    let mut current = self.load();
    loop {
      let Give::Added(next) = give(current) else {
        return Err(());
      };
      match self
        .0
        .compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst)
      {
        Ok(_) => return Ok(()),
        Err(seen) => current = seen,
      }
    }
  }

  /// Replaces `current` by `next`, where the word is still `current`; else answers the word seen.
  #[inline]
  pub(crate) fn replace(&self, current: usize, next: usize) -> Result<(), usize> {
    self
      .0
      .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
      .map(drop)
  }

  /// Replaces the word by `f(word)` atomically, and answers the word `f` was applied to. Only the
  /// holder of the reserve lock changes the flags, through this.
  pub(crate) fn update(&self, f: impl Fn(usize) -> usize) -> usize {
    let mut current = self.load();
    loop {
      match self
        .0
        .compare_exchange_weak(current, f(current), Ordering::SeqCst, Ordering::SeqCst)
      {
        Ok(_) => return current,
        Err(seen) => current = seen,
      }
    }
  }
}

#[cfg(test)]
mod tests;
