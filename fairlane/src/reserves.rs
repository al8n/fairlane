//! A lane's waiting reserves: oldest first, and a unit that comes back is granted, not freed.
//!
//! Every decision is made under the reserve lock, and nothing foreign runs under it: an
//! event-listener `notify` wakes a waker, and a listener's `discard` drops one, so both happen
//! after the lock is let go. The lock holds two counts:
//!
//! - `waiting`: reserves in line that no grant has been decided for;
//! - `granted`: grants decided and not yet taken up by their waiter.
//!
//! A grant's notification carries no verdict. A woken reserve reads its verdict from this state:
//! `Closed` where the lane is closed, else one grant. A close settles every grant not yet taken
//! up back into the remaining capacity, so a reserve that was granted and had not taken its grant
//! when the close came resolves `Closed`.
//!
//! A grant decided for a waiter that leaves before its notification lands reaches nobody:
//! `notify` answers 0, and the unit is released again from the top.
//!
//! `event.listen()` runs under the line lock: it inserts a listener and runs no foreign code. This
//! assumes what wakers are expected to do: `wake` schedules its task and never polls it inline.
//! event-listener runs wakers inside its own list lock, so a waker that polled inline would take
//! that lock again on its first listener poll and deadlock event-listener by itself; taking
//! `listen()` out of the line lock would therefore remove no deadlock.

use event_listener::{Event, EventListener, IntoNotification, Listener};

use crate::{
  remaining::{CLOSED, Remaining, WAITING, free, word},
  sync::{AtomicUsize, Mutex, Ordering, lock},
};

pub(crate) struct Reserves {
  line: Mutex<Line>,
  event: Event,
  /// Reserves that wait and have not resolved: the snapshot's `senders_waiting`.
  parked: AtomicUsize,
}

#[derive(Default)]
struct Line {
  waiting: usize,
  granted: usize,
}

/// What a reserve that found no free unit did.
pub(crate) enum Begin {
  /// A unit had come back: it is taken.
  Granted,
  /// It waits in line.
  Waiting(EventListener),
  Closed,
}

/// What a woken reserve learns.
pub(crate) enum Verdict {
  Granted,
  Closed,
  /// The wake carried no grant: it waits again.
  Again(EventListener),
}

impl Reserves {
  pub(crate) fn new() -> Self {
    Self {
      line: Mutex::new(Line::default()),
      event: Event::new(),
      parked: AtomicUsize::new(0),
    }
  }

  /// Reserves that wait now.
  #[inline]
  pub(crate) fn parked(&self) -> usize {
    self.parked.load(Ordering::SeqCst)
  }

  /// A reserve whose `take` was refused: it takes a unit that came back meanwhile, or waits.
  pub(crate) fn begin(&self, remaining: &Remaining) -> Begin {
    let mut line = lock(&self.line);
    match Self::queue(&mut line, remaining) {
      Ok(false) => Begin::Granted,
      Ok(true) => {
        self.parked.fetch_add(1, Ordering::SeqCst);
        Begin::Waiting(self.event.listen())
      }
      Err(()) => Begin::Closed,
    }
  }

  /// A waiting reserve was woken: its verdict, read from the line.
  pub(crate) fn woken(&self, remaining: &Remaining) -> Verdict {
    let mut line = lock(&self.line);
    if remaining.load() & CLOSED != 0 {
      self.parked.fetch_sub(1, Ordering::SeqCst);
      return Verdict::Closed;
    }
    if line.granted > 0 {
      line.granted -= 1;
      self.parked.fetch_sub(1, Ordering::SeqCst);
      return Verdict::Granted;
    }
    match Self::queue(&mut line, remaining) {
      Ok(false) => {
        self.parked.fetch_sub(1, Ordering::SeqCst);
        Verdict::Granted
      }
      Ok(true) => Verdict::Again(self.event.listen()),
      Err(()) => {
        self.parked.fetch_sub(1, Ordering::SeqCst);
        Verdict::Closed
      }
    }
  }

  /// Under the lock: takes a free unit where the line is empty (`Ok(false)`), or joins the line
  /// (`Ok(true)`). `Err(())` where the lane is closed.
  fn queue(line: &mut Line, remaining: &Remaining) -> Result<bool, ()> {
    let mut current = remaining.load();
    loop {
      if current & CLOSED != 0 {
        return Err(());
      }
      let next = if current & WAITING == 0 && free(current) > 0 {
        current - word(1, 0)
      } else {
        current | WAITING
      };
      match remaining.replace(current, next) {
        Ok(()) if next & WAITING == 0 => return Ok(false),
        Ok(()) => {
          line.waiting += 1;
          return Ok(true);
        }
        Err(seen) => current = seen,
      }
    }
  }

  /// A waiting reserve dropped before it resolved. A grant it held goes on, as a release would.
  pub(crate) fn leave(&self, remaining: &Remaining, listener: EventListener) {
    let notified = listener.discard();
    let pass_on = {
      let mut line = lock(&self.line);
      self.parked.fetch_sub(1, Ordering::SeqCst);
      if notified {
        // A grant, or the close's wake. A close settled every grant already.
        let open = remaining.load() & CLOSED == 0;
        if open && line.granted > 0 {
          line.granted -= 1;
          true
        } else {
          false
        }
      } else {
        // No notification: still in line, unless a grant decided for it is on its way to the
        // next waiter. Either way the line holds one waiter fewer.
        if line.waiting > 0 {
          line.waiting -= 1;
          if line.waiting == 0 {
            remaining.update(|current| current & !WAITING);
          }
        }
        false
      }
    };
    if pass_on {
      self.release(remaining);
    }
  }

  /// One unit back: free again where nobody waits, else granted to the oldest waiter.
  pub(crate) fn release(&self, remaining: &Remaining) {
    loop {
      if remaining.try_give().is_ok() {
        return;
      }
      {
        let mut line = lock(&self.line);
        if remaining.load() & WAITING == 0 {
          // The line emptied since the word was read: give the unit back freely.
          continue;
        }
        line.waiting -= 1;
        line.granted += 1;
        if line.waiting == 0 {
          remaining.update(|current| current & !WAITING);
        }
      }
      if self.event.notify(1.additional()) == 1 {
        return;
      }
      // The waiter left first. The unit is this release's again, unless a close settled it.
      let mut line = lock(&self.line);
      if remaining.load() & CLOSED != 0 {
        return;
      }
      line.granted -= 1;
    }
  }

  /// Closes the lane's room: `CLOSED` set, every grant not taken up settled back, and every
  /// waiter woken to read `Closed`. A second close does nothing.
  pub(crate) fn close(&self, remaining: &Remaining) {
    {
      let mut line = lock(&self.line);
      let granted = line.granted;
      let before = remaining.update(|current| {
        if current & CLOSED != 0 {
          current
        } else {
          word(free(current) + granted, CLOSED)
        }
      });
      if before & CLOSED != 0 {
        return;
      }
      line.granted = 0;
      line.waiting = 0;
    }
    self.event.notify(usize::MAX);
  }
}

#[cfg(test)]
mod tests;
