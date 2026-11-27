//! A lane's waiting reserves: oldest first, and a unit that comes back is granted, not freed.
//!
//! - **The line** is a [`Waiters`] queue under the lane's reserve lock. A release that finds a
//!   reserve in line GRANTS the oldest one its unit by making it `Notified`, under the lock, and
//!   `free` does not move. A close makes every reserve `Closed`, and settles each grant not yet
//!   taken up back into the remaining capacity. A reserve reads its verdict from its own state, so
//!   a wake carries nothing; a reserve that leaves reads what it holds, and passes a grant on or
//!   gives up its place.
//! - **The word's flags** change only under the lock, through the line's transitions in
//!   [`remaining`](crate::remaining): `WAITING` is set exactly while a reserve is in line.
//! - **No user code runs under the lock.** A waker is cloned before the lock is taken; one taken
//!   out under it is woken or dropped after it is let go. A waker can do anything there, such as
//!   drop another reserve of this lane, which takes this lock again.
//! - **No user code runs while a place or a grant has no owner.** A reserve's [`Key`] stays in its
//!   future across every waker clone, and leaves it only under the lock, in the step that frees its
//!   place. Wakers a poll no longer needs go back to the caller in a [`Spare`], to be dropped once
//!   the caller holds its permit.

use core::{
  mem,
  task::{Poll, Waker},
};

use crate::{
  error::Closed,
  reentry,
  remaining::{Back, CLOSED, Join, Refusal, Remaining, back, closed, join, left},
  sync::{AtomicUsize, Mutex, Ordering, lock},
  waiters::{self, Key, State, Waiters},
  wake::{Spare, Wakes},
};

/// Where a reserve is.
pub(crate) enum Wait {
  Start,
  /// In the line, or granted or closed and not yet told.
  Waiting(Key),
  Done,
}

pub(crate) struct Reserves {
  line: Mutex<Waiters>,
  /// Reserves that wait and have not resolved: the snapshot's `senders_waiting`.
  parked: AtomicUsize,
}

impl Reserves {
  pub(crate) fn new() -> Self {
    Self {
      line: Mutex::new(Waiters::new()),
      parked: AtomicUsize::new(0),
    }
  }

  /// Reserves that wait now.
  #[inline]
  pub(crate) fn parked(&self) -> usize {
    self.parked.load(Ordering::SeqCst)
  }

  /// Polls the reserve at `wait`, of the channel at `channel`, to its verdict. Wakers it no longer
  /// needs go to `spare`, for the caller to drop once it owns the verdict.
  pub(crate) fn poll(
    &self,
    remaining: &Remaining,
    wait: &mut Wait,
    waker: &Waker,
    spare: &mut Spare,
    channel: usize,
  ) -> Poll<Result<(), Closed>> {
    match wait {
      Wait::Start => match remaining.try_take() {
        Ok(()) => done(wait, Ok(())),
        Err(Refusal::Closed) => done(wait, Err(Closed)),
        Err(Refusal::Busy) => {
          // A reserve that must wait, inside its own channel's choice, would wait for a receive
          // that can never come. Only this slow path pays for the check.
          reentry::refuse(channel, "waited for room");
          self.begin(remaining, wait, waker.clone(), spare)
        }
      },
      Wait::Waiting(_) => self.again(wait, waker, spare, channel),
      // Polled after it resolved: the future's contract leaves this open, and it never wakes.
      Wait::Done => Poll::Pending,
    }
  }

  /// A reserve whose `take` was refused, with its waker's clone: it takes a unit that came back
  /// meanwhile, or joins the line.
  fn begin(
    &self,
    remaining: &Remaining,
    wait: &mut Wait,
    fresh: Waker,
    spare: &mut Spare,
  ) -> Poll<Result<(), Closed>> {
    let mut line = lock(&self.line);
    // Before the word changes, and before the waker is handed over.
    if let Err(error) = line.make_room() {
      drop(line);
      drop(fresh);
      waiters::full(error);
    }
    let mut current = remaining.load();
    loop {
      let step = join(current);
      let next = match step {
        Join::Closed => {
          drop(line);
          spare.keep(Some(fresh));
          return done(wait, Err(Closed));
        }
        Join::Take(next) | Join::Queue(next) => next,
      };
      match remaining.replace(current, next) {
        Ok(()) if matches!(step, Join::Take(_)) => {
          drop(line);
          spare.keep(Some(fresh));
          return done(wait, Ok(()));
        }
        Ok(()) => {
          *wait = Wait::Waiting(line.insert(fresh));
          self.parked.fetch_add(1, Ordering::SeqCst);
          return Poll::Pending;
        }
        Err(seen) => current = seen,
      }
    }
  }

  /// A reserve in the line, polled again: its verdict, read from its own state, or its waker kept
  /// up to date.
  ///
  /// A verdict is collected at once. A reserve still in line, polled inside its own channel's
  /// choice, is refused by name before it waits again, as one that joins the line is.
  fn again(
    &self,
    wait: &mut Wait,
    waker: &Waker,
    spare: &mut Spare,
    channel: usize,
  ) -> Poll<Result<(), Closed>> {
    let mut fresh: Option<Waker> = None;
    let mut checked = false;
    loop {
      {
        let mut line = lock(&self.line);
        let Wait::Waiting(key) = &*wait else {
          return Poll::Pending;
        };
        match line.state(key) {
          State::Queued if !checked => {
            // Checked with the lock let go: a panic there leaves the line as it is.
            drop(line);
            reentry::refuse(channel, "waited for room");
            checked = true;
            continue;
          }
          State::Queued => {
            if line.will_wake(key, waker) {
              spare.keep(fresh);
              return Poll::Pending;
            }
            if let Some(new) = fresh.take() {
              spare.keep(line.set_waker(key, new));
              return Poll::Pending;
            }
          }
          State::Notified | State::Closed => {
            let Wait::Waiting(key) = mem::replace(wait, Wait::Done) else {
              unreachable!("the reserve is in the line");
            };
            let (state, stored) = line.remove(key);
            self.parked.fetch_sub(1, Ordering::SeqCst);
            spare.keep(stored);
            spare.keep(fresh);
            return Poll::Ready(match state {
              State::Notified => Ok(()),
              _ => Err(Closed),
            });
          }
        }
      }
      // The waker changed. Its clone is user code: it is made with the lock let go, and with the
      // key still in `wait`, so a panic in it leaves the reserve to its future's drop.
      fresh = Some(waker.clone());
    }
  }

  /// A reserve dropped while it waits: it gives up its place, or passes on a grant it holds.
  pub(crate) fn leave(&self, remaining: &Remaining, wait: &mut Wait) {
    if !matches!(wait, Wait::Waiting(_)) {
      return;
    }
    let (stored, granted) = {
      let mut line = lock(&self.line);
      let Wait::Waiting(key) = mem::replace(wait, Wait::Done) else {
        unreachable!("the reserve is in the line");
      };
      let (state, stored) = line.remove(key);
      self.parked.fetch_sub(1, Ordering::SeqCst);
      let granted = match state {
        State::Queued => {
          let queued = line.queued();
          remaining.update(|current| left(current, queued));
          None
        }
        State::Notified => Self::pass_on(&mut line, remaining),
        State::Closed => None,
      };
      (stored, granted)
    };
    let mut wakes = Wakes::new();
    wakes.discard(stored);
    wakes.wake(granted);
    wakes.finish();
  }

  /// One unit back: free again where nobody is in line, else granted to the oldest reserve in
  /// line. Answers the granted reserve's waker, for the caller to wake once its own state changes
  /// are done.
  #[must_use = "the granted reserve's waker is to be woken"]
  pub(crate) fn release(&self, remaining: &Remaining) -> Option<Waker> {
    if remaining.try_give().is_ok() {
      return None;
    }
    let mut line = lock(&self.line);
    Self::pass_on(&mut line, remaining)
  }

  /// Under the lock: a unit goes to the oldest reserve in line, or to `free` where nobody is in
  /// line.
  fn pass_on(line: &mut Waiters, remaining: &Remaining) -> Option<Waker> {
    // Where to is decided by the line, which only this lock changes; the compare-and-swap loop
    // only absorbs a `take` or a `give`, which move `free` while `WAITING` is clear.
    let queued = line.queued();
    remaining.update(|current| match back(current, queued) {
      Back::Grant(next) | Back::Add(next) => next,
    });
    if queued > 0 {
      line.notify_oldest().flatten()
    } else {
      None
    }
  }

  /// Closes the lane's room: `CLOSED` set, every grant not taken up settled back, and every
  /// reserve in line closed, its wake owed, for [`dispatch`](Self::dispatch) once every state
  /// change of the close is done. A second close changes nothing.
  pub(crate) fn close(&self, remaining: &Remaining) {
    let mut line = lock(&self.line);
    if remaining.load() & CLOSED != 0 {
      return;
    }
    let granted = line.close_all();
    remaining.update(|current| closed(current, granted));
  }

  /// Wakes the reserves a close left owed, a chunk at a time, each chunk after the lock.
  pub(crate) fn dispatch(&self, wakes: &mut Wakes) {
    waiters::dispatch(&self.line, wakes);
  }
}

#[inline]
fn done(wait: &mut Wait, verdict: Result<(), Closed>) -> Poll<Result<(), Closed>> {
  *wait = Wait::Done;
  Poll::Ready(verdict)
}

#[cfg(test)]
mod tests;
