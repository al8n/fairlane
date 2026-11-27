//! A lane's waiting receivers: a line of them under the lane's arrivals lock, and a count.
//!
//! - **A receive going to sleep** joins the line of every lane its quota admits, and is counted on
//!   each, under each lane's lock. Then it looks again, in full.
//! - **A commit** moves the tail, then reads the count. Only where it is not zero does it take the
//!   lock and notify the oldest receiver in line, unless one is notified already and not yet awake:
//!   that one counts as the one, so a burst of commits wakes no herd. The count keeps a commit that
//!   nobody waits for off the lock.
//! - **No wakeup is lost.** The count's changes and the tail's moves are `SeqCst` read-modify-writes,
//!   and both reads are `SeqCst` loads. In the single total order `S`, if the receive's look at the
//!   tail comes before the commit's compare-and-swap, the commit's count load comes after the
//!   receive's increment and reads it, or a later value that still counts the receive while it is
//!   in line. That increment happens-before the load, so the commit's lock comes after the
//!   receive's critical section and finds the receive in line, or the receive already left it,
//!   awake. Otherwise the receive's look comes after the compare-and-swap, and sees the item.
//! - **A notified receiver that is dropped** passes its notification to the next receiver in line.
//!   One that woke and goes on to take spends it: the receive that takes the items wakes the next
//!   receiver where items are left (the chain).

use core::task::Waker;

use crate::{
  sync::{AtomicUsize, Mutex, Ordering, lock, pair_fence},
  waiters::{self, Key, State, Waiters},
  wake::Wakes,
};

pub(crate) struct Arrivals {
  count: AtomicUsize,
  line: Mutex<Waiters>,
}

impl Arrivals {
  pub(crate) fn new() -> Self {
    Self {
      count: AtomicUsize::new(0),
      line: Mutex::new(Waiters::new()),
    }
  }

  /// Joins the line with `waker`, a clone made before the lock, and counts itself. The receive
  /// looks again after this.
  pub(crate) fn listen(&self, waker: Waker) -> Key {
    let key = {
      let mut line = lock(&self.line);
      if let Err(error) = line.make_room() {
        drop(line);
        drop(waker);
        waiters::full(error);
      }
      let key = line.insert(waker);
      self.count.fetch_add(1, Ordering::SeqCst);
      key
    };
    pair_fence();
    key
  }

  /// Whether the receiver of `key` was notified. Where not, its waker is kept up to date; a waker
  /// it no longer needs is dropped through `wakes`, after the lock.
  pub(crate) fn woken(&self, key: &Key, waker: &Waker, wakes: &mut Wakes) -> bool {
    let mut fresh: Option<Waker> = None;
    loop {
      {
        let mut line = lock(&self.line);
        match line.state(key) {
          State::Queued => {
            if line.will_wake(key, waker) {
              drop(line);
              wakes.discard(fresh);
              return false;
            }
            if let Some(new) = fresh.take() {
              let replaced = line.set_waker(key, new);
              drop(line);
              wakes.discard(replaced);
              return false;
            }
          }
          State::Notified | State::Closed => {
            drop(line);
            wakes.discard(fresh);
            return true;
          }
        }
      }
      // The waker changed. Its clone is user code, made with the lock let go; the state is read
      // again once the clone is in hand, so a notification in between is not missed.
      fresh = Some(waker.clone());
    }
  }

  /// Leaves the line. Where the receiver had been notified and is being dropped (`spend` false),
  /// the notification goes to the next receiver in line; where it woke and goes on to take
  /// (`spend` true), it is spent.
  ///
  /// Answers the waker the receiver held, to be dropped, and the waker of the receiver its
  /// notification went to, to be woken: by the caller, once the receive has left every line.
  #[must_use = "the wakers are to be dropped and woken"]
  pub(crate) fn leave(&self, key: Key, spend: bool) -> (Option<Waker>, Option<Waker>) {
    let mut line = lock(&self.line);
    let (state, stored) = line.remove(key);
    self.count.fetch_sub(1, Ordering::SeqCst);
    let passed = if state == State::Notified && !spend && line.notified() == 0 {
      line.notify_oldest().flatten()
    } else {
      None
    };
    (stored, passed)
  }

  /// Notifies one waiting receiver, where any waits and none is notified and not yet awake. Answers
  /// its waker, for the caller to wake.
  #[must_use = "the notified receiver's waker is to be woken"]
  pub(crate) fn wake_one(&self) -> Option<Waker> {
    pair_fence();
    if self.count.load(Ordering::SeqCst) == 0 {
      return None;
    }
    let mut line = lock(&self.line);
    if line.notified() == 0 {
      line.notify_oldest().flatten()
    } else {
      None
    }
  }

  /// Notifies every waiting receiver, at a close, each wake owed, for
  /// [`dispatch`](Self::dispatch) once every state change of the close is done.
  pub(crate) fn close(&self) {
    lock(&self.line).notify_all();
  }

  /// Wakes the receivers a close left owed, a chunk at a time, each chunk after the lock.
  pub(crate) fn dispatch(&self, wakes: &mut Wakes) {
    waiters::dispatch(&self.line, wakes);
  }

  /// Receivers counted as waiting.
  #[inline]
  pub(crate) fn waiting(&self) -> usize {
    self.count.load(Ordering::SeqCst)
  }
}
