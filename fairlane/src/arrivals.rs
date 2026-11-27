//! A lane's waiting receivers: a line of them under the lane's arrivals lock, and a count.
//!
//! - **A receive going to sleep** joins the line of every lane its quota admits, and is counted on
//!   each, under each lane's lock. Then it looks again, in full.
//! - **A notice belongs to a receive,** not to its place in one lane's line. Each pending receive
//!   has one identity, a [`Receive`], shared by its places in every line: it holds at most one
//!   notice, of one lane, and two receives that share a task waker are still two.
//! - **A commit** moves the tail, then reads the count. Only where it is not zero does it take the
//!   lock and give its lane's notice: to the oldest receive in line that holds none, whatever
//!   notices are outstanding, so a notified receive whose task is busy elsewhere holds back no
//!   other. Where every receive in line holds another lane's notice, the oldest takes this lane's,
//!   and its own is given again in its lane ([`Shared::notify`](crate::channel)). The count keeps a
//!   commit that nobody waits for off the lock.
//! - **No wakeup is lost.** The count's changes and the tail's moves are `SeqCst` read-modify-writes,
//!   and both reads are `SeqCst` loads. In the single total order `S`, if the receive's look at the
//!   tail comes before the commit's compare-and-swap, the commit's count load comes after the
//!   receive's increment and reads it, or a later value that still counts the receive while it is
//!   in line. That increment happens-before the load, so the commit's lock comes after the
//!   receive's critical section and finds the receive in line, or the receive already left it,
//!   awake. Otherwise the receive's look comes after the compare-and-swap, and sees the item.
//! - **A notified receive that is dropped** gives its notice again in its lane, by the same rule.
//!   One that woke and goes on to take spends it: the receive that takes the items wakes the next
//!   receive where items are left (the chain), by the same rule.

use core::task::Waker;

use crate::{
  sync::{Arc, AtomicUsize, Mutex, Ordering, lock, pair_fence},
  waiters::{self, Key, State, Waiters},
  wake::Wakes,
};

/// A pending receive's notification identity, shared by its places in every lane's line.
///
/// One word, read and changed only by compare-and-swap, under whichever lane's lock is giving a
/// notice: the lanes' locks differ, so one word is what keeps a receive to one notice.
pub(crate) struct Receive {
  /// [`FREE`], a notice's lane plus one, or [`GONE`].
  held: AtomicUsize,
}

/// It holds no notice.
const FREE: usize = 0;
/// It is leaving, and takes no notice.
const GONE: usize = usize::MAX;

impl Receive {
  pub(crate) fn new() -> Self {
    Self {
      held: AtomicUsize::new(FREE),
    }
  }

  /// Leaves for good, before its places do: no line gives it a notice from now on. Answers the
  /// lane whose notice it held, for its leave to give again.
  pub(crate) fn leave(&self) -> Option<usize> {
    match self.held.swap(GONE, Ordering::SeqCst) {
      FREE | GONE => None,
      held => Some(held - 1),
    }
  }
}

/// What one lane's line did with its notice.
pub(crate) enum Notice {
  /// A receive that held none took it: its waker, to wake.
  Fresh(Option<Waker>),
  /// Every receive in line held another lane's notice: the oldest took this lane's, and the lane
  /// it names is to give its own again. No waker: that receive was woken already.
  Moved(usize),
  /// Nobody in line.
  Nobody,
}

pub(crate) struct Arrivals {
  count: AtomicUsize,
  line: Mutex<Waiters<Arc<Receive>>>,
}

impl Arrivals {
  pub(crate) fn new() -> Self {
    Self {
      count: AtomicUsize::new(0),
      line: Mutex::new(Waiters::new()),
    }
  }

  /// Takes the line's lock once, at the channel's construction: no later call is its first use.
  pub(crate) fn warm(&self) {
    drop(lock(&self.line));
  }

  /// Joins the line with `waker`, a clone made before the lock, and with the identity of its
  /// receive, and counts itself. The receive looks again after this.
  pub(crate) fn listen(&self, waker: Waker, receive: Arc<Receive>) -> Key {
    let key = {
      let mut line = lock(&self.line);
      line.make_room();
      let key = line.insert(waker, receive);
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

  /// Leaves the line, and answers the waker its place held, to be dropped by the caller once the
  /// receive has left every line. A notice the receive held is its identity's, given again by its
  /// leave ([`Receive::leave`]), not by this line.
  #[must_use = "the stored waker is to be dropped after the lock"]
  pub(crate) fn leave(&self, key: Key) -> Option<Waker> {
    let mut line = lock(&self.line);
    let (_, stored) = line.remove(key);
    self.count.fetch_sub(1, Ordering::SeqCst);
    stored
  }

  /// Gives lane `lane`'s notice, this line's lane: to the oldest receive in line that holds none;
  /// else, where every receive in line holds another lane's notice, the oldest takes this one, and
  /// its own lane is to give that one again ([`Notice::Moved`]). Under this line's lock only; no
  /// waker runs here.
  #[must_use = "a fresh notice's waker is to be woken"]
  pub(crate) fn notify(&self, lane: usize) -> Notice {
    pair_fence();
    if self.count.load(Ordering::SeqCst) == 0 {
      return Notice::Nobody;
    }
    let mine = lane + 1;
    let mut line = lock(&self.line);
    loop {
      let mut oldest_held = None;
      let mut at = line.first_queued();
      while let Some(index) = at {
        match line.tag_at(index).held.compare_exchange(
          FREE,
          mine,
          Ordering::SeqCst,
          Ordering::SeqCst,
        ) {
          Ok(_) => return Notice::Fresh(line.notify_at(index, true)),
          Err(held) if held != GONE && held != mine && oldest_held.is_none() => {
            oldest_held = Some((index, held));
          }
          Err(_) => {}
        }
        at = line.next_queued(index);
      }
      let Some((index, held)) = oldest_held else {
        return Notice::Nobody;
      };
      // Every receive in line holds a notice: the oldest takes this lane's, and the lane whose
      // notice it held gives that one again. Where its notice moved meanwhile, the line is read
      // again.
      if line
        .tag_at(index)
        .held
        .compare_exchange(held, mine, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
      {
        let _ = line.notify_at(index, false);
        return Notice::Moved(held - 1);
      }
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
