//! One lane's queue: a bounded, lock-free FIFO for many pushers and many poppers.
//!
//! It is Vyukov's bounded queue with two additions: the close mark is the top bit of the tail, and
//! each cell may carry a stamp beside its item.
//!
//! - **Positions.** `tail` counts every position ever claimed by a push and `head` every position
//!   ever claimed by a pop. Position `p` lives in cell `p & mask`. A position never reaches the
//!   mark bit: that is 2^63 pushes.
//! - **A cell's sequence** says whose turn it is at that cell: `p` while it is free for the push of
//!   position `p`; `p + 1` once that push has written it; `p + len` once the pop of `p` has read
//!   it, which frees it for the push of the next lap.
//! - **A push** claims its position first, with one compare-and-swap on the tail; that instant
//!   commits the item, and the position is its ordinal. The caller holds room for it (a permit),
//!   so the position's cell has been popped, or is being popped, and the push waits only for that
//!   pop's last instructions before it writes.
//! - **A pop** decides "empty" from the head and the tail alone. Where the tail is past the head, a
//!   pusher has committed that position, and the pop waits for its last instructions.
//! - **Orderings.** Every modification of the head and of the tail is a `SeqCst` read-modify-write,
//!   and every head or tail read that decides "empty" is a `SeqCst` load, head first. So each such
//!   load reads the last modification before it in the single total order `S`, and the two loads
//!   of one decision sit in `S` in program order. Pops move the head past `h` only after reading a
//!   tail above `h`, so head never passes tail at any point of `S`. A decision that reads `h` and
//!   then `t` with `h >= t` therefore saw head and tail meet at the tail read's place in `S`: the
//!   ring was empty at that instant. A `Relaxed` head read would not do: a `SeqCst` load keeps
//!   later accesses after it but not an earlier relaxed one before it, so the head read could
//!   return a head written by a pop after the tail read (head 5, tail 6; A reads tail 6; a push
//!   moves it to 7; a pop moves head to 6; A reads head 6 and answers empty over a ring that held
//!   {5}, then {5, 6}, then {6}). Neither loom 0.7 nor miri can show that reordering: neither
//!   lets a load read a write that comes later in its interleaving.
//! - **The close** sets the mark with one `fetch_or`. A push's compare-and-swap expects an unmarked
//!   tail, so no push is committed after the mark; one that loses to it gets its item back.

use core::{alloc::Layout, mem::MaybeUninit};

use crossbeam_utils::CachePadded;

use crate::sync::{AtomicU64, Ordering, Spin, UnsafeCell};

/// The close mark: the top bit of the tail.
const MARK: u64 = 1 << 63;

struct Cell<T> {
  sequence: AtomicU64,
  value: UnsafeCell<MaybeUninit<T>>,
}

/// One item as it left the ring.
#[derive(Debug)]
pub(crate) struct Popped<T, S> {
  pub(crate) ordinal: u64,
  pub(crate) item: T,
  /// `None` for a ring made without stamps.
  pub(crate) stamp: Option<S>,
}

pub(crate) struct Ring<T, S> {
  head: CachePadded<AtomicU64>,
  tail: CachePadded<AtomicU64>,
  cells: Box<[Cell<T>]>,
  /// One stamp beside each cell, written and read in the same windows as its item.
  stamps: Option<Box<[UnsafeCell<MaybeUninit<S>>]>>,
  /// The number of cells, a power of two, less one.
  mask: u64,
}

// SAFETY: a cell's item and stamp are reached only by the thread whose position owns the cell, in
// the window the cell's sequence grants it (see `push` and `pop`); every other field is atomic.
// Items and stamps move between threads, so both must be `Send`.
unsafe impl<T: Send, S: Send> Sync for Ring<T, S> {}

impl<T, S: Copy> Ring<T, S> {
  /// A ring that holds `capacity` items, with a stamp beside each where `stamped`. `None` where
  /// `capacity`'s power of two overflows, or its cells cannot be laid out.
  pub(crate) fn new(capacity: usize, stamped: bool) -> Option<Self> {
    let len = capacity.max(1).checked_next_power_of_two()?;
    let mask = u64::try_from(len).ok()?.checked_sub(1)?;
    if mask >= MARK {
      return None;
    }
    // Both layouts first, so that one past `isize::MAX` is refused before anything is allocated.
    // `try_reserve_exact` then refuses an allocation the system cannot give, so neither case
    // panics or aborts.
    Layout::array::<Cell<T>>(len).ok()?;
    if stamped {
      Layout::array::<UnsafeCell<MaybeUninit<S>>>(len).ok()?;
    }
    let mut cells = Vec::new();
    cells.try_reserve_exact(len).ok()?;
    cells.extend((0..=mask).map(|position| Cell {
      sequence: AtomicU64::new(position),
      value: UnsafeCell::new(MaybeUninit::uninit()),
    }));
    let stamps = if stamped {
      let mut stamps = Vec::new();
      stamps.try_reserve_exact(len).ok()?;
      stamps.extend((0..=mask).map(|_| UnsafeCell::new(MaybeUninit::uninit())));
      Some(stamps.into_boxed_slice())
    } else {
      None
    };
    Some(Self {
      head: CachePadded::new(AtomicU64::new(0)),
      tail: CachePadded::new(AtomicU64::new(0)),
      cells: cells.into_boxed_slice(),
      stamps,
      mask,
    })
  }

  /// Commits `item` at the next position, and answers the position: its ordinal. Where the ring is
  /// closed, nothing is committed and the item comes back.
  ///
  /// A stamped ring calls `stamp` once, just before the commit; an unstamped ring never calls it.
  ///
  /// The caller holds room for the item. Without it the push would still be sound, and would wait
  /// for a pop to free its cell.
  pub(crate) fn push(&self, item: T, stamp: impl FnOnce() -> S) -> Result<u64, T> {
    let stamp = self.stamps.is_some().then(stamp);
    let mut tail = self.tail.load(Ordering::Relaxed);
    let position = loop {
      if tail & MARK != 0 {
        return Err(item);
      }
      // `SeqCst`: a receiver going to sleep counts itself before it reads the tail, and this push
      // reads that count after it moves the tail. One of the two sees the other.
      match self
        .tail
        .compare_exchange_weak(tail, tail + 1, Ordering::SeqCst, Ordering::Relaxed)
      {
        Ok(_) => break tail,
        Err(seen) => tail = seen,
      }
    };

    let index = (position & self.mask) as usize;
    let cell = &self.cells[index];
    // The pop of the previous lap may still be reading the cell. `Acquire` pairs with its
    // `Release`, so its read happens before the write below.
    let spin = Spin::new();
    while cell.sequence.load(Ordering::Acquire) != position {
      spin.wait();
    }
    cell.value.with_mut(|value| {
      // SAFETY: the compare-and-swap above made this thread the one owner of `position`, and the
      // cell's sequence reads `position`: no pop of `position` can begin before the store below,
      // and the pop of the previous lap has finished with the cell. So nothing else reaches the
      // cell's value now, and writing a whole `MaybeUninit` over it is sound.
      unsafe { value.write(MaybeUninit::new(item)) }
    });
    if let (Some(stamps), Some(stamp)) = (&self.stamps, stamp) {
      stamps[index].with_mut(|slot| {
        // SAFETY: the same window as the item's: this thread owns `position`, and the stamp beside
        // the cell is reached only by the owner of the cell's position.
        unsafe { slot.write(MaybeUninit::new(stamp)) }
      });
    }
    // `Release` publishes the item and its stamp to the pop that reads `position + 1`.
    cell.sequence.store(position + 1, Ordering::Release);
    Ok(position)
  }

  /// Takes the oldest item, or answers `None` where the ring is empty: head and tail meet.
  ///
  /// A receive that goes on to wait counts itself before it looks again, and a push reads that
  /// count after it moves the tail, so a push that an empty answer here missed wakes it.
  pub(crate) fn pop(&self) -> Option<Popped<T, S>> {
    let spin = Spin::new();
    let mut head = self.head.load(Ordering::SeqCst);
    loop {
      // Head first, then tail, both `SeqCst`: `head == tail` here means head and tail met at the
      // tail read's place in `S` (see the module's orderings).
      let tail = self.tail.load(Ordering::SeqCst) & !MARK;
      if head >= tail {
        return None;
      }
      let index = (head & self.mask) as usize;
      let cell = &self.cells[index];
      let sequence = cell.sequence.load(Ordering::Acquire);
      if sequence == head + 1 {
        match self
          .head
          .compare_exchange_weak(head, head + 1, Ordering::SeqCst, Ordering::SeqCst)
        {
          Ok(_) => {
            let item = cell.value.with(|value| {
              // SAFETY: the compare-and-swap above made this thread the one owner of `head`, and
              // the cell's sequence read `head + 1` with `Acquire`, which pairs with the push's
              // `Release` after it wrote the value: the value is initialized, and no other thread
              // reaches it until the store below hands the cell to the next lap. Reading moves
              // the item out; the cell is treated as uninitialized from here on.
              unsafe { value.read().assume_init() }
            });
            let stamp = self.stamps.as_ref().map(|stamps| {
              stamps[index].with(|slot| {
                // SAFETY: the push of `head` wrote the stamp before its `Release`, in its window,
                // and this thread owns the window now. `S` is `Copy`, so the read moves nothing
                // out.
                unsafe { slot.read().assume_init() }
              })
            });
            cell.sequence.store(head + self.mask + 1, Ordering::Release);
            return Some(Popped {
              ordinal: head,
              item,
              stamp,
            });
          }
          Err(seen) => head = seen,
        }
      } else if sequence < head + 1 {
        // The tail is past `head`, so a push owns `head` and has not published it yet, or the pop
        // of the previous lap has not handed the cell over. Either is a few instructions away.
        spin.wait();
        head = self.head.load(Ordering::SeqCst);
      } else {
        // Another pop took `head`.
        head = self.head.load(Ordering::SeqCst);
      }
    }
  }

  /// Sets the close mark. True where this call set it.
  #[inline]
  pub(crate) fn close(&self) -> bool {
    self.tail.fetch_or(MARK, Ordering::SeqCst) & MARK == 0
  }

  #[inline]
  pub(crate) fn is_closed(&self) -> bool {
    self.tail.load(Ordering::SeqCst) & MARK != 0
  }

  /// Whether head and tail meet: no item is committed and untaken. Both reads are `SeqCst`, head
  /// first, as `pop`'s (see the module's orderings).
  #[inline]
  pub(crate) fn is_empty(&self) -> bool {
    let head = self.head.load(Ordering::SeqCst);
    head >= self.tail.load(Ordering::SeqCst) & !MARK
  }

  /// Closed and empty. Once true it stays true: no push is committed after the mark.
  ///
  /// The head read may be `Relaxed`: once the mark is set the tail no longer moves, and head never
  /// passes it, so a head read newer than the tail read only confirms the end.
  #[inline]
  pub(crate) fn is_ended(&self) -> bool {
    let head = self.head.load(Ordering::Relaxed);
    let tail = self.tail.load(Ordering::SeqCst);
    tail & MARK != 0 && head >= tail & !MARK
  }

  /// Every push ever committed. A reading for the snapshot: no decision rests on it.
  #[inline]
  pub(crate) fn committed(&self) -> u64 {
    self.tail.load(Ordering::SeqCst) & !MARK
  }

  /// Every pop ever claimed. A reading for the snapshot: no decision rests on it.
  #[inline]
  pub(crate) fn received(&self) -> u64 {
    self.head.load(Ordering::Relaxed)
  }
}

impl<T, S> Drop for Ring<T, S> {
  fn drop(&mut self) {
    let head = self.head.load(Ordering::Relaxed);
    let tail = self.tail.load(Ordering::Relaxed) & !MARK;
    for position in head..tail {
      let cell = &self.cells[(position & self.mask) as usize];
      if cell.sequence.load(Ordering::Relaxed) == position + 1 {
        cell.value.with_mut(|value| {
          // SAFETY: `&mut self`: no push or pop runs. A cell whose sequence reads `position + 1`
          // holds the item committed at `position`, written and never popped, so it is initialized
          // and dropped here once.
          unsafe { (*value).assume_init_drop() }
        });
      }
    }
  }
}

#[cfg(test)]
mod tests;
