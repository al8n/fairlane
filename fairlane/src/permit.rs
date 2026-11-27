use core::{fmt, mem::ManuallyDrop};

use crate::{
  channel::Shared, error::SendError, lane::Lane, policy::Policy, sender::Sender, wake::wake,
};

/// Room held in one lane, borrowed from its sender. Its `send` commits an item there and never
/// waits.
///
/// Dropping it unsent gives the room back, to the lane's oldest waiting reserve first.
#[must_use = "dropping a permit gives its room back unused"]
pub struct Permit<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  /// The lane's index, resolved once when the room was reserved: the commit and the refund go to
  /// this lane whatever `Lane::index` answers later.
  index: usize,
  /// The lane as the caller named it, for [`lane`](Self::lane).
  lane: L,
}

impl<'a, T, L: Lane, Y: Policy<L>> Permit<'a, T, L, Y> {
  #[inline]
  pub(crate) fn new(shared: &'a Shared<T, L, Y>, index: usize, lane: L) -> Self {
    Self {
      shared,
      index,
      lane,
    }
  }

  /// The lane the room is held in.
  #[inline]
  pub fn lane(&self) -> L {
    self.lane
  }

  /// Commits `item` to the permit's lane. It never waits. It fails only where the channel closed
  /// after the permit was granted, or where the lane has taken its 2^63 − 1 items and closed alone
  /// ([`Received::ordinal`](crate::Received::ordinal)); it then hands the item back and lets the
  /// room go.
  ///
  /// # Panics
  ///
  /// - Where the policy's `stamp` panics, before the commit, the room is given back, and then the
  ///   item is dropped by the unwind.
  /// - A `Waker` must not panic, block, or call into a channel (the crate's documentation,
  ///   "Wakers, policies and items"): a channel has no executor, and every wake runs on the
  ///   caller's thread. If one this send wakes panics anyway, the panic unwinds out of this call:
  ///   after the item is committed, or, where the lane has spent its positions and closes, with the
  ///   item it was handing back dropped.
  ///
  /// ```
  /// # use core::num::NonZeroUsize;
  /// use fairlane::{SendError, Single};
  ///
  /// # let (tx, rx) = fairlane::bounded::<&str>(NonZeroUsize::MIN)?;
  /// let permit = tx.try_reserve(Single)?;
  /// tx.close();
  /// match permit.send("late") {
  ///   Ok(()) => unreachable!("the channel closed first"),
  ///   Err(SendError(item)) => assert_eq!(item, "late"),
  /// }
  /// # drop(rx);
  /// # Ok::<(), Box<dyn core::error::Error>>(())
  /// ```
  #[inline]
  pub fn send(self, item: T) -> Result<(), SendError<T>> {
    let this = ManuallyDrop::new(self);
    this.shared.commit(this.index, item).map_err(SendError)
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for Permit<'_, T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    wake(self.shared.lane_at(self.index).release());
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for Permit<'_, T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Permit").field("lane", &self.lane).finish()
  }
}

/// Room held in one lane, owning its channel: it holds a [`Sender`], so while it is out the
/// channel stays open even where every other sender is gone.
#[must_use = "dropping a permit gives its room back unused"]
pub struct OwnedPermit<T, L: Lane, Y: Policy<L>> {
  /// `None` once sent.
  sender: Option<Sender<T, L, Y>>,
  /// As [`Permit`]'s: resolved once, when the room was reserved.
  index: usize,
  lane: L,
}

impl<T, L: Lane, Y: Policy<L>> OwnedPermit<T, L, Y> {
  #[inline]
  pub(crate) fn new(sender: Sender<T, L, Y>, index: usize, lane: L) -> Self {
    Self {
      sender: Some(sender),
      index,
      lane,
    }
  }

  /// The lane the room is held in.
  #[inline]
  pub fn lane(&self) -> L {
    self.lane
  }

  /// [`Permit::send`], for an owned permit. Its sender goes once the item is committed. Where it
  /// was the last sender, that closes the channel, and a waker the close runs that panics unwinds
  /// out of this call with the item already committed.
  ///
  /// # Panics
  ///
  /// As [`Permit::send`].
  pub fn send(mut self, item: T) -> Result<(), SendError<T>> {
    match self.sender.take() {
      Some(sender) => Permit::new(sender.shared(), self.index, self.lane).send(item),
      None => Err(SendError(item)),
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for OwnedPermit<T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    if let Some(sender) = self.sender.take() {
      wake(sender.shared().lane_at(self.index).release());
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for OwnedPermit<T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("OwnedPermit")
      .field("lane", &self.lane)
      .finish()
  }
}
