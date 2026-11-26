use core::{fmt, mem::ManuallyDrop};

use crate::{channel::Shared, error::SendError, lane::Lane, policy::Policy, sender::Sender};

/// Room held in one lane, borrowed from its sender. Its `send` commits an item there and never
/// waits.
///
/// Dropping it unsent gives the room back, to the lane's oldest waiting reserve first.
#[must_use = "dropping a permit gives its room back unused"]
pub struct Permit<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  lane: L,
}

impl<'a, T, L: Lane, Y: Policy<L>> Permit<'a, T, L, Y> {
  #[inline]
  pub(crate) fn new(shared: &'a Shared<T, L, Y>, lane: L) -> Self {
    Self { shared, lane }
  }

  /// The lane the room is held in.
  #[inline]
  pub fn lane(&self) -> L {
    self.lane
  }

  /// Commits `item` to the permit's lane. It never waits. It fails only where the channel closed
  /// after the permit was granted, and then hands the item back and lets the room go.
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
    this.shared.commit(this.lane, item).map_err(SendError)
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for Permit<'_, T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    self.shared.lane(self.lane).release();
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
  lane: L,
}

impl<T, L: Lane, Y: Policy<L>> OwnedPermit<T, L, Y> {
  #[inline]
  pub(crate) fn new(sender: Sender<T, L, Y>, lane: L) -> Self {
    Self {
      sender: Some(sender),
      lane,
    }
  }

  /// The lane the room is held in.
  #[inline]
  pub fn lane(&self) -> L {
    self.lane
  }

  /// [`Permit::send`], for an owned permit. Its sender goes once the item is committed.
  pub fn send(mut self, item: T) -> Result<(), SendError<T>> {
    match self.sender.take() {
      Some(sender) => Permit::new(sender.shared(), self.lane).send(item),
      None => Err(SendError(item)),
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for OwnedPermit<T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    if let Some(sender) = self.sender.take() {
      sender.shared().lane(self.lane).release();
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
