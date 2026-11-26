use core::{
  fmt,
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};

use crate::{
  channel::Shared,
  error::{Closed, SendError, TryReserveError, TrySendError},
  lane::{Lane, Single},
  park::park_on,
  permit::{OwnedPermit, Permit},
  policy::{DefaultPolicy, Policy},
  remaining::Refusal,
  reserves::Wait,
  snapshot::Snapshot,
  sync::Arc,
  waiters::Key,
  wake::Spare,
};

/// The sending half of a channel. Clones share the channel; the last one dropped closes it, and
/// what is queued can still be received.
///
/// Every door takes the lane, also on a channel of one lane: `tx.send(Single, item)`.
pub struct Sender<T, L: Lane = Single, Y: Policy<L> = DefaultPolicy<L>> {
  shared: Arc<Shared<T, L, Y>>,
}

impl<T, L: Lane, Y: Policy<L>> Sender<T, L, Y> {
  #[inline]
  pub(crate) fn new(shared: Arc<Shared<T, L, Y>>) -> Self {
    Self { shared }
  }

  #[inline]
  pub(crate) fn shared(&self) -> &Shared<T, L, Y> {
    &self.shared
  }

  /// Room in `lane`, waiting for it where there is none: a [`Permit`].
  ///
  /// Waiting reserves are granted in the order they began to wait, and a reserve that does not
  /// wait never passes one that does. Dropping the future while it waits gives a grant it already
  /// holds to the next waiter.
  ///
  /// It answers [`Closed`] once the channel is closed. A reserve that was granted room, and had
  /// not resumed to take it when the channel closed, answers [`Closed`] too: a close settles every
  /// grant not yet taken up.
  #[inline]
  pub fn reserve(&self, lane: L) -> Reserve<'_, T, L, Y> {
    self.reserve_at(self.shared.resolve(lane), lane)
  }

  #[inline]
  fn reserve_at(&self, index: usize, lane: L) -> Reserve<'_, T, L, Y> {
    Reserve {
      shared: &self.shared,
      index,
      lane,
      wait: Wait::Start,
    }
  }

  /// Room in `lane` now, or why not: [`Full`](TryReserveError::Full) where it has none, or where
  /// an earlier reserve waits for it.
  pub fn try_reserve(&self, lane: L) -> Result<Permit<'_, T, L, Y>, TryReserveError> {
    self.try_reserve_at(self.shared.resolve(lane), lane)
  }

  fn try_reserve_at(&self, index: usize, lane: L) -> Result<Permit<'_, T, L, Y>, TryReserveError> {
    match self.shared.lane_at(index).remaining.try_take() {
      Ok(()) => Ok(Permit::new(&self.shared, index, lane)),
      Err(Refusal::Busy) => Err(TryReserveError::Full),
      Err(Refusal::Closed) => Err(TryReserveError::Closed),
    }
  }

  /// [`reserve`](Self::reserve), parking this thread while it waits.
  ///
  /// ```
  /// # use core::num::NonZeroUsize;
  /// use fairlane::{Closed, Single};
  ///
  /// fn produce(tx: &fairlane::Sender<u32>) {
  ///   loop {
  ///     let permit = match tx.reserve_blocking(Single) {
  ///       Ok(permit) => permit,
  ///       Err(Closed) => return,
  ///     };
  ///     if permit.send(1).is_err() {
  ///       return;
  ///     }
  ///   }
  /// }
  ///
  /// let (tx, rx) = fairlane::bounded::<u32>(NonZeroUsize::MIN)?;
  /// drop(rx);
  /// produce(&tx);
  /// # Ok::<(), fairlane::Malformed>(())
  /// ```
  pub fn reserve_blocking(&self, lane: L) -> Result<Permit<'_, T, L, Y>, Closed> {
    let index = self.shared.resolve(lane);
    match self.try_reserve_at(index, lane) {
      Ok(permit) => Ok(permit),
      Err(TryReserveError::Closed) => Err(Closed),
      Err(TryReserveError::Full) => park_on(self.reserve_at(index, lane)),
    }
  }

  /// [`reserve`](Self::reserve), for a permit that owns its channel: an [`OwnedPermit`].
  #[inline]
  pub fn reserve_owned(&self, lane: L) -> ReserveOwned<T, L, Y> {
    let index = self.shared.resolve(lane);
    ReserveOwned {
      sender: Some(self.clone()),
      index,
      lane,
      wait: Wait::Start,
    }
  }

  /// Reserves room in `lane`, waiting for it, and commits `item` there. The item comes back in
  /// the error where the channel is closed.
  #[inline]
  pub fn send(&self, lane: L, item: T) -> Sending<'_, T, L, Y> {
    Sending {
      reserve: self.reserve(lane),
      item: Some(item),
    }
  }

  /// Commits `item` to `lane` now, or hands it back: `Full` where the lane has no room, or where
  /// an earlier reserve waits for it.
  pub fn try_send(&self, lane: L, item: T) -> Result<(), TrySendError<T>> {
    self.try_send_at(self.shared.resolve(lane), lane, item)
  }

  fn try_send_at(&self, index: usize, lane: L, item: T) -> Result<(), TrySendError<T>> {
    match self.try_reserve_at(index, lane) {
      Ok(permit) => permit
        .send(item)
        .map_err(|SendError(item)| TrySendError::Closed(item)),
      Err(TryReserveError::Full) => Err(TrySendError::Full(item)),
      Err(TryReserveError::Closed) => Err(TrySendError::Closed(item)),
    }
  }

  /// [`send`](Self::send), parking this thread while it waits.
  pub fn send_blocking(&self, lane: L, item: T) -> Result<(), SendError<T>> {
    let index = self.shared.resolve(lane);
    match self.try_send_at(index, lane, item) {
      Ok(()) => Ok(()),
      Err(TrySendError::Closed(item)) => Err(SendError(item)),
      Err(TrySendError::Full(item)) => park_on(Sending {
        reserve: self.reserve_at(index, lane),
        item: Some(item),
      }),
    }
  }

  /// Closes the channel: no reserve is granted and no permit commits from now on, in any lane.
  /// Every waiting reserve resolves [`Closed`], and what is queued can still be received.
  ///
  /// The lanes close one after another, in index order, so a commit to a later lane can land
  /// while an earlier lane is closed; once this call returns, nothing commits anywhere. True for
  /// the one call that closed the channel.
  #[inline]
  pub fn close(&self) -> bool {
    self.shared.close()
  }

  /// Whether the channel is closed.
  #[inline]
  pub fn is_closed(&self) -> bool {
    self.shared.is_closed()
  }

  /// Resolves once the channel is closed.
  #[inline]
  pub fn closed(&self) -> WhenClosed<'_, T, L, Y> {
    WhenClosed {
      shared: &self.shared,
      key: None,
    }
  }

  /// The channel's policy. Its numbers are live: the next choice reads them.
  #[inline]
  pub fn policy(&self) -> &Y {
    self.shared.policy()
  }

  /// A reading of the channel's queues.
  #[inline]
  pub fn snapshot(&self) -> Snapshot<L> {
    self.shared.snapshot()
  }
}

impl<T, L: Lane, Y: Policy<L>> Clone for Sender<T, L, Y> {
  #[inline]
  fn clone(&self) -> Self {
    self.shared.add_sender();
    Self {
      shared: self.shared.clone(),
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for Sender<T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    self.shared.drop_sender();
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for Sender<T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Sender")
      .field("closed", &self.is_closed())
      .finish_non_exhaustive()
  }
}

impl<T, L: Lane, Y: Policy<L>> Shared<T, L, Y> {
  /// Polls the reserve at `wait`, of lane `index`, to its verdict. Wakers it no longer needs go to
  /// `spare`, which the caller drops once it owns the verdict.
  #[inline]
  fn poll_reserve(
    &self,
    index: usize,
    wait: &mut Wait,
    cx: &mut Context<'_>,
    spare: &mut Spare,
  ) -> Poll<Result<(), Closed>> {
    let state = self.lane_at(index);
    state
      .reserves
      .poll(&state.remaining, wait, cx.waker(), spare, self.address())
  }

  /// A reserve dropped: one that waits leaves the line, passing on a grant it holds.
  #[inline]
  fn leave_reserve(&self, index: usize, wait: &mut Wait) {
    let state = self.lane_at(index);
    state.reserves.leave(&state.remaining, wait);
  }
}

/// The future of [`Sender::reserve`]: a [`Permit`], or [`Closed`].
#[must_use = "a future does nothing unless polled"]
pub struct Reserve<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  /// The lane's index, resolved once by the door: every poll, the grant and a cancel name it.
  index: usize,
  lane: L,
  wait: Wait,
}

/// No field is pinned.
impl<T, L: Lane, Y: Policy<L>> Unpin for Reserve<'_, T, L, Y> {}

impl<'a, T, L: Lane, Y: Policy<L>> Future for Reserve<'a, T, L, Y> {
  type Output = Result<Permit<'a, T, L, Y>, Closed>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    let mut spare = Spare::new();
    let output = this
      .shared
      .poll_reserve(this.index, &mut this.wait, cx, &mut spare)
      .map_ok(|()| Permit::new(this.shared, this.index, this.lane));
    // The permit exists before a spare waker's drop runs, so a panic there gives its room back.
    drop(spare);
    output
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for Reserve<'_, T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    self.shared.leave_reserve(self.index, &mut self.wait);
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for Reserve<'_, T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Reserve")
      .field("lane", &self.lane)
      .finish_non_exhaustive()
  }
}

/// The future of [`Sender::reserve_owned`]: an [`OwnedPermit`], or [`Closed`].
#[must_use = "a future does nothing unless polled"]
pub struct ReserveOwned<T, L: Lane, Y: Policy<L>> {
  /// `None` once the permit took it.
  sender: Option<Sender<T, L, Y>>,
  /// As [`Reserve`]'s: resolved once by the door.
  index: usize,
  lane: L,
  wait: Wait,
}

/// No field is pinned.
impl<T, L: Lane, Y: Policy<L>> Unpin for ReserveOwned<T, L, Y> {}

impl<T, L: Lane, Y: Policy<L>> Future for ReserveOwned<T, L, Y> {
  type Output = Result<OwnedPermit<T, L, Y>, Closed>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    let Some(sender) = &this.sender else {
      return Poll::Pending;
    };
    let mut spare = Spare::new();
    let output = match sender
      .shared
      .poll_reserve(this.index, &mut this.wait, cx, &mut spare)
    {
      Poll::Pending => Poll::Pending,
      Poll::Ready(Err(closed)) => Poll::Ready(Err(closed)),
      Poll::Ready(Ok(())) => match this.sender.take() {
        Some(sender) => Poll::Ready(Ok(OwnedPermit::new(sender, this.index, this.lane))),
        None => Poll::Pending,
      },
    };
    // As `Reserve`'s: the permit exists before a spare waker's drop runs.
    drop(spare);
    output
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for ReserveOwned<T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    if let Some(sender) = &self.sender {
      sender.shared.leave_reserve(self.index, &mut self.wait);
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for ReserveOwned<T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ReserveOwned")
      .field("lane", &self.lane)
      .finish_non_exhaustive()
  }
}

/// The future of [`Sender::send`].
#[must_use = "a future does nothing unless polled"]
pub struct Sending<'a, T, L: Lane, Y: Policy<L>> {
  reserve: Reserve<'a, T, L, Y>,
  /// `None` once sent, or handed back.
  item: Option<T>,
}

/// The item is moved, never pinned.
impl<T, L: Lane, Y: Policy<L>> Unpin for Sending<'_, T, L, Y> {}

impl<T, L: Lane, Y: Policy<L>> Future for Sending<'_, T, L, Y> {
  type Output = Result<(), SendError<T>>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    let reserve = &mut this.reserve;
    let mut spare = Spare::new();
    let verdict =
      match reserve
        .shared
        .poll_reserve(reserve.index, &mut reserve.wait, cx, &mut spare)
      {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(verdict) => verdict,
      };
    let permit = verdict.map(|()| Permit::new(reserve.shared, reserve.index, reserve.lane));
    // As `Reserve`'s: the permit exists before a spare waker's drop runs, and the item is still
    // the future's.
    drop(spare);
    let Some(item) = this.item.take() else {
      return Poll::Pending;
    };
    Poll::Ready(match permit {
      Ok(permit) => permit.send(item),
      Err(Closed) => Err(SendError(item)),
    })
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for Sending<'_, T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Sending")
      .field("lane", &self.reserve.lane)
      .finish_non_exhaustive()
  }
}

/// The future of [`Sender::closed`]: resolves once the channel is closed.
#[must_use = "a future does nothing unless polled"]
pub struct WhenClosed<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  /// Its place in the closers' line, once it waits.
  key: Option<Key>,
}

impl<T, L: Lane, Y: Policy<L>> Future for WhenClosed<'_, T, L, Y> {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    let this = self.get_mut();
    this.shared.poll_closed(&mut this.key, cx.waker())
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for WhenClosed<'_, T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    self.shared.leave_closed(&mut self.key);
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for WhenClosed<'_, T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("WhenClosed").finish_non_exhaustive()
  }
}
