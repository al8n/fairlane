use core::{
  fmt,
  future::Future,
  mem,
  pin::Pin,
  task::{Context, Poll},
};

use event_listener::EventListener;

use crate::{
  channel::Shared,
  error::{Closed, SendError, TryReserveError, TrySendError},
  lane::{Lane, Single},
  park::park_on,
  permit::{OwnedPermit, Permit},
  policy::{DefaultPolicy, Policy},
  remaining::Refusal,
  reserves::{Begin, Verdict},
  snapshot::Snapshot,
  sync::Arc,
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
    Reserve {
      shared: &self.shared,
      lane,
      wait: Wait::Start,
    }
  }

  /// Room in `lane` now, or why not: [`Full`](TryReserveError::Full) where it has none, or where
  /// an earlier reserve waits for it.
  pub fn try_reserve(&self, lane: L) -> Result<Permit<'_, T, L, Y>, TryReserveError> {
    match self.shared.lane(lane).remaining.try_take() {
      Ok(()) => Ok(Permit::new(&self.shared, lane)),
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
    match self.try_reserve(lane) {
      Ok(permit) => Ok(permit),
      Err(TryReserveError::Closed) => Err(Closed),
      Err(TryReserveError::Full) => park_on(self.reserve(lane)),
    }
  }

  /// [`reserve`](Self::reserve), for a permit that owns its channel: an [`OwnedPermit`].
  #[inline]
  pub fn reserve_owned(&self, lane: L) -> ReserveOwned<T, L, Y> {
    ReserveOwned {
      sender: Some(self.clone()),
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
    match self.try_reserve(lane) {
      Ok(permit) => permit
        .send(item)
        .map_err(|SendError(item)| TrySendError::Closed(item)),
      Err(TryReserveError::Full) => Err(TrySendError::Full(item)),
      Err(TryReserveError::Closed) => Err(TrySendError::Closed(item)),
    }
  }

  /// [`send`](Self::send), parking this thread while it waits.
  pub fn send_blocking(&self, lane: L, item: T) -> Result<(), SendError<T>> {
    match self.try_send(lane, item) {
      Ok(()) => Ok(()),
      Err(TrySendError::Closed(item)) => Err(SendError(item)),
      Err(TrySendError::Full(item)) => park_on(self.send(lane, item)),
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
      listener: None,
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

/// Where a reserve is.
pub(crate) enum Wait {
  Start,
  Waiting(EventListener),
  Done,
}

impl Wait {
  /// Polls a reserve of `lane` to its verdict.
  pub(crate) fn poll<T, L: Lane, Y: Policy<L>>(
    &mut self,
    shared: &Shared<T, L, Y>,
    lane: L,
    cx: &mut Context<'_>,
  ) -> Poll<Result<(), Closed>> {
    let state = shared.lane(lane);
    loop {
      match self {
        Wait::Start => match state.remaining.try_take() {
          Ok(()) => return self.done(Ok(())),
          Err(Refusal::Closed) => return self.done(Err(Closed)),
          Err(Refusal::Busy) => match state.reserves.begin(&state.remaining) {
            Begin::Granted => return self.done(Ok(())),
            Begin::Closed => return self.done(Err(Closed)),
            Begin::Waiting(listener) => *self = Wait::Waiting(listener),
          },
        },
        Wait::Waiting(listener) => {
          if Pin::new(listener).poll(cx).is_pending() {
            return Poll::Pending;
          }
          match state.reserves.woken(&state.remaining) {
            Verdict::Granted => return self.done(Ok(())),
            Verdict::Closed => return self.done(Err(Closed)),
            Verdict::Again(listener) => *self = Wait::Waiting(listener),
          }
        }
        // Polled after it resolved: the future's contract leaves this open, and it never wakes.
        Wait::Done => return Poll::Pending,
      }
    }
  }

  #[inline]
  fn done(&mut self, verdict: Result<(), Closed>) -> Poll<Result<(), Closed>> {
    *self = Wait::Done;
    Poll::Ready(verdict)
  }

  /// A reserve dropped: one that waits leaves the line, passing on a grant it holds.
  pub(crate) fn leave<T, L: Lane, Y: Policy<L>>(&mut self, shared: &Shared<T, L, Y>, lane: L) {
    if let Wait::Waiting(listener) = mem::replace(self, Wait::Done) {
      let state = shared.lane(lane);
      state.reserves.leave(&state.remaining, listener);
    }
  }
}

/// The future of [`Sender::reserve`]: a [`Permit`], or [`Closed`].
#[must_use = "a future does nothing unless polled"]
pub struct Reserve<'a, T, L: Lane, Y: Policy<L>> {
  shared: &'a Shared<T, L, Y>,
  lane: L,
  wait: Wait,
}

/// No field is pinned.
impl<T, L: Lane, Y: Policy<L>> Unpin for Reserve<'_, T, L, Y> {}

impl<'a, T, L: Lane, Y: Policy<L>> Future for Reserve<'a, T, L, Y> {
  type Output = Result<Permit<'a, T, L, Y>, Closed>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    this
      .wait
      .poll(this.shared, this.lane, cx)
      .map_ok(|()| Permit::new(this.shared, this.lane))
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for Reserve<'_, T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    self.wait.leave(self.shared, self.lane);
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
    match this.wait.poll(&sender.shared, this.lane, cx) {
      Poll::Pending => Poll::Pending,
      Poll::Ready(Err(closed)) => Poll::Ready(Err(closed)),
      Poll::Ready(Ok(())) => match this.sender.take() {
        Some(sender) => Poll::Ready(Ok(OwnedPermit::new(sender, this.lane))),
        None => Poll::Pending,
      },
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> Drop for ReserveOwned<T, L, Y> {
  #[inline]
  fn drop(&mut self) {
    if let Some(sender) = &self.sender {
      self.wait.leave(&sender.shared, self.lane);
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
    let verdict = match this
      .reserve
      .wait
      .poll(this.reserve.shared, this.reserve.lane, cx)
    {
      Poll::Pending => return Poll::Pending,
      Poll::Ready(verdict) => verdict,
    };
    let Some(item) = this.item.take() else {
      return Poll::Pending;
    };
    Poll::Ready(match verdict {
      Ok(()) => Permit::new(this.reserve.shared, this.reserve.lane).send(item),
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
  listener: Option<EventListener>,
}

impl<T, L: Lane, Y: Policy<L>> Future for WhenClosed<'_, T, L, Y> {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    let this = self.get_mut();
    loop {
      if this.shared.is_closed() {
        this.listener = None;
        return Poll::Ready(());
      }
      match &mut this.listener {
        None => this.listener = Some(this.shared.closed_event().listen()),
        Some(listener) => {
          if Pin::new(listener).poll(cx).is_pending() {
            return Poll::Pending;
          }
          this.listener = None;
        }
      }
    }
  }
}

impl<T, L: Lane, Y: Policy<L>> fmt::Debug for WhenClosed<'_, T, L, Y> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("WhenClosed").finish_non_exhaustive()
  }
}
