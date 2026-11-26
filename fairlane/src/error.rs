use core::fmt;

/// Nothing more is taken in: the channel is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Closed(pub(crate) ());

impl fmt::Display for Closed {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("the channel is closed")
  }
}

impl core::error::Error for Closed {}

/// Nothing the receive admits is left, and none can come: every lane it admits is closed and
/// empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ended(pub(crate) ());

impl fmt::Display for Ended {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("every lane the receive admits is closed and empty")
  }
}

impl core::error::Error for Ended {}

/// A send that failed because the channel is closed, with the item it could not commit.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SendError<T>(pub(crate) T);

impl<T> SendError<T> {
  /// The item that was not sent.
  #[inline]
  pub fn into_inner(self) -> T {
    self.0
  }
}

impl<T> fmt::Debug for SendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("SendError").finish_non_exhaustive()
  }
}

impl<T> fmt::Display for SendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("sending on a closed channel")
  }
}

impl<T> core::error::Error for SendError<T> {}

/// Why a reserve that never waits took no room.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TryReserveError {
  /// The lane has no room now, or an earlier reserve is waiting for it.
  Full,
  /// The channel is closed.
  Closed,
}

impl fmt::Display for TryReserveError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Full => f.write_str("the lane is full"),
      Self::Closed => f.write_str("the channel is closed"),
    }
  }
}

impl core::error::Error for TryReserveError {}

/// Why a send that never waits committed nothing, with the item.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrySendError<T> {
  /// The lane has no room now, or an earlier reserve is waiting for it.
  Full(T),
  /// The channel is closed.
  Closed(T),
}

impl<T> fmt::Debug for TrySendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Full(_) => f.write_str("Full(..)"),
      Self::Closed(_) => f.write_str("Closed(..)"),
    }
  }
}

impl<T> fmt::Display for TrySendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Full(_) => f.write_str("the lane is full"),
      Self::Closed(_) => f.write_str("the channel is closed"),
    }
  }
}

impl<T> core::error::Error for TrySendError<T> {}
