//! The one driver of the blocking doors: the awaited door's future, polled on this thread, which
//! parks between polls. There is no second implementation of any wait.

#[cfg(not(loom))]
pub(crate) use self::std_driver::park_on;

#[cfg(not(loom))]
mod std_driver {
  use core::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
  };
  use std::{sync::Arc, task::Wake, thread};

  /// The blocking calls' waker. On some targets (Windows 7's keyed events, the pthread parker) an
  /// unpark waits for the parked thread to reach its wait. That thread holds no channel lock on
  /// its way there, since every queue guard is let go before the poll returns `Pending`, so an
  /// unpark can delay the wakes after it, never stop them.
  struct Unparker(thread::Thread);

  impl Wake for Unparker {
    fn wake(self: Arc<Self>) {
      self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
      self.0.unpark();
    }
  }

  /// Polls `future` to its end on this thread, parking whenever it is pending.
  pub(crate) fn park_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
      if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
        return output;
      }
      thread::park();
    }
  }
}

/// Under loom, loom's own driver. loom 0.7's `thread::unpark` ends a wait on a loom `Mutex` while
/// the mutex is still held (`rt/thread.rs`, `set_unparked`), where std's `unpark` leaves a mutex
/// wait alone: a waker run by another thread while this thread waits for one of the crate's locks
/// would end that wait. `block_on`'s waker is a loom `Notify`, which ends only a wait on itself.
#[cfg(loom)]
pub(crate) fn park_on<F: core::future::Future>(future: F) -> F::Output {
  loom::future::block_on(future)
}
