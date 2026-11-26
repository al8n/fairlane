//! The one driver of the blocking doors: the awaited door's future, polled on this thread, which
//! parks between polls. There is no second implementation of any wait.

use core::{
  future::Future,
  pin::pin,
  task::{Context, Poll, Waker},
};
use std::{sync::Arc, task::Wake};

use crate::sync::thread;

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
