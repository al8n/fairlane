//! X2's checker: whether a concurrent history can be explained by the model.
//!
//! A history is a list of operations, each with the instant it was called and the instant it
//! returned, read from one counter that every thread increments. It is linearizable where some
//! order of the model's steps keeps real time (an operation that returned before another was
//! called comes before it) and gives every operation the answer it got.
//!
//! A close is not an operation here: it closes lane after lane, so it is not one step of the
//! model, and its laws are the loom models MC1 to MC4 and the unit laws.

use crate::{
  error::{TryReserveError, TrySendError},
  lane::Lane,
  options::Options,
  policy::Policy,
  quota::Quota,
  receiver::{Receiver, RecvManyError},
  sender::Sender,
};

use super::Model;

/// One operation of a history.
#[derive(Debug, Clone)]
pub(crate) enum Op<L: Lane> {
  Send { lane: L, item: u64 },
  Recv { quota: Quota<L> },
}

/// What it answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Answer<L> {
  Sent,
  Full,
  Closed,
  /// Each item as (lane, ordinal, item).
  Took(Vec<(L, u64, u64)>),
  Nothing,
  Ended,
  EmptyQuota,
}

#[derive(Debug, Clone)]
pub(crate) struct Event<L: Lane> {
  pub(crate) call: u64,
  pub(crate) ret: u64,
  pub(crate) op: Op<L>,
  pub(crate) answer: Answer<L>,
}

/// The model's answer to `op`, applied as one atomic step.
fn apply<L: Lane, Y: Policy<L, Stamp = ()>>(
  model: &mut Model<u64, L, ()>,
  op: &Op<L>,
  policy: &Y,
) -> Answer<L> {
  match op {
    Op::Send { lane, item } => match model.try_reserve(*lane) {
      Ok(permit) => match model.send(permit, *item, ()) {
        Ok(_) => Answer::Sent,
        Err(_) => Answer::Closed,
      },
      Err(TryReserveError::Full) => Answer::Full,
      Err(TryReserveError::Closed) => Answer::Closed,
    },
    Op::Recv { quota } => {
      if !quota.admits_any() {
        return Answer::EmptyQuota;
      }
      let taken = model.recv_many(quota, (), policy);
      if !taken.is_empty() {
        Answer::Took(
          taken
            .into_iter()
            .map(|taken| (taken.lane, taken.ordinal, taken.item))
            .collect(),
        )
      } else if model.ended(quota) {
        Answer::Ended
      } else {
        Answer::Nothing
      }
    }
  }
}

/// Whether `history` is linearizable against a fresh model of `options` under `policy`.
pub(crate) fn linearizable<L: Lane, Y: Policy<L, Stamp = ()>>(
  options: &Options<L>,
  policy: &Y,
  history: &[Event<L>],
) -> bool {
  let model = Model::new(options);
  let mut placed = vec![false; history.len()];
  search(model, policy, history, &mut placed, 0)
}

fn search<L: Lane, Y: Policy<L, Stamp = ()>>(
  model: Model<u64, L, ()>,
  policy: &Y,
  history: &[Event<L>],
  placed: &mut [bool],
  done: usize,
) -> bool {
  if done == history.len() {
    return true;
  }
  for next in 0..history.len() {
    if placed[next] {
      continue;
    }
    // Real time: nothing still to place may have returned before `next` was called.
    let ready = (0..history.len())
      .all(|other| placed[other] || other == next || history[other].ret > history[next].call);
    if !ready {
      continue;
    }
    let mut after = model.clone();
    if apply(&mut after, &history[next].op, policy) != history[next].answer {
      continue;
    }
    placed[next] = true;
    if search(after, policy, history, placed, done + 1) {
      return true;
    }
    placed[next] = false;
  }
  false
}

/// The channel's answer to `op`.
pub(crate) fn perform<L: Lane, Y: Policy<L>>(
  tx: &Sender<u64, L, Y>,
  rx: &Receiver<u64, L, Y>,
  op: &Op<L>,
) -> Answer<L> {
  match op {
    Op::Send { lane, item } => match tx.try_send(*lane, *item) {
      Ok(()) => Answer::Sent,
      Err(TrySendError::Full(_)) => Answer::Full,
      Err(TrySendError::Closed(_)) => Answer::Closed,
    },
    Op::Recv { quota } => {
      let mut batch = Vec::new();
      match rx.try_recv_many(&mut batch, quota) {
        Ok(0) => Answer::Nothing,
        Ok(_) => Answer::Took(
          batch
            .into_iter()
            .map(|received| (received.lane(), received.ordinal(), received.into_item()))
            .collect(),
        ),
        Err(RecvManyError::Ended) => Answer::Ended,
        Err(RecvManyError::EmptyQuota) => Answer::EmptyQuota,
      }
    }
  }
}
