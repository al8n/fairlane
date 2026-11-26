//! X2's checker: whether a concurrent history can be explained by the model's steps.
//!
//! A history is a list of operations, each with the instant it was called and the instant it
//! returned, read from one counter that every thread increments. It is linearizable where some
//! order of the model's steps (design I.4 and I.5) keeps real time and gives every operation the
//! answer it got:
//!
//! - a `try_send` is a reserve step, which ends it where it reads `Full`, and then a commit step;
//! - a `try_recv_many` is a run of choices, each taking one item for the batch so far, closed by
//!   one choice that takes nothing. No other receive's choice falls inside a run (D1). Each item's
//!   room comes back by a step of its own, after its choice and before the receive returns: the
//!   channel gives room back after the pop is complete;
//! - every step of an operation lies between its call and its return, and an operation that
//!   returned before another was called has placed all of its steps before that one's first.
//!
//! A close is not an operation here: it closes lane after lane, and its laws are the loom models
//! MC1 to MC4 and the unit laws.

use core::hash::Hash;
use std::collections::HashSet;

use crate::{
  error::{TryReserveError, TrySendError},
  lane::Lane,
  options::Options,
  policy::Policy,
  quota::Quota,
  receiver::{Receiver, RecvManyError},
  sender::Sender,
};

use super::{Batch, Choice, Model, PermitId};

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

/// How far one operation has come through its steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Progress {
  /// No step placed.
  Start,
  /// A `try_send` whose reserve took room: its commit is next.
  Reserved(PermitId),
  /// A receive's run: `taken` items chosen into `batch`; `closed` once the choice that took
  /// nothing is placed; `given`, one bit per item whose room is back.
  Run {
    taken: usize,
    batch: Batch,
    closed: bool,
    given: u32,
  },
  Done,
}

/// The search's state: the model, every operation's progress, and the receive whose run is open.
#[derive(Clone, PartialEq, Eq, Hash)]
struct State<L: Lane> {
  model: Model<u64, L, ()>,
  progress: Vec<Progress>,
  open: Option<usize>,
}

/// Whether `history` is linearizable against a fresh model of `options` under `policy`.
pub(crate) fn linearizable<L: Lane + Hash, Y: Policy<L, Stamp = ()>>(
  options: &Options<L>,
  policy: &Y,
  history: &[Event<L>],
) -> bool {
  let state = State {
    model: Model::new(options),
    progress: vec![Progress::Start; history.len()],
    open: None,
  };
  let mut refuted = HashSet::new();
  search(state, policy, history, &mut refuted)
}

fn search<L: Lane + Hash, Y: Policy<L, Stamp = ()>>(
  state: State<L>,
  policy: &Y,
  history: &[Event<L>],
  refuted: &mut HashSet<State<L>>,
) -> bool {
  if state
    .progress
    .iter()
    .all(|progress| *progress == Progress::Done)
  {
    return true;
  }
  if refuted.contains(&state) {
    return false;
  }
  for at in 0..history.len() {
    if state.progress[at] == Progress::Done {
      continue;
    }
    // Real time: every operation that returned before this one was called is done.
    let ready = (0..history.len()).all(|other| {
      state.progress[other] == Progress::Done || history[other].ret > history[at].call
    });
    if !ready {
      continue;
    }
    for next in steps(&state, at, policy, &history[at]) {
      if search(next, policy, history, refuted) {
        return true;
      }
    }
  }
  refuted.insert(state);
  false
}

/// Every state that one step of operation `at` can lead to, where that step answers as the
/// history says.
fn steps<L: Lane + Hash, Y: Policy<L, Stamp = ()>>(
  state: &State<L>,
  at: usize,
  policy: &Y,
  event: &Event<L>,
) -> Vec<State<L>> {
  let mut next = Vec::new();
  let with = |model: Model<u64, L, ()>, progress: Progress, open: Option<usize>| {
    let mut after = state.clone();
    after.model = model;
    after.progress[at] = progress;
    after.open = open;
    after
  };
  match (&event.op, state.progress[at]) {
    (Op::Send { lane, .. }, Progress::Start) => {
      let mut model = state.model.clone();
      match (model.try_reserve(*lane), &event.answer) {
        (Ok(permit), Answer::Sent | Answer::Closed) => {
          next.push(with(model, Progress::Reserved(permit), state.open));
        }
        (Err(TryReserveError::Full), Answer::Full)
        | (Err(TryReserveError::Closed), Answer::Closed) => {
          next.push(with(model, Progress::Done, state.open));
        }
        _ => {}
      }
    }
    (Op::Send { item, .. }, Progress::Reserved(permit)) => {
      let mut model = state.model.clone();
      let sent = model.send(permit, *item, ()).is_ok();
      if sent == (event.answer == Answer::Sent) {
        next.push(with(model, Progress::Done, state.open));
      }
    }
    (Op::Recv { quota }, Progress::Start) if !quota.admits_any() => {
      if event.answer == Answer::EmptyQuota {
        next.push(with(state.model.clone(), Progress::Done, state.open));
      }
    }
    (Op::Recv { quota }, progress) => {
      let (taken, batch, closed, given) = match progress {
        Progress::Start => (0, Batch::new(quota), false, 0),
        Progress::Run {
          taken,
          batch,
          closed,
          given,
        } => (taken, batch, closed, given),
        Progress::Reserved(_) | Progress::Done => return next,
      };
      let expected: &[(L, u64, u64)] = match &event.answer {
        Answer::Took(items) => items,
        Answer::Nothing | Answer::Ended => &[],
        _ => return next,
      };
      // The next choice, where the run is not closed and no other run is open.
      if !closed && state.open.is_none_or(|open| open == at) {
        let mut model = state.model.clone();
        let mut batch = batch;
        match model.choose_held(quota, (), policy, &mut batch) {
          Choice::Took(item) => {
            if expected.get(taken) == Some(&(item.lane, item.ordinal, item.item)) {
              let run = Progress::Run {
                taken: taken + 1,
                batch,
                closed: false,
                given,
              };
              next.push(with(model, run, Some(at)));
            }
          }
          Choice::Stopped | Choice::Nothing => {
            let answers = match &event.answer {
              Answer::Took(_) => true,
              Answer::Nothing => !model.ended(quota),
              Answer::Ended => model.ended(quota),
              _ => false,
            };
            if taken == expected.len() && answers {
              let run = Progress::Run {
                taken,
                batch,
                closed: true,
                given,
              };
              next.push(with(model, settle(run, expected.len()), None));
            }
          }
        }
      }
      // A room given back, for any item chosen whose room is still held.
      for item in 0..taken {
        if given & (1 << item) == 0 {
          let mut model = state.model.clone();
          model.give(expected[item].0);
          let run = Progress::Run {
            taken,
            batch,
            closed,
            given: given | (1 << item),
          };
          next.push(with(model, settle(run, expected.len()), state.open));
        }
      }
    }
    // A send's run cannot be; a done operation has no step.
    (Op::Send { .. }, Progress::Run { .. } | Progress::Done) => {}
  }
  next
}

/// A run that is closed with every room back is done.
fn settle(progress: Progress, items: usize) -> Progress {
  match progress {
    Progress::Run {
      closed: true,
      given,
      ..
    } if given.count_ones() as usize == items => Progress::Done,
    progress => progress,
  }
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
