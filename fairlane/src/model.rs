//! The reference model: the whole channel as one state, changed by atomic steps.
//!
//! It is the oracle the two-tier channel is tested against. It states the lane choice a second
//! time, on its own: it never calls [`choice::choose`](crate::choice::choose),
//! [`choice::count`](crate::choice::count) or the batch rule, so a wrong rule in either shows as
//! a disagreement. It asks the policy, because the policy is an input of both.
//!
//! A batch is a run of choices. A close is a run of closes of one lane each, in index order.

use std::collections::{BTreeMap, VecDeque};

use crate::{
  error::{Closed, TryReserveError},
  lane::{Lane, lane_count},
  options::Options,
  policy::{LaneView, Policy},
  quota::Quota,
};

/// A permit the model handed out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PermitId(u64);

/// A reserve that waits in the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct WaiterId(u64);

/// What a reserve did at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reserved {
  /// It took room.
  Granted(PermitId),
  /// It waits, behind every earlier waiter of its lane.
  Waiting(WaiterId),
  /// The lane is closed.
  Closed,
}

/// One item taken out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Taken<T, L> {
  pub(crate) item: T,
  pub(crate) lane: L,
  pub(crate) ordinal: u64,
}

/// One lane's counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneCounts {
  pub(crate) queued: usize,
  pub(crate) reserved: usize,
  pub(crate) remaining: usize,
  pub(crate) senders_waiting: usize,
  pub(crate) committed: u64,
  pub(crate) received: u64,
  pub(crate) closed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Waiter {
  /// In its lane's line.
  Waiting(usize),
  /// Granted room in this lane, not yet taken up.
  Granted(usize),
  /// Its lane closed before it was granted, or its grant was settled at the close.
  Closed,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct LaneModel<T, S> {
  capacity: usize,
  express: bool,
  queue: VecDeque<(u64, T, S)>,
  /// Room held: permits out, and grants not yet taken up.
  held: usize,
  line: VecDeque<WaiterId>,
  committed: u64,
  received: u64,
  closed: bool,
}

/// A batch's running state: how much it holds, and the limit its lanes have set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Batch {
  held: usize,
  limit: usize,
}

impl Batch {
  pub(crate) fn new<L: Lane>(quota: &Quota<L>) -> Self {
    Self {
      held: 0,
      limit: quota.limit(),
    }
  }
}

/// What one choice did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Choice<T, L> {
  Took(Taken<T, L>),
  /// The chosen lane's limit is reached: the batch ends, and nothing else is taken instead.
  Stopped,
  /// No lane the quota admits holds an item.
  Nothing,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Model<T, L: Lane, S> {
  lanes: Vec<LaneModel<T, S>>,
  passed_over: Vec<u32>,
  permits: BTreeMap<PermitId, usize>,
  waiters: BTreeMap<WaiterId, Waiter>,
  next_id: u64,
  _lane: core::marker::PhantomData<L>,
}

impl<T, L: Lane, S: Copy> Model<T, L, S> {
  pub(crate) fn new(options: &Options<L>) -> Self {
    let lanes = (0..lane_count::<L>())
      .map(|index| LaneModel {
        capacity: options.lane_at(index).capacity().get(),
        express: options.is_express_at(index),
        queue: VecDeque::new(),
        held: 0,
        line: VecDeque::new(),
        committed: 0,
        received: 0,
        closed: false,
      })
      .collect();
    Self {
      lanes,
      passed_over: vec![0; lane_count::<L>()],
      permits: BTreeMap::new(),
      waiters: BTreeMap::new(),
      next_id: 0,
      _lane: core::marker::PhantomData,
    }
  }

  fn id(&mut self) -> u64 {
    self.next_id += 1;
    self.next_id
  }

  fn has_room(&self, index: usize) -> bool {
    let lane = &self.lanes[index];
    lane.queue.len() + lane.held < lane.capacity
  }

  fn grant(&mut self, index: usize) -> PermitId {
    self.lanes[index].held += 1;
    let permit = PermitId(self.id());
    self.permits.insert(permit, index);
    permit
  }

  /// A reserve that never waits.
  pub(crate) fn try_reserve(&mut self, lane: L) -> Result<PermitId, TryReserveError> {
    let index = lane.index();
    if self.lanes[index].closed {
      return Err(TryReserveError::Closed);
    }
    if !self.lanes[index].line.is_empty() || !self.has_room(index) {
      return Err(TryReserveError::Full);
    }
    Ok(self.grant(index))
  }

  /// A reserve that waits where it must.
  pub(crate) fn reserve(&mut self, lane: L) -> Reserved {
    match self.try_reserve(lane) {
      Ok(permit) => Reserved::Granted(permit),
      Err(TryReserveError::Closed) => Reserved::Closed,
      Err(TryReserveError::Full) => {
        let waiter = WaiterId(self.id());
        let index = lane.index();
        self.lanes[index].line.push_back(waiter);
        self.waiters.insert(waiter, Waiter::Waiting(index));
        Reserved::Waiting(waiter)
      }
    }
  }

  /// A waiting reserve that looks: its permit, `Closed`, or `None` while it still waits. A
  /// resolved waiter is gone afterwards.
  pub(crate) fn verdict(&mut self, waiter: WaiterId) -> Option<Result<PermitId, Closed>> {
    match self.waiters.get(&waiter).copied()? {
      Waiter::Waiting(_) => None,
      Waiter::Granted(index) => {
        self.waiters.remove(&waiter);
        let permit = PermitId(self.id());
        self.permits.insert(permit, index);
        Some(Ok(permit))
      }
      Waiter::Closed => {
        self.waiters.remove(&waiter);
        Some(Err(Closed))
      }
    }
  }

  /// A waiting reserve that is dropped. Room granted to it goes on as a release would.
  pub(crate) fn leave(&mut self, waiter: WaiterId) {
    match self.waiters.remove(&waiter) {
      Some(Waiter::Waiting(index)) => self.lanes[index].line.retain(|w| *w != waiter),
      Some(Waiter::Granted(index)) => self.release(index),
      Some(Waiter::Closed) | None => {}
    }
  }

  /// Room given back to lane `index`: to its oldest waiter, or free.
  fn release(&mut self, index: usize) {
    let lane = &mut self.lanes[index];
    lane.held -= 1;
    if lane.closed {
      return;
    }
    if let Some(waiter) = lane.line.pop_front() {
      lane.held += 1;
      self.waiters.insert(waiter, Waiter::Granted(index));
    }
  }

  /// The room of an item taken by [`choose_held`](Self::choose_held), given back.
  pub(crate) fn give(&mut self, lane: L) {
    self.release(lane.index());
  }

  /// A permit dropped unsent.
  pub(crate) fn drop_permit(&mut self, permit: PermitId) {
    let index = self.permits.remove(&permit).expect("a live permit");
    self.release(index);
  }

  /// A permit's send: the item's ordinal, or the item back where its lane is closed.
  pub(crate) fn send(&mut self, permit: PermitId, item: T, stamp: S) -> Result<u64, T> {
    let index = self.permits.remove(&permit).expect("a live permit");
    if self.lanes[index].closed {
      self.release(index);
      return Err(item);
    }
    let lane = &mut self.lanes[index];
    lane.held -= 1;
    let ordinal = lane.committed;
    lane.committed += 1;
    lane.queue.push_back((ordinal, item, stamp));
    Ok(ordinal)
  }

  /// The lane a permit holds room in.
  pub(crate) fn permit_lane(&self, permit: PermitId) -> L {
    L::from_index(self.permits[&permit]).expect("a valid lane")
  }

  /// One choice of a batch, for `quota`, read at `now`; the room of an item taken goes back at
  /// once.
  pub(crate) fn choose<Y: Policy<L, Stamp = S>>(
    &mut self,
    quota: &Quota<L>,
    now: S,
    policy: &Y,
    batch: &mut Batch,
  ) -> Choice<T, L> {
    let choice = self.choose_held(quota, now, policy, batch);
    if let Choice::Took(taken) = &choice {
      self.give(taken.lane);
    }
    choice
  }

  /// [`choose`](Self::choose), with the room of an item taken still held: the channel gives it
  /// back after the pop, by its own step, [`give`](Self::give).
  pub(crate) fn choose_held<Y: Policy<L, Stamp = S>>(
    &mut self,
    quota: &Quota<L>,
    now: S,
    policy: &Y,
    batch: &mut Batch,
  ) -> Choice<T, L> {
    let waiting = |lane: &LaneModel<T, S>| !lane.queue.is_empty();
    let admitted: Vec<usize> = (0..self.lanes.len())
      .filter(|&index| quota.admits_at(index) && waiting(&self.lanes[index]))
      .collect();
    let Some(&first) = admitted.first() else {
      return Choice::Nothing;
    };

    // Express lanes first, by index; outside the policy.
    let express = admitted
      .iter()
      .copied()
      .find(|&index| self.lanes[index].express);
    let others: Vec<usize> = admitted
      .iter()
      .copied()
      .filter(|&index| !self.lanes[index].express)
      .collect();
    let chosen = express.unwrap_or_else(|| {
      let mut view = LaneView::new(now);
      for &index in &others {
        let lane = L::from_index(index).expect("a valid lane");
        let queue = &self.lanes[index].queue;
        view = view.with_lane(
          lane,
          queue.len(),
          queue.front().map(|(_, _, stamp)| *stamp),
          self.passed_over[index],
        );
      }
      match policy.owed(&view).map(Lane::index) {
        Some(owed) if others.contains(&owed) => owed,
        _ => others.first().copied().unwrap_or(first),
      }
    });

    // The batch rule: stop at the chosen lane's limit, never take another lane instead.
    let limit = batch.limit.min(quota.limit_at(chosen));
    if batch.held >= limit {
      return Choice::Stopped;
    }
    batch.held += 1;
    batch.limit = limit;

    if !self.lanes[chosen].express {
      for &index in &others {
        if index != chosen {
          self.passed_over[index] = self.passed_over[index].saturating_add(1);
        }
      }
      self.passed_over[chosen] = 0;
    }

    let lane = &mut self.lanes[chosen];
    let (ordinal, item, _) = lane.queue.pop_front().expect("a lane read as waiting");
    lane.received += 1;
    lane.held += 1;
    Choice::Took(Taken {
      item,
      lane: L::from_index(chosen).expect("a valid lane"),
      ordinal,
    })
  }

  /// A whole batch for `quota` at `now`: choices until one stops or finds nothing.
  pub(crate) fn recv_many<Y: Policy<L, Stamp = S>>(
    &mut self,
    quota: &Quota<L>,
    now: S,
    policy: &Y,
  ) -> Vec<Taken<T, L>> {
    let mut batch = Batch::new(quota);
    let mut taken = Vec::new();
    while let Choice::Took(item) = self.choose(quota, now, policy, &mut batch) {
      taken.push(item);
    }
    taken
  }

  /// Whether every lane `quota` admits is closed and empty.
  pub(crate) fn ended(&self, quota: &Quota<L>) -> bool {
    (0..self.lanes.len())
      .filter(|&index| quota.admits_at(index))
      .all(|index| self.lanes[index].closed && self.lanes[index].queue.is_empty())
  }

  /// Closes lane `index`: no commit after it; every waiter resolves `Closed`; room granted and
  /// not yet taken up is settled back.
  pub(crate) fn close_lane(&mut self, index: usize) {
    let lane = &mut self.lanes[index];
    if lane.closed {
      return;
    }
    lane.closed = true;
    for waiter in lane.line.drain(..) {
      self.waiters.insert(waiter, Waiter::Closed);
    }
    for state in self.waiters.values_mut() {
      if *state == Waiter::Granted(index) {
        *state = Waiter::Closed;
        lane.held -= 1;
      }
    }
  }

  /// `close()`: every lane, in index order.
  pub(crate) fn close(&mut self) {
    for index in 0..self.lanes.len() {
      self.close_lane(index);
    }
  }

  /// The last receiver gone: a close, and every queued item let go, in index order.
  pub(crate) fn disconnect(&mut self) -> Vec<T> {
    self.close();
    let mut dropped = Vec::new();
    for lane in &mut self.lanes {
      for (_, item, _) in lane.queue.drain(..) {
        lane.received += 1;
        dropped.push(item);
      }
    }
    dropped
  }

  pub(crate) fn counts(&self, lane: L) -> LaneCounts {
    let model = &self.lanes[lane.index()];
    LaneCounts {
      queued: model.queue.len(),
      reserved: model.held,
      remaining: model.capacity - model.queue.len() - model.held,
      senders_waiting: model.line.len(),
      committed: model.committed,
      received: model.received,
      closed: model.closed,
    }
  }

  pub(crate) fn is_closed(&self) -> bool {
    self.lanes.iter().all(|lane| lane.closed)
  }
}

pub(crate) mod check;

#[cfg(test)]
mod tests;
