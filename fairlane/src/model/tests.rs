use core::num::{NonZeroU32, NonZeroUsize};

use super::*;
use crate::{
  options::LaneOptions,
  policy::{PassLimit, Strict},
  testing::{Rng, Three},
};

fn cap(n: usize) -> NonZeroUsize {
  NonZeroUsize::new(n).unwrap()
}

/// Three lanes of capacity `n`; lane A express when `express`.
fn options(n: usize, express: bool) -> Options<Three> {
  Options::from_fn(|lane| {
    let options = LaneOptions::new(cap(n));
    if lane == Three::A {
      options.with_express(express)
    } else {
      options
    }
  })
}

type M = Model<u64, Three, ()>;

fn granted(reserved: Reserved) -> PermitId {
  match reserved {
    Reserved::Granted(permit) => permit,
    other => panic!("expected a grant, got {other:?}"),
  }
}

fn waiting(reserved: Reserved) -> WaiterId {
  match reserved {
    Reserved::Waiting(waiter) => waiter,
    other => panic!("expected a wait, got {other:?}"),
  }
}

fn fill(model: &mut M, lane: Three, items: impl IntoIterator<Item = u64>) {
  for item in items {
    let permit = model.try_reserve(lane).unwrap();
    model.send(permit, item, ()).unwrap();
  }
}

#[test]
fn model_grants_in_the_order_reserves_waited() {
  let mut model = M::new(&options(1, false));
  let first = granted(model.reserve(Three::B));
  let w1 = waiting(model.reserve(Three::B));
  let w2 = waiting(model.reserve(Three::B));
  let w3 = waiting(model.reserve(Three::B));
  assert_eq!(model.try_reserve(Three::B), Err(TryReserveError::Full));
  model.drop_permit(first);
  assert_eq!(model.verdict(w2), None);
  assert_eq!(model.verdict(w3), None);
  let second = model.verdict(w1).unwrap().unwrap();
  model.drop_permit(second);
  assert_eq!(model.verdict(w3), None);
  assert!(model.verdict(w2).unwrap().is_ok());
  assert_eq!(model.counts(Three::B).senders_waiting, 1);
}

#[test]
fn model_grants_pass_on_from_a_dropped_waiter() {
  let mut model = M::new(&options(1, false));
  let first = granted(model.reserve(Three::B));
  let w1 = waiting(model.reserve(Three::B));
  let w2 = waiting(model.reserve(Three::B));
  model.drop_permit(first);
  model.leave(w1);
  assert!(model.verdict(w2).unwrap().is_ok());
  assert_eq!(model.counts(Three::B).reserved, 1);
}

#[test]
fn model_grants_room_to_nobody_when_the_line_is_empty() {
  let mut model = M::new(&options(2, false));
  let permit = model.try_reserve(Three::C).unwrap();
  assert_eq!(model.counts(Three::C).remaining, 1);
  model.drop_permit(permit);
  assert_eq!(model.counts(Three::C).remaining, 2);
}

#[test]
fn model_close_settles_grants_not_taken_up() {
  let mut model = M::new(&options(1, false));
  let first = granted(model.reserve(Three::B));
  let w1 = waiting(model.reserve(Three::B));
  let w2 = waiting(model.reserve(Three::B));
  model.drop_permit(first);
  model.close_lane(Three::B.index());
  assert_eq!(model.verdict(w1), Some(Err(Closed)));
  assert_eq!(model.verdict(w2), Some(Err(Closed)));
  let counts = model.counts(Three::B);
  assert_eq!((counts.reserved, counts.remaining), (0, 1));
  assert_eq!(model.reserve(Three::B), Reserved::Closed);
  assert_eq!(model.try_reserve(Three::B), Err(TryReserveError::Closed));
}

#[test]
fn model_close_fails_a_later_send_and_keeps_the_queue() {
  let mut model = M::new(&options(4, false));
  fill(&mut model, Three::B, [1, 2]);
  let permit = model.try_reserve(Three::B).unwrap();
  model.close();
  assert!(model.is_closed());
  assert_eq!(model.send(permit, 3, ()), Err(3));
  let quota = Quota::up_to(8);
  assert!(!model.ended(&quota));
  let taken = model.recv_many(&quota, (), &Strict);
  assert_eq!(taken.iter().map(|t| t.item).collect::<Vec<_>>(), [1, 2]);
  assert!(model.ended(&quota));
}

#[test]
fn model_disconnect_lets_go_of_what_is_queued() {
  let mut model = M::new(&options(4, false));
  fill(&mut model, Three::A, [1]);
  fill(&mut model, Three::C, [2, 3]);
  let permit = model.try_reserve(Three::B).unwrap();
  let dropped = model.disconnect();
  assert_eq!(dropped, [1, 2, 3]);
  assert_eq!(model.send(permit, 4, ()), Err(4));
  assert_eq!(model.reserve(Three::A), Reserved::Closed);
  assert!(model.ended(&Quota::up_to(1)));
  assert_eq!(model.counts(Three::C).received, 2);
}

/// Owes `lane` once its passed-over count reaches `limit`; `PassLimit` with one lane limited.
fn owe_after(lane: Three, limit: u32) -> PassLimit<Three> {
  Three::ALL
    .iter()
    .fold(PassLimit::new(NonZeroU32::MIN), |policy, &each| {
      policy.with_lane(
        each,
        (each == lane).then(|| NonZeroU32::new(limit).unwrap()),
      )
    })
}

#[test]
fn model_x4_stops_at_the_cap() {
  let mut model = M::new(&options(64, false));
  fill(&mut model, Three::A, 0..64);
  fill(&mut model, Three::B, 100..164);
  let policy = owe_after(Three::B, 2);
  let one = Quota::up_to(1);
  for _ in 0..2 {
    assert_eq!(model.recv_many(&one, (), &policy)[0].lane, Three::A);
  }
  let quota = Quota::up_to(32)
    .limit_with(Three::A, 1)
    .limit_with(Three::B, 32);
  let batch = model.recv_many(&quota, (), &policy);
  assert_eq!(
    batch.iter().map(|t| t.lane).collect::<Vec<_>>(),
    [Three::B],
    "the batch stops at the interactive limit"
  );
  let next = model.recv_many(&quota, (), &policy);
  assert_eq!(next.iter().map(|t| t.lane).collect::<Vec<_>>(), [Three::A]);
}

#[test]
fn model_o4_bounds_the_wait_by_count() {
  // Two lanes, `f = 3` on the lower one: it is passed over exactly `f` times.
  let f = 3;
  let policy = owe_after(Three::B, f);
  let mut model = M::new(&options(4, false));
  let quota = Quota::up_to(1).lanes(..=Three::B);
  let mut served = Vec::new();
  for round in 0..40_u64 {
    for lane in [Three::A, Three::B] {
      while model.counts(lane).remaining > 0 {
        fill(&mut model, lane, [round]);
      }
    }
    served.push(model.recv_many(&quota, (), &policy)[0].lane);
  }
  let gaps = gaps_between(&served, Three::B);
  assert!(!gaps.is_empty());
  assert!(gaps.iter().all(|&gap| gap == f as usize), "{gaps:?}");

  // Three lanes, `f` on each: at most `f + n - 1` items of other lanes between two of one.
  let n = 3;
  let policy = PassLimit::<Three>::new(NonZeroU32::new(f).unwrap());
  let mut model = M::new(&options(4, false));
  let quota = Quota::up_to(1);
  let mut served = Vec::new();
  for round in 0..90_u64 {
    for lane in Three::ALL {
      while model.counts(lane).remaining > 0 {
        fill(&mut model, lane, [round]);
      }
    }
    served.push(model.recv_many(&quota, (), &policy)[0].lane);
  }
  for lane in Three::ALL {
    let gaps = gaps_between(&served, lane);
    assert!(!gaps.is_empty(), "{lane:?} is never served");
    let bound = f as usize + n - 1;
    assert!(gaps.iter().all(|&gap| gap <= bound), "{lane:?}: {gaps:?}");
  }
}

/// How many items of other lanes came between two consecutive items of `lane`.
fn gaps_between(served: &[Three], lane: Three) -> Vec<usize> {
  let at: Vec<usize> = served
    .iter()
    .enumerate()
    .filter(|(_, each)| **each == lane)
    .map(|(index, _)| index)
    .collect();
  at.windows(2).map(|pair| pair[1] - pair[0] - 1).collect()
}

/// One random walk over the model, checking every law after every step.
fn walk(seed: u64, steps: usize) {
  let mut rng = Rng::new(seed);
  let express = rng.one_in(2);
  let capacity = 1 + rng.below(3);
  let options = options(capacity, express);
  let mut model = M::new(&options);
  let mut permits: Vec<PermitId> = Vec::new();
  let mut waiters: Vec<(WaiterId, Three)> = Vec::new();
  let mut next_item = 0_u64;
  let mut next_received = [0_u64; 3];

  for step in 0..steps {
    let context = || format!("seed {seed}, step {step}");
    let lane = Three::ALL[rng.below(3)];
    match rng.below(10) {
      0 => match model.try_reserve(lane) {
        Ok(permit) => permits.push(permit),
        Err(TryReserveError::Full) => {}
        Err(TryReserveError::Closed) => assert!(model.counts(lane).closed, "{}", context()),
      },
      1 => {
        let line_before = model.counts(lane).senders_waiting;
        match model.reserve(lane) {
          Reserved::Granted(permit) => {
            assert_eq!(
              line_before,
              0,
              "B3: a reserve passed a waiter, {}",
              context()
            );
            permits.push(permit);
          }
          Reserved::Waiting(waiter) => waiters.push((waiter, lane)),
          Reserved::Closed => assert!(model.counts(lane).closed, "{}", context()),
        }
      }
      2 if !waiters.is_empty() => {
        let (waiter, _) = waiters[rng.below(waiters.len())];
        match model.verdict(waiter) {
          Some(Ok(permit)) => {
            waiters.retain(|(w, _)| *w != waiter);
            permits.push(permit);
          }
          Some(Err(_)) => waiters.retain(|(w, _)| *w != waiter),
          None => {}
        }
      }
      3 if !waiters.is_empty() => {
        let (waiter, _) = waiters.swap_remove(rng.below(waiters.len()));
        model.leave(waiter);
      }
      4 | 5 if !permits.is_empty() => {
        let permit = permits.swap_remove(rng.below(permits.len()));
        let lane = model.permit_lane(permit);
        let closed = model.counts(lane).closed;
        next_item += 1;
        match model.send(permit, next_item, ()) {
          Ok(_) => assert!(!closed, "C1: a commit after the close, {}", context()),
          Err(item) => {
            assert!(closed, "{}", context());
            assert_eq!(item, next_item);
          }
        }
      }
      6 if !permits.is_empty() => {
        let permit = permits.swap_remove(rng.below(permits.len()));
        model.drop_permit(permit);
      }
      7 | 8 => {
        let quota = random_quota(&mut rng);
        let before: Vec<LaneCounts> = Three::ALL.iter().map(|&l| model.counts(l)).collect();
        let taken = model.recv_many(&quota, (), &Strict);
        check_batch(&options, &quota, &before, &taken, &context());
        for item in &taken {
          let index = item.lane.index();
          assert_eq!(item.ordinal, next_received[index], "D4, {}", context());
          next_received[index] += 1;
        }
        let ended = Three::ALL.iter().filter(|&&l| quota.admits(l)).all(|&l| {
          let counts = model.counts(l);
          counts.closed && counts.queued == 0
        });
        assert_eq!(model.ended(&quota), ended, "C3, {}", context());
      }
      9 if rng.one_in(8) => {
        model.close_lane(lane.index());
        for &(waiter, of) in &waiters {
          if of == lane {
            assert_eq!(
              model.verdict(waiter),
              Some(Err(Closed)),
              "C1: a waiter of a closed lane, {}",
              context()
            );
          }
        }
        waiters.retain(|(_, of)| *of != lane);
      }
      _ => {}
    }

    for lane in Three::ALL {
      let counts = model.counts(lane);
      assert!(
        counts.queued + counts.reserved <= capacity,
        "B1, {}",
        context()
      );
      assert_eq!(
        counts.remaining,
        capacity - counts.queued - counts.reserved,
        "{}",
        context()
      );
      if counts.senders_waiting > 0 {
        assert_eq!(
          model.try_reserve(lane),
          Err(TryReserveError::Full),
          "B3, {}",
          context()
        );
      }
    }
  }
}

fn random_quota(rng: &mut Rng) -> Quota<Three> {
  let mut quota = Quota::up_to(rng.below(5));
  for lane in Three::ALL {
    if rng.one_in(3) {
      quota = quota.limit_with(lane, rng.below(4));
    }
  }
  if rng.one_in(3) {
    quota = quota.lanes(..=Three::ALL[rng.below(3)]);
  }
  quota
}

/// O2, O3 (with `Strict`), D2 and D3 for one batch, against the counts before it.
fn check_batch(
  options: &Options<Three>,
  quota: &Quota<Three>,
  before: &[LaneCounts],
  taken: &[Taken<u64, Three>],
  context: &str,
) {
  for item in taken {
    assert!(quota.admits(item.lane), "D3, {context}");
  }
  let limit = taken
    .iter()
    .map(|item| quota.limit_for(item.lane))
    .min()
    .unwrap_or(usize::MAX)
    .min(quota.limit());
  assert!(taken.len() <= limit, "D2, {context}");
  let waiting: Vec<Three> = Three::ALL
    .iter()
    .copied()
    .filter(|&lane| quota.admits(lane) && before[lane.index()].queued > 0)
    .collect();
  let expected = waiting
    .iter()
    .copied()
    .find(|lane| options.is_express_at(lane.index()))
    .or_else(|| waiting.first().copied());
  match (taken.first(), expected) {
    (Some(first), Some(expected)) => assert_eq!(first.lane, expected, "O2/O3, {context}"),
    (None, Some(expected)) => {
      // Only a limit of zero stops a batch before its first item, and `admits` excludes it.
      panic!("nothing taken, {expected:?} waits, {context}");
    }
    (Some(_), None) => panic!("taken from nothing, {context}"),
    (None, None) => {}
  }
}

#[test]
#[cfg_attr(miri, ignore)]
fn model_keeps_its_laws_under_random_steps() {
  for seed in 0..300 {
    walk(seed, 300);
  }
}
