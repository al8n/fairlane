//! X1 for several lanes: the channel and the reference model under the same seeded steps, with an
//! express lane or none, under `Strict`, `PassLimit`, `Elapsed` and the default pair. Time is a
//! `ManualClock`, which both sides read at the same moments.

use core::{num::NonZeroU32, time::Duration};

use super::*;
use crate::{
  Elapsed, FirstOf, ManualClock, PassLimit, Permit, Policy, Quota, Strict,
  model::{Model, PermitId},
  testing::{Rng, Three},
};

fn walk<Y: Policy<Three>>(seed: u64, steps: usize, policy: Y, clock: &ManualClock) {
  let mut rng = Rng::new(seed);
  let capacity = 1 + rng.below(3);
  let express = rng.one_in(2);
  let options = Options::from_fn(|lane| {
    let options = LaneOptions::new(cap(capacity));
    if lane == Three::A {
      options.with_express(express)
    } else {
      options
    }
  });
  let mut model = Model::<u64, Three, Y::Stamp>::new(&options);
  let (tx, rx) = channel_with::<u64, Three, Y>(options, policy).unwrap();
  let mut held: Vec<(Permit<'_, u64, Three, Y>, PermitId)> = Vec::new();
  let mut next_item = 0_u64;

  for step in 0..steps {
    let at = format!("seed {seed}, step {step}");
    let lane = Three::ALL[rng.below(3)];
    match rng.below(10) {
      0..=2 => match (tx.try_reserve(lane), model.try_reserve(lane)) {
        (Ok(permit), Ok(id)) => held.push((permit, id)),
        (Err(ours), Err(theirs)) => assert_eq!(ours, theirs, "try_reserve, {at}"),
        (ours, theirs) => panic!("try_reserve: {:?} for {theirs:?}, {at}", ours.err()),
      },
      3 | 4 if !held.is_empty() => {
        let (permit, id) = held.swap_remove(rng.below(held.len()));
        next_item += 1;
        let stamp = tx.policy().stamp();
        match (permit.send(next_item), model.send(id, next_item, stamp)) {
          (Ok(()), Ok(_)) => {}
          (Err(SendError(a)), Err(b)) => assert_eq!(a, b, "{at}"),
          (ours, theirs) => panic!("send: {ours:?} for {theirs:?}, {at}"),
        }
      }
      5 if !held.is_empty() => {
        let (permit, id) = held.swap_remove(rng.below(held.len()));
        drop(permit);
        model.drop_permit(id);
      }
      6 | 7 => {
        let mut quota = Quota::up_to(rng.below(5));
        for each in Three::ALL {
          if rng.one_in(3) {
            quota = quota.limit_with(each, rng.below(4));
          }
        }
        if rng.one_in(3) {
          quota = quota.lanes(..=Three::ALL[rng.below(3)]);
        }
        let now = tx.policy().stamp();
        let mut batch = Vec::new();
        let ours = rx.try_recv_many(&mut batch, &quota);
        if !Three::ALL.iter().any(|&each| quota.admits(each)) {
          assert_eq!(ours, Err(RecvManyError::EmptyQuota), "{at}");
          continue;
        }
        let ended = model.ended(&quota);
        let theirs = model.recv_many(&quota, now, tx.policy());
        let ours_items: Vec<(Three, u64, u64)> = batch
          .iter()
          .map(|received| (received.lane(), received.ordinal(), *received.item()))
          .collect();
        let theirs_items: Vec<(Three, u64, u64)> = theirs
          .iter()
          .map(|taken| (taken.lane, taken.ordinal, taken.item))
          .collect();
        assert_eq!(ours_items, theirs_items, "a batch, {at}");
        match ours {
          Ok(n) => assert_eq!(n, theirs_items.len(), "{at}"),
          Err(RecvManyError::Ended) => assert!(ended && theirs_items.is_empty(), "{at}"),
          Err(RecvManyError::EmptyQuota) => panic!("an admitting quota refused, {at}"),
        }
      }
      8 => clock.advance(Duration::from_millis(rng.below(4) as u64)),
      9 if rng.one_in(12) => {
        tx.close();
        model.close();
      }
      _ => {}
    }

    let ours = tx.snapshot();
    for each in Three::ALL {
      let theirs = model.counts(each);
      assert_eq!(
        (
          ours.queued(each),
          ours.reserved(each),
          ours.remaining(each),
          ours.committed(each),
          ours.received(each),
        ),
        (
          theirs.queued,
          theirs.reserved,
          theirs.remaining,
          theirs.committed,
          theirs.received,
        ),
        "{each:?}'s snapshot, {at}"
      );
    }
    assert_eq!(ours.is_closed(), model.is_closed(), "{at}");
  }
  drop(held);
}

fn limit(n: u32) -> NonZeroU32 {
  NonZeroU32::new(n).unwrap()
}

#[test]
#[cfg_attr(miri, ignore)]
fn differential_lanes() {
  for seed in 0..120 {
    let clock = ManualClock::new();
    walk(seed, 150, Strict, &clock);
    walk(seed, 150, PassLimit::<Three>::new(limit(2)), &clock);
    walk(
      seed,
      150,
      Elapsed::<Three, _>::with_clock(Duration::from_millis(5), clock.clone()),
      &clock,
    );
    walk(
      seed,
      150,
      FirstOf::new(
        PassLimit::<Three>::new(limit(3)),
        Elapsed::<Three, _>::with_clock(Duration::from_millis(5), clock.clone()),
      ),
      &clock,
    );
  }
}

#[test]
fn differential_lanes_one_seed() {
  let clock = ManualClock::new();
  walk(3, 40, PassLimit::<Three>::new(limit(2)), &clock);
}
