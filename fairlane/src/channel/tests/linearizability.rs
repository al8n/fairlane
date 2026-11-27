//! Linearizability: concurrent histories on real threads, each checked for an order of the model's
//! steps that keeps real time and gives every operation its answer. `Strict` and `PassLimit` only:
//! their choice reads no stamp, so a lane read at a different instant changes nothing.

use std::sync::{
  Barrier,
  atomic::{AtomicU64, Ordering::SeqCst},
};

use super::*;
use crate::{
  PassLimit, Policy, Quota, Strict,
  model::check::{Event, Op, linearizable, perform},
  testing::{Rng, Three},
};

fn random_op(rng: &mut Rng, item: u64) -> Op<Three> {
  match rng.below(2) {
    0 => Op::Send {
      lane: Three::ALL[rng.below(3)],
      item,
    },
    _ => {
      let mut quota = Quota::up_to(1 + rng.below(3));
      if rng.one_in(3) {
        quota = quota.limit_with(Three::ALL[rng.below(3)], rng.below(2));
      }
      if rng.one_in(3) {
        quota = quota.lanes(..=Three::ALL[rng.below(3)]);
      }
      Op::Recv { quota }
    }
  }
}

fn history<Y: Policy<Three, Stamp = ()>>(seed: u64, options: &Options<Three>, policy: Y) {
  let mut rng = Rng::new(seed);
  let threads: Vec<Vec<Op<Three>>> = (0..3_u64)
    .map(|thread| {
      (0..3_u64)
        .map(|step| random_op(&mut rng, thread * 10 + step + 1))
        .collect()
    })
    .collect();
  let (tx, rx) = channel_with::<u64, Three, Y>(options.clone(), policy).unwrap();
  let clock = AtomicU64::new(0);
  let start = Barrier::new(threads.len());
  let events: Vec<Event<Three>> = std::thread::scope(|scope| {
    let handles: Vec<_> = threads
      .iter()
      .map(|ops| {
        let (tx, rx, clock, start) = (&tx, &rx, &clock, &start);
        scope.spawn(move || {
          start.wait();
          ops
            .iter()
            .map(|op| {
              let call = clock.fetch_add(1, SeqCst);
              let answer = perform(tx, rx, op);
              let ret = clock.fetch_add(1, SeqCst);
              Event {
                call,
                ret,
                op: op.clone(),
                answer,
              }
            })
            .collect::<Vec<_>>()
        })
      })
      .collect();
    handles
      .into_iter()
      .flat_map(|handle| handle.join().unwrap())
      .collect()
  });
  assert!(
    linearizable(options, tx.policy(), &events),
    "seed {seed}: no order of the model explains {events:#?}"
  );
}

#[test]
#[cfg_attr(miri, ignore)]
fn x2_histories_are_linearizable() {
  for seed in 0..400 {
    let mut rng = Rng::new(seed ^ 0x5eed);
    let capacity = 1 + rng.below(2);
    let express = rng.one_in(3);
    let options = Options::from_fn(|lane| {
      let options = LaneOptions::new(cap(capacity));
      if lane == Three::A {
        options.with_express(express)
      } else {
        options
      }
    });
    if rng.one_in(2) {
      history(seed, &options, Strict);
    } else {
      history(
        seed,
        &options,
        PassLimit::<Three>::new(core::num::NonZeroU32::MIN.saturating_add(1)),
      );
    }
  }
}
