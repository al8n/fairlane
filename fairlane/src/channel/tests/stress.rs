//! A stress test on real threads, with no timing assertion. Every door, several lanes, an
//! express lane: every item is received once, each lane's ordinals have no gap, and each sender's
//! items in one lane come in the order it sent them.

use std::collections::BTreeMap;

use super::*;
use crate::{PassLimit, Quota, lane::Lane, park::park_on, testing::Three};

#[test]
fn stress_every_item_once_and_in_its_lane_order() {
  let per_sender: u64 = if cfg!(miri) { 12 } else { 3_000 };
  let options = Options::from_fn(|lane| {
    let options = LaneOptions::new(cap(4));
    if lane == Three::A {
      options.with_express(true)
    } else {
      options
    }
  });
  let policy = PassLimit::<Three>::new(core::num::NonZeroU32::MIN.saturating_add(1));
  let (tx, rx) = channel_with::<u64, Three, _>(options, policy).unwrap();
  let item = |sender: u64, sequence: u64| (sender << 32) | sequence;
  let lane_of = |sequence: u64| Three::ALL[(sequence % 3) as usize];

  let received: Vec<(Three, u64, u64)> = std::thread::scope(|scope| {
    for sender in 0..4_u64 {
      let tx = tx.clone();
      scope.spawn(move || {
        for sequence in 0..per_sender {
          let lane = lane_of(sequence);
          let value = item(sender, sequence);
          match sender {
            0 => park_on(tx.send(lane, value)).unwrap(),
            1 => tx.send_blocking(lane, value).unwrap(),
            2 => {
              let mut value = value;
              loop {
                match tx.try_send(lane, value) {
                  Ok(()) => break,
                  Err(TrySendError::Full(back)) => {
                    value = back;
                    std::thread::yield_now();
                  }
                  Err(TrySendError::Closed(_)) => panic!("closed early"),
                }
              }
            }
            _ => park_on(tx.reserve_owned(lane))
              .unwrap()
              .send(value)
              .unwrap(),
          }
        }
      });
    }
    drop(tx);
    let receivers: Vec<_> = (0..3)
      .map(|kind| {
        let rx = rx.clone();
        scope.spawn(move || {
          let mut got = Vec::new();
          match kind {
            0 => {
              let mut batch = Vec::new();
              while park_on(rx.recv_many(&mut batch, Quota::up_to(5))).is_ok() {}
              got.extend(batch);
            }
            1 => {
              while let Ok(received) = rx.recv_blocking() {
                got.push(received);
              }
            }
            _ => loop {
              match rx.try_recv() {
                Ok(Some(received)) => got.push(received),
                Ok(None) => std::thread::yield_now(),
                Err(Ended) => break,
              }
            },
          }
          got
            .into_iter()
            .map(|received| (received.lane(), received.ordinal(), received.into_item()))
            .collect::<Vec<_>>()
        })
      })
      .collect();
    receivers
      .into_iter()
      .flat_map(|handle| handle.join().unwrap())
      .collect()
  });
  drop(rx);

  assert_eq!(received.len() as u64, 4 * per_sender, "every item, once");
  let mut by_lane: BTreeMap<usize, Vec<(u64, u64)>> = BTreeMap::new();
  for (lane, ordinal, value) in received {
    by_lane
      .entry(lane.index())
      .or_default()
      .push((ordinal, value));
  }
  for (lane, mut items) in by_lane {
    items.sort_unstable();
    for (at, (ordinal, _)) in items.iter().enumerate() {
      assert_eq!(*ordinal, at as u64, "lane {lane}: ordinals with no gap");
    }
    for sender in 0..4_u64 {
      let order: Vec<u64> = items
        .iter()
        .filter(|(_, value)| value >> 32 == sender)
        .map(|(_, value)| value & 0xffff_ffff)
        .collect();
      assert!(
        order.windows(2).all(|pair| pair[0] < pair[1]),
        "lane {lane}: sender {sender}'s items out of order"
      );
    }
  }
}
