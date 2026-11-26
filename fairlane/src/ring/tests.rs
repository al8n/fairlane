use std::{
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::SeqCst},
  },
  thread,
};

use super::*;

/// Counts its drops, so a test sees every item dropped exactly once.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
  fn drop(&mut self) {
    self.0.fetch_add(1, SeqCst);
  }
}

#[test]
fn ring_is_fifo_and_its_ordinals_are_positions() {
  let ring = Ring::<u32, ()>::new(4, false).unwrap();
  for item in 0..4 {
    assert_eq!(ring.push(item, ()), Ok(u64::from(item)));
  }
  assert_eq!(ring.queued(), 4);
  for item in 0..4 {
    let popped = ring.pop().unwrap();
    assert_eq!((popped.ordinal, popped.item), (u64::from(item), item));
    assert_eq!(popped.stamp, None);
  }
  assert!(ring.pop().is_none());
  assert!(ring.is_empty());
  assert_eq!(
    (ring.committed(), ring.received(), ring.queued()),
    (4, 4, 0)
  );
}

#[test]
fn ring_laps_over_a_buffer_larger_than_its_capacity() {
  let ring = Ring::<u64, ()>::new(3, false).unwrap();
  assert_eq!(ring.mask, 3);
  let mut next = 0;
  for _ in 0..50 {
    for _ in 0..3 {
      assert_eq!(ring.push(next, ()), Ok(next));
      next += 1;
    }
    for expected in next - 3..next {
      let popped = ring.pop().unwrap();
      assert_eq!((popped.ordinal, popped.item), (expected, expected));
    }
  }
  assert_eq!(ring.committed(), 150);
}

#[test]
fn ring_of_one() {
  let ring = Ring::<&str, ()>::new(1, false).unwrap();
  assert_eq!(ring.mask, 0);
  for round in 0..5_u64 {
    assert_eq!(ring.push("x", ()), Ok(round));
    assert_eq!(ring.pop().unwrap().item, "x");
  }
}

#[test]
fn ring_close_hands_a_later_push_its_item() {
  let ring = Ring::<String, ()>::new(2, false).unwrap();
  ring.push("kept".to_owned(), ()).unwrap();
  assert!(!ring.is_closed());
  assert!(ring.close());
  assert!(!ring.close());
  assert!(ring.is_closed());
  assert_eq!(ring.push("late".to_owned(), ()), Err("late".to_owned()));
  assert!(!ring.is_ended());
  assert_eq!(ring.pop().unwrap().item, "kept");
  assert!(ring.is_ended());
  assert!(ring.pop().is_none());
  assert_eq!(ring.committed(), 1);
}

#[test]
fn ring_is_ended_only_when_closed_and_empty() {
  let ring = Ring::<u8, ()>::new(2, false).unwrap();
  assert!(!ring.is_ended());
  ring.close();
  assert!(ring.is_ended());
}

#[test]
fn ring_drops_what_is_left_once() {
  let drops = Arc::new(AtomicUsize::new(0));
  let ring = Ring::<Counted, ()>::new(4, false).unwrap();
  for _ in 0..3 {
    assert!(ring.push(Counted(drops.clone()), ()).is_ok());
  }
  drop(ring.pop().unwrap());
  assert_eq!(drops.load(SeqCst), 1);
  drop(ring);
  assert_eq!(drops.load(SeqCst), 3);
}

#[test]
fn ring_drops_heap_items_left_across_a_lap() {
  let ring = Ring::<Box<u64>, ()>::new(2, false).unwrap();
  for item in 0..5 {
    // Room first, as a permit would hold it: a full ring is popped before the next push.
    if ring.queued() == 2 {
      ring.pop().unwrap();
    }
    ring.push(Box::new(item), ()).unwrap();
  }
  assert_eq!(ring.queued(), 2);
  drop(ring);
}

#[test]
fn ring_carries_stamps() {
  let ring = Ring::<u8, (u64, u32)>::new(2, true).unwrap();
  ring.push(1, (10, 20)).unwrap();
  ring.push(2, (30, 40)).unwrap();
  assert_eq!(ring.pop().unwrap().stamp, Some((10, 20)));
  assert_eq!(ring.pop().unwrap().stamp, Some((30, 40)));
}

#[test]
fn ring_holds_zero_sized_items() {
  let ring = Ring::<(), ()>::new(2, true).unwrap();
  for round in 0..10_u64 {
    assert_eq!(ring.push((), ()), Ok(round));
    assert_eq!(ring.pop().unwrap().ordinal, round);
  }
}

#[test]
fn ring_refuses_what_cannot_be_held() {
  assert!(Ring::<u8, ()>::new(usize::MAX, false).is_none());
  assert!(Ring::<[u8; 1 << 20], ()>::new(1 << 50, false).is_none());
  assert!(Ring::<u8, [u8; 1 << 20]>::new(1 << 50, true).is_none());
}

/// Two pushers and two poppers on real threads. Room is counted as permits count it: a pusher
/// takes a unit before it pushes, and a popper gives one back after its pop is complete.
#[test]
fn ring_moves_items_between_threads() {
  let per_pusher: u64 = if cfg!(miri) { 20 } else { 2_000 };
  let capacity = 3;
  let ring = Arc::new(Ring::<(u64, u64), ()>::new(capacity, false).unwrap());
  let room = Arc::new(AtomicUsize::new(capacity));
  let popped = Arc::new(AtomicUsize::new(0));
  let total = 2 * per_pusher as usize;

  let pushers: Vec<_> = (0..2)
    .map(|pusher| {
      let (ring, room) = (ring.clone(), room.clone());
      thread::spawn(move || {
        for sequence in 0..per_pusher {
          loop {
            let free = room.load(SeqCst);
            if free > 0
              && room
                .compare_exchange(free, free - 1, SeqCst, SeqCst)
                .is_ok()
            {
              break;
            }
            thread::yield_now();
          }
          ring.push((pusher, sequence), ()).unwrap();
        }
      })
    })
    .collect();
  let poppers: Vec<_> = (0..2)
    .map(|_| {
      let (ring, room, popped) = (ring.clone(), room.clone(), popped.clone());
      thread::spawn(move || {
        let mut seen = Vec::new();
        while popped.load(SeqCst) < total {
          match ring.pop() {
            Some(item) => {
              room.fetch_add(1, SeqCst);
              popped.fetch_add(1, SeqCst);
              seen.push((item.ordinal, item.item));
            }
            None => thread::yield_now(),
          }
        }
        seen
      })
    })
    .collect();
  for pusher in pushers {
    pusher.join().unwrap();
  }
  let mut seen: Vec<(u64, (u64, u64))> = poppers
    .into_iter()
    .flat_map(|popper| popper.join().unwrap())
    .collect();
  seen.sort();
  assert_eq!(seen.len(), total);
  for (at, (ordinal, _)) in seen.iter().enumerate() {
    assert_eq!(*ordinal, at as u64, "each ordinal once, with no gap");
  }
  for pusher in 0..2 {
    let order: Vec<u64> = seen
      .iter()
      .filter(|(_, (from, _))| *from == pusher)
      .map(|(_, (_, sequence))| *sequence)
      .collect();
    assert_eq!(order, (0..per_pusher).collect::<Vec<_>>());
  }
}
