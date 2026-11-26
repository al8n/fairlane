use loom::{
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::SeqCst},
  },
  thread,
};

use super::model;
use crate::ring::Ring;

/// MK1 (Q1, O1, D4): two pushers, each with a permit, and one popper, on a ring of two cells.
/// Both items are popped once, each with the ordinal its push answered.
#[test]
fn mk1_two_pushers_one_popper() {
  model(|| {
    let ring = Arc::new(Ring::<u32, ()>::new(2, false).unwrap());
    let pushers: Vec<_> = [10, 20]
      .into_iter()
      .map(|item| {
        let ring = ring.clone();
        thread::spawn(move || (item, ring.push(item, || ()).unwrap()))
      })
      .collect();
    let popper = {
      let ring = ring.clone();
      thread::spawn(move || {
        let mut popped = Vec::new();
        while popped.len() < 2 {
          match ring.pop() {
            Some(item) => popped.push((item.item, item.ordinal)),
            None => thread::yield_now(),
          }
        }
        popped
      })
    };
    let mut pushed: Vec<(u32, u64)> = pushers.into_iter().map(|h| h.join().unwrap()).collect();
    let mut popped = popper.join().unwrap();
    pushed.sort_unstable();
    popped.sort_unstable();
    assert_eq!(popped, pushed);
    let mut ordinals: Vec<u64> = popped.iter().map(|(_, ordinal)| *ordinal).collect();
    ordinals.sort_unstable();
    assert_eq!(ordinals, [0, 1]);
    assert!(ring.pop().is_none());
  });
}

/// MK2 (Q5): a pop against a push that has moved the tail and not yet filled its cell. Where the
/// popper saw the ring not empty, the pop waits and returns the item.
#[test]
fn mk2_a_pop_waits_for_a_claimed_cell() {
  model(|| {
    let ring = Arc::new(Ring::<u32, ()>::new(1, false).unwrap());
    let pusher = {
      let ring = ring.clone();
      thread::spawn(move || ring.push(7, || ()).unwrap())
    };
    let popper = {
      let ring = ring.clone();
      thread::spawn(move || {
        let seen = !ring.is_empty();
        (seen, ring.pop().map(|item| item.item))
      })
    };
    pusher.join().unwrap();
    let (seen, popped) = popper.join().unwrap();
    if seen {
      assert_eq!(
        popped,
        Some(7),
        "a pop beside a committed item returned nothing"
      );
    }
  });
}

/// MK3 (the lap): two poppers, capacity 2, three items. The third is pushed on room a popper gave
/// back, into a cell a slower popper of the previous lap may still be reading.
#[test]
fn mk3_a_push_of_the_next_lap_waits_for_a_slow_popper() {
  model(|| {
    let ring = Arc::new(Ring::<u32, ()>::new(2, false).unwrap());
    ring.push(1, || ()).unwrap();
    ring.push(2, || ()).unwrap();
    let room = Arc::new(AtomicUsize::new(0));
    let poppers: Vec<_> = (0..2)
      .map(|_| {
        let (ring, room) = (ring.clone(), room.clone());
        thread::spawn(move || {
          let item = ring.pop().unwrap().item;
          room.fetch_add(1, SeqCst);
          item
        })
      })
      .collect();
    let pusher = {
      let (ring, room) = (ring.clone(), room.clone());
      thread::spawn(move || {
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
        ring.push(3, || ()).unwrap()
      })
    };
    let mut items: Vec<u32> = poppers.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(pusher.join().unwrap(), 2);
    items.push(ring.pop().unwrap().item);
    items.sort_unstable();
    assert_eq!(items, [1, 2, 3]);
  });
}
