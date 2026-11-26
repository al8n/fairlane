use std::{
  sync::Arc,
  task::{Wake, Waker},
};

use super::*;

/// A waker of its own task: wakers made by separate calls never `will_wake` each other, and a clone
/// `will_wake` its original, so the tests tell waiters apart by their wakers.
struct Task;

impl Wake for Task {
  fn wake(self: Arc<Self>) {}
}

fn task(_: usize) -> Waker {
  Waker::from(Arc::new(Task))
}

#[test]
fn notifies_oldest_first() {
  let wakers: Vec<Waker> = (0..3).map(task).collect();
  let mut waiters = Waiters::new();
  let keys: Vec<Key> = wakers.iter().map(|w| waiters.insert(w.clone())).collect();
  assert_eq!(waiters.queued(), 3);
  for (index, waker) in wakers.iter().enumerate() {
    let woken = waiters
      .notify_oldest()
      .expect("someone is in line")
      .unwrap();
    assert!(
      woken.will_wake(waker),
      "waiter {index} is notified in its turn"
    );
    assert_eq!(waiters.state(&keys[index]), State::Notified);
  }
  assert!(waiters.notify_oldest().is_none());
  assert_eq!((waiters.queued(), waiters.notified()), (0, 3));
  for key in keys {
    assert!(matches!(waiters.remove(key), (State::Notified, None)));
  }
  assert_eq!(waiters.notified(), 0);
}

#[test]
fn a_waiter_leaves_from_the_middle_of_the_line() {
  let wakers: Vec<Waker> = (0..3).map(task).collect();
  let mut waiters = Waiters::new();
  let mut keys: Vec<Option<Key>> = wakers
    .iter()
    .map(|w| Some(waiters.insert(w.clone())))
    .collect();
  let (state, waker) = waiters.remove(keys[1].take().unwrap());
  assert_eq!(state, State::Queued);
  assert!(waker.unwrap().will_wake(&wakers[1]));
  assert_eq!(waiters.queued(), 2);
  assert!(
    waiters
      .notify_oldest()
      .unwrap()
      .unwrap()
      .will_wake(&wakers[0])
  );
  assert!(
    waiters
      .notify_oldest()
      .unwrap()
      .unwrap()
      .will_wake(&wakers[2])
  );
  assert!(waiters.notify_oldest().is_none());
}

#[test]
fn a_freed_place_is_used_again_and_goes_to_the_back() {
  let wakers: Vec<Waker> = (0..3).map(task).collect();
  let mut waiters = Waiters::new();
  let first = waiters.insert(wakers[0].clone());
  let second = waiters.insert(wakers[1].clone());
  let _ = waiters.remove(first);
  let third = waiters.insert(wakers[2].clone());
  assert_eq!(waiters.slots.len(), 2, "the freed place is used again");
  assert!(
    waiters
      .notify_oldest()
      .unwrap()
      .unwrap()
      .will_wake(&wakers[1])
  );
  assert!(
    waiters
      .notify_oldest()
      .unwrap()
      .unwrap()
      .will_wake(&wakers[2])
  );
  let _ = (waiters.remove(second), waiters.remove(third));
}

#[test]
fn close_all_closes_every_waiter_and_counts_the_notified() {
  let wakers: Vec<Waker> = (0..4).map(task).collect();
  let mut waiters = Waiters::new();
  let keys: Vec<Key> = wakers.iter().map(|w| waiters.insert(w.clone())).collect();
  let _ = waiters.notify_oldest();
  let mut woken = Vec::new();
  assert_eq!(waiters.close_all(&mut woken), 1, "one grant to settle");
  assert_eq!(woken.len(), 3, "the waiters still in line are woken");
  assert_eq!((waiters.queued(), waiters.notified()), (0, 0));
  assert!(waiters.notify_oldest().is_none());
  for key in keys {
    assert!(matches!(waiters.remove(key), (State::Closed, None)));
  }
}

#[test]
fn notify_all_notifies_the_line_in_order() {
  let wakers: Vec<Waker> = (0..3).map(task).collect();
  let mut waiters = Waiters::new();
  let keys: Vec<Key> = wakers.iter().map(|w| waiters.insert(w.clone())).collect();
  let mut woken = Vec::new();
  waiters.notify_all(&mut woken);
  assert_eq!(woken.len(), 3);
  for (woken, waker) in woken.iter().zip(&wakers) {
    assert!(woken.will_wake(waker));
  }
  assert_eq!((waiters.queued(), waiters.notified()), (0, 3));
  for key in keys {
    assert_eq!(waiters.state(&key), State::Notified);
    let _ = waiters.remove(key);
  }
}

#[test]
fn a_stored_waker_is_replaced_and_handed_back() {
  let old = task(0);
  let new = task(1);
  let mut waiters = Waiters::new();
  let key = waiters.insert(old.clone());
  assert!(waiters.will_wake(&key, &old));
  assert!(!waiters.will_wake(&key, &new));
  let replaced = waiters.set_waker(&key, new.clone()).unwrap();
  assert!(replaced.will_wake(&old));
  assert!(waiters.will_wake(&key, &new));
  let (state, stored) = waiters.remove(key);
  assert_eq!(state, State::Queued);
  assert!(stored.unwrap().will_wake(&new));
}
