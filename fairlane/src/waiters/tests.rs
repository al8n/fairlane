use std::{
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::SeqCst},
  },
  task::{Wake, Waker},
};

use super::*;

/// One task's count of wakes.
struct Task(AtomicUsize);

impl Wake for Task {
  fn wake(self: Arc<Self>) {
    self.0.fetch_add(1, SeqCst);
  }

  fn wake_by_ref(self: &Arc<Self>) {
    self.0.fetch_add(1, SeqCst);
  }
}

/// Tasks numbered from 0. A waker is told apart by waking it and seeing whose count moved:
/// `Waker::will_wake` is best effort, and under Miri a clone need not `will_wake` its original.
struct Tasks(Vec<Arc<Task>>);

impl Tasks {
  fn new(count: usize) -> Self {
    Self(
      (0..count)
        .map(|_| Arc::new(Task(AtomicUsize::new(0))))
        .collect(),
    )
  }

  fn waker(&self, task: usize) -> Waker {
    Waker::from(self.0[task].clone())
  }

  /// The task `waker` wakes.
  fn which(&self, waker: &Waker) -> usize {
    let before: Vec<usize> = self.0.iter().map(|task| task.0.load(SeqCst)).collect();
    waker.wake_by_ref();
    self
      .0
      .iter()
      .zip(before)
      .position(|(task, before)| task.0.load(SeqCst) != before)
      .expect("the waker belongs to one of the tasks")
  }

  /// The tasks a broadcast left owed, in order, every owed waker taken out.
  fn owed(&self, waiters: &mut Waiters) -> Vec<usize> {
    let mut owed = Vec::new();
    loop {
      let mut chunk: [Option<Waker>; 2] = [None, None];
      let taken = waiters.take_owed(&mut chunk);
      owed.extend(
        chunk[..taken]
          .iter()
          .map(|waker| self.which(waker.as_ref().expect("an owed place holds its waker"))),
      );
      if taken < chunk.len() {
        return owed;
      }
    }
  }

  /// The task of the oldest waiter in line, which `notify_oldest` notifies.
  fn notify(&self, waiters: &mut Waiters) -> usize {
    let woken = waiters
      .notify_oldest()
      .expect("someone is in line")
      .expect("a waiter in line has its waker");
    self.which(&woken)
  }
}

/// Puts `waker` in line as the queues' owners do: room first, then the waiter.
fn insert(waiters: &mut Waiters, waker: Waker) -> Key {
  waiters.make_room().unwrap();
  waiters.insert(waker)
}

#[test]
fn room_is_made_only_where_no_freed_place_is_left() {
  let tasks = Tasks::new(2);
  let mut waiters = Waiters::new();
  let first = insert(&mut waiters, tasks.waker(0));
  let _ = waiters.remove(first);
  let capacity = waiters.slots.capacity();
  waiters.make_room().unwrap();
  assert_eq!(
    waiters.slots.capacity(),
    capacity,
    "a freed place needs no room"
  );
  let second = insert(&mut waiters, tasks.waker(1));
  assert_eq!(waiters.slots.len(), 1);
  let _ = waiters.remove(second);
}

#[test]
fn notifies_oldest_first() {
  let tasks = Tasks::new(3);
  let mut waiters = Waiters::new();
  let keys: Vec<Key> = (0..3)
    .map(|task| insert(&mut waiters, tasks.waker(task)))
    .collect();
  assert_eq!(waiters.queued(), 3);
  for (task, key) in keys.iter().enumerate() {
    assert_eq!(tasks.notify(&mut waiters), task, "notified in its turn");
    assert_eq!(waiters.state(key), State::Notified);
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
  let tasks = Tasks::new(3);
  let mut waiters = Waiters::new();
  let mut keys: Vec<Option<Key>> = (0..3)
    .map(|task| Some(insert(&mut waiters, tasks.waker(task))))
    .collect();
  let (state, waker) = waiters.remove(keys[1].take().unwrap());
  assert_eq!(state, State::Queued);
  assert_eq!(tasks.which(&waker.unwrap()), 1, "its own waker comes back");
  assert_eq!(waiters.queued(), 2);
  assert_eq!(tasks.notify(&mut waiters), 0);
  assert_eq!(tasks.notify(&mut waiters), 2);
  assert!(waiters.notify_oldest().is_none());
}

#[test]
fn a_freed_place_is_used_again_and_goes_to_the_back() {
  let tasks = Tasks::new(3);
  let mut waiters = Waiters::new();
  let first = insert(&mut waiters, tasks.waker(0));
  let second = insert(&mut waiters, tasks.waker(1));
  let _ = waiters.remove(first);
  let third = insert(&mut waiters, tasks.waker(2));
  assert_eq!(waiters.slots.len(), 2, "the freed place is used again");
  assert_eq!(tasks.notify(&mut waiters), 1);
  assert_eq!(tasks.notify(&mut waiters), 2);
  let _ = (waiters.remove(second), waiters.remove(third));
}

#[test]
fn close_all_closes_every_waiter_and_counts_the_notified() {
  let tasks = Tasks::new(4);
  let mut waiters = Waiters::new();
  let keys: Vec<Key> = (0..4)
    .map(|task| insert(&mut waiters, tasks.waker(task)))
    .collect();
  let _ = waiters.notify_oldest();
  assert_eq!(waiters.close_all(), 1, "one grant to settle");
  assert_eq!(
    tasks.owed(&mut waiters),
    [1, 2, 3],
    "the waiters still in line are owed a wake"
  );
  assert_eq!((waiters.queued(), waiters.notified()), (0, 0));
  assert!(waiters.notify_oldest().is_none());
  for key in keys {
    assert!(matches!(waiters.remove(key), (State::Closed, None)));
  }
}

#[test]
fn notify_all_notifies_the_line_in_order() {
  let tasks = Tasks::new(3);
  let mut waiters = Waiters::new();
  let keys: Vec<Key> = (0..3)
    .map(|task| insert(&mut waiters, tasks.waker(task)))
    .collect();
  waiters.notify_all();
  assert_eq!((waiters.queued(), waiters.notified()), (0, 3));
  assert_eq!(tasks.owed(&mut waiters), [0, 1, 2]);
  for key in keys {
    assert_eq!(waiters.state(&key), State::Notified);
    let _ = waiters.remove(key);
  }
}

#[test]
fn a_stored_waker_is_replaced_and_handed_back() {
  let tasks = Tasks::new(2);
  let mut waiters = Waiters::new();
  let key = insert(&mut waiters, tasks.waker(0));
  assert!(
    !waiters.will_wake(&key, &tasks.waker(1)),
    "another task's waker never will-wakes"
  );
  let replaced = waiters.set_waker(&key, tasks.waker(1)).unwrap();
  assert_eq!(tasks.which(&replaced), 0);
  let (state, stored) = waiters.remove(key);
  assert_eq!(state, State::Queued);
  assert_eq!(tasks.which(&stored.unwrap()), 1);
}

#[test]
fn a_broadcast_needs_no_memory_and_dispatch_wakes_every_chunk() {
  let count = 2 * CHUNK + 3;
  let tasks = Tasks::new(count);
  let line = Mutex::new(Waiters::new());
  let keys: Vec<Key> = (0..count)
    .map(|task| insert(&mut lock(&line), tasks.waker(task)))
    .collect();
  let capacity = lock(&line).slots.capacity();
  lock(&line).close_all();
  assert_eq!(
    lock(&line).slots.capacity(),
    capacity,
    "a close allocates nothing"
  );
  let mut wakes = Wakes::new();
  dispatch(&line, &mut wakes);
  wakes.finish();
  for task in 0..count {
    assert_eq!(tasks.0[task].0.load(SeqCst), 1, "task {task} is woken once");
  }
  let mut line = lock(&line);
  for key in keys {
    assert!(matches!(line.remove(key), (State::Closed, None)));
  }
}

#[test]
fn a_waiter_that_leaves_before_its_owed_wake_takes_its_waker_and_the_rest_stay_owed() {
  let tasks = Tasks::new(3);
  let mut waiters = Waiters::new();
  let mut keys: Vec<Key> = (0..3)
    .map(|task| insert(&mut waiters, tasks.waker(task)))
    .collect();
  waiters.notify_all();
  let (state, waker) = waiters.remove(keys.remove(1));
  assert_eq!(state, State::Notified);
  assert_eq!(tasks.which(&waker.expect("its own waker comes back")), 1);
  assert_eq!(waiters.notified(), 2);
  assert_eq!(tasks.owed(&mut waiters), [0, 2]);
  // A close after the broadcast settles the grants without owing a second wake.
  assert_eq!(waiters.close_all(), 2);
  assert!(tasks.owed(&mut waiters).is_empty());
  for key in keys {
    assert!(matches!(waiters.remove(key), (State::Closed, None)));
  }
}
