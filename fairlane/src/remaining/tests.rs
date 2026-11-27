use super::*;

/// Every word of capacity 0..=4, with each pair of flags.
#[test]
fn every_word() {
  for free_units in 0..=4 {
    for flags in [0, WAITING, CLOSED, WAITING | CLOSED] {
      let current = word(free_units, flags);
      match take(current) {
        Ok(next) => {
          assert_eq!(flags, 0, "take with a flag set: {current:#b}");
          assert!(free_units > 0);
          assert_eq!(free(next), free_units - 1);
          assert_eq!(next & (WAITING | CLOSED), 0);
        }
        Err(Refusal::Closed) => assert_ne!(flags & CLOSED, 0),
        Err(Refusal::Busy) => {
          assert_eq!(flags & CLOSED, 0);
          assert!(flags & WAITING != 0 || free_units == 0);
        }
      }
      match give(current) {
        Give::Added(next) => {
          assert_eq!(flags & WAITING, 0, "added beside a waiter: {current:#b}");
          assert_eq!(free(next), free_units + 1);
          assert_eq!(next & (WAITING | CLOSED), flags);
        }
        Give::Grant => assert_ne!(flags & WAITING, 0),
      }
    }
  }
}

/// The line's invariant on an open lane: `WAITING` exactly while a reserve is in line, and only
/// where `free` is 0. On a closed lane: nobody in line and no `WAITING`.
fn holds(word: usize, queued: usize) -> bool {
  if word & CLOSED != 0 {
    word & WAITING == 0 && queued == 0
  } else {
    (word & WAITING != 0) == (queued > 0) && (word & WAITING == 0 || free(word) == 0)
  }
}

/// Every line state that keeps the invariant, over free units 0..=3 and 0..=3 reserves in line.
fn states() -> impl Iterator<Item = (usize, usize)> {
  (0..=3).flat_map(|free_units| {
    [0, WAITING, CLOSED, WAITING | CLOSED]
      .into_iter()
      .flat_map(move |flags| (0..=3).map(move |queued| (word(free_units, flags), queued)))
      .filter(|&(word, queued)| holds(word, queued))
  })
}

/// Each transition of the line, from every state that keeps the invariant, leads to one that
/// keeps it, and moves only what it says it moves.
#[test]
fn the_line_transitions_keep_its_invariant() {
  for (current, queued) in states() {
    match join(current) {
      Join::Closed => assert_ne!(current & CLOSED, 0),
      Join::Take(next) => {
        assert_eq!(free(next), free(current) - 1);
        assert!(holds(next, queued), "take: {current:#b}, {queued}");
      }
      Join::Queue(next) => {
        assert_eq!(free(next), free(current));
        assert!(holds(next, queued + 1), "queue: {current:#b}, {queued}");
      }
    }
    if current & CLOSED == 0 {
      match back(current, queued) {
        Back::Grant(next) => {
          assert!(queued > 0);
          assert_eq!(free(next), free(current), "a grant moves no free unit");
          assert!(holds(next, queued - 1), "grant: {current:#b}, {queued}");
        }
        Back::Add(next) => {
          assert_eq!(queued, 0);
          assert_eq!(free(next), free(current) + 1);
          assert!(holds(next, 0), "add: {current:#b}");
        }
      }
      if queued > 0 {
        assert!(
          holds(left(current, queued - 1), queued - 1),
          "leave: {current:#b}, {queued}"
        );
      }
      for granted in 0..=2 {
        let next = closed(current, granted);
        assert_eq!(free(next), free(current) + granted);
        assert!(holds(next, 0), "close: {current:#b}, {granted}");
      }
    }
  }
}

#[test]
fn the_atomic_word_takes_and_gives() {
  let remaining = Remaining::new(2);
  assert_eq!(remaining.try_take(), Ok(()));
  assert_eq!(remaining.try_take(), Ok(()));
  assert_eq!(remaining.try_take(), Err(Refusal::Busy));
  assert_eq!(remaining.try_give(), Ok(()));
  assert_eq!(free(remaining.load()), 1);
  remaining.update(|current| current | WAITING);
  assert_eq!(remaining.try_give(), Err(()));
  assert_eq!(remaining.try_take(), Err(Refusal::Busy));
  remaining.update(|current| current & !WAITING | CLOSED);
  assert_eq!(remaining.try_take(), Err(Refusal::Closed));
  assert_eq!(remaining.try_give(), Ok(()));
  assert_eq!(remaining.replace(word(9, 0), 0), Err(remaining.load()));
}
