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
