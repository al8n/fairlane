use std::panic::catch_unwind;

use super::*;

fn refused(channel: usize) -> bool {
  catch_unwind(|| refuse(channel, "received")).is_err()
}

#[test]
fn a_mark_refuses_its_channel_until_its_guard_drops() {
  let outer = Choosing::enter(1);
  let inner = Choosing::enter(2);
  assert!(refused(1) && refused(2));
  assert!(!refused(3), "a channel not chosen for is not refused");
  drop(outer);
  assert!(!refused(1), "out of order, the right mark goes");
  assert!(refused(2));
  drop(inner);
  assert!(!refused(2));
}

#[test]
fn marks_past_the_table_are_not_kept_and_unmark_nothing() {
  let marks: Vec<Choosing> = (1..=DEPTH + 1).map(Choosing::enter).collect();
  assert!((1..=DEPTH).all(refused), "the first marks are kept");
  assert!(
    !refused(DEPTH + 1),
    "past the table, the check does not mark"
  );
  drop(marks);
  assert!((1..=DEPTH + 1).all(|channel| !refused(channel)));
}
