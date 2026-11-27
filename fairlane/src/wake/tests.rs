use super::*;

/// A place made for an item that was not there to take is given back: misses past the inline
/// places grow nothing.
#[test]
fn a_missed_pop_grows_no_staging() {
  let mut staged = Staged::new();
  for _ in 0..(2 * STAGED_INLINE) {
    staged.room();
    staged.unroom();
  }
  assert_eq!((staged.made, staged.used), (0, 0));
  assert_eq!(staged.more.capacity(), 0, "nothing grew");
}
