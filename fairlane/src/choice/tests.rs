use core::num::NonZeroU32;

use super::*;
use crate::{
  policy::{PassLimit, Strict},
  testing::Three,
};

/// A policy that always names the same lane, in the view or not.
struct Names(Three);

impl Policy<Three> for Names {
  type Stamp = ();

  fn stamp(&self) {}

  fn owed(&self, _: &LaneView<Three, ()>) -> Option<Three> {
    Some(self.0)
  }
}

fn view(lanes: &[Three]) -> LaneView<Three, ()> {
  lanes.iter().fold(LaneView::new(()), |view, &lane| {
    view.with_lane(lane, 1, Some(()), 0)
  })
}

#[test]
fn choose_takes_the_first_lane_unless_one_is_owed() {
  assert_eq!(
    choose(&view(&[Three::B, Three::C]), &Strict),
    Some(Three::B)
  );
  assert_eq!(
    choose(&view(&[Three::B, Three::C]), &Names(Three::C)),
    Some(Three::C)
  );
}

#[test]
fn choose_ignores_an_owed_lane_outside_the_view() {
  assert_eq!(
    choose(&view(&[Three::B, Three::C]), &Names(Three::A)),
    Some(Three::B)
  );
}

#[test]
fn choose_finds_nothing_in_an_empty_view() {
  assert_eq!(choose(&view(&[]), &Names(Three::A)), None);
}

#[test]
fn count_resets_the_served_lane() {
  let mut passed = PerLane::<u32, Three>::from_fn(|_| 5);
  count(&mut passed, Three::B, &view(&[Three::A, Three::B]));
  assert_eq!(passed.as_slice(), &[6, 0, 5]);
}

#[test]
fn count_touches_only_the_view() {
  let mut passed = PerLane::<u32, Three>::default();
  count(&mut passed, Three::A, &view(&[Three::A, Three::C]));
  assert_eq!(*passed.get(Three::B), 0);
  assert_eq!(*passed.get(Three::C), 1);
}

#[test]
fn count_saturates() {
  let mut passed = PerLane::<u32, Three>::from_fn(|_| u32::MAX);
  count(&mut passed, Three::A, &view(&[Three::A, Three::B]));
  assert_eq!(passed.as_slice(), &[0, u32::MAX, u32::MAX]);
}

/// With two lanes always waiting and a limit `f` on the second, the second lane is passed over
/// exactly `f` times between two of its own.
#[test]
fn choose_and_count_serve_a_limited_lane_every_f_plus_one() {
  let f = 3;
  let policy = PassLimit::<Three>::new(NonZeroU32::new(f).unwrap()).with_lane(Three::A, None);
  let mut passed = PerLane::<u32, Three>::default();
  let mut served = Vec::new();
  for _ in 0..12 {
    let view = [Three::A, Three::B]
      .iter()
      .fold(LaneView::new(()), |view, &lane| {
        view.with_lane(lane, 1, Some(()), *passed.get(lane))
      });
    let lane = choose(&view, &policy).unwrap();
    count(&mut passed, lane, &view);
    served.push(lane);
  }
  let positions: Vec<usize> = served
    .iter()
    .enumerate()
    .filter(|(_, lane)| **lane == Three::B)
    .map(|(at, _)| at)
    .collect();
  assert_eq!(positions, [3, 7, 11]);
}

#[test]
fn fits_holds_a_batch_to_its_limit() {
  assert!(fits(0, 32, usize::MAX));
  assert!(fits(31, 32, usize::MAX));
  assert!(!fits(32, 32, usize::MAX));
  assert!(!fits(0, 0, usize::MAX));
}

/// A batch that holds one batch item may not take an interactive item whose limit is one.
#[test]
fn fits_stops_at_x4s_shape() {
  // The quota's limit is 32, and the batch lane's limit is 32: the effective limit stays 32.
  let held = 1;
  let limit = 32;
  assert!(!fits(held, limit, 1));
  assert!(fits(0, 32, 1));
}
