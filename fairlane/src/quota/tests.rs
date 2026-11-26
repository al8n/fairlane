use super::*;
use crate::testing::Three;

#[test]
fn quota_up_to_admits_every_lane() {
  let quota = Quota::<Three>::up_to(32);
  assert_eq!(quota.limit(), 32);
  for lane in Three::ALL {
    assert!(quota.admits(lane));
    assert_eq!(quota.at_most_for(lane), 32);
  }
  assert!(quota.admits_any());
}

#[test]
fn quota_at_most_is_capped_by_the_limit() {
  let quota = Quota::<Three>::up_to(8)
    .at_most(Three::A, 1)
    .at_most(Three::B, 100);
  assert_eq!(quota.at_most_for(Three::A), 1);
  assert_eq!(quota.at_most_for(Three::B), 8);
  assert_eq!(quota.at_most_for(Three::C), 8);
}

#[test]
fn quota_at_most_zero_admits_none_of_the_lane() {
  let quota = Quota::<Three>::up_to(8).at_most(Three::B, 0);
  assert!(!quota.admits(Three::B));
  assert!(quota.admits(Three::C));
}

#[test]
fn quota_lanes_through_includes_its_end() {
  let quota = Quota::<Three>::up_to(1).lanes(..=Three::B);
  assert!(quota.admits(Three::A));
  assert!(quota.admits(Three::B));
  assert!(!quota.admits(Three::C));
}

#[test]
fn quota_lanes_before_excludes_its_end() {
  let quota = Quota::<Three>::up_to(1).lanes(..Three::B);
  assert!(quota.admits(Three::A));
  assert!(!quota.admits(Three::B));
}

#[test]
fn quota_lanes_from_skips_the_lanes_before() {
  let quota = Quota::<Three>::up_to(1).lanes(Three::B..);
  assert!(!quota.admits(Three::A));
  assert!(quota.admits(Three::B));
  assert!(quota.admits(Three::C));
  let quota =
    Quota::<Three>::up_to(1).lanes((Bound::Excluded(Three::A), Bound::Included(Three::B)));
  assert!(!quota.admits(Three::A));
  assert!(quota.admits(Three::B));
  assert!(!quota.admits(Three::C));
}

#[test]
fn quota_lanes_replaces_the_earlier_range() {
  let quota = Quota::<Three>::up_to(1)
    .lanes(..=Three::A)
    .lanes(Three::C..);
  assert!(!quota.admits(Three::A));
  assert!(quota.admits(Three::C));
}

#[test]
fn quota_that_admits_nothing() {
  assert!(!Quota::<Three>::up_to(0).admits_any());
  assert!(
    !Quota::<Three>::up_to(4)
      .lanes(Three::C..Three::A)
      .admits_any()
  );
  assert!(
    !Quota::<Three>::up_to(4)
      .lanes(Three::B..Three::B)
      .admits_any()
  );
  let zeroes = Three::ALL
    .iter()
    .fold(Quota::<Three>::up_to(4), |quota, &lane| {
      quota.at_most(lane, 0)
    });
  assert!(!zeroes.admits_any());
  let one_left = Quota::<Three>::up_to(4)
    .at_most(Three::A, 0)
    .lanes(..=Three::A);
  assert!(!one_left.admits_any());
}

#[test]
fn quota_from_a_number_and_a_reference() {
  let quota: Quota<Three> = 32.into();
  assert_eq!(quota, Quota::up_to(32));
  let again: Quota<Three> = (&quota).into();
  assert_eq!(again, quota);
}
