use super::*;
use crate::testing::Three;

const fn limit(n: u32) -> NonZeroU32 {
  match NonZeroU32::new(n) {
    Some(n) => n,
    None => panic!("zero"),
  }
}

fn counts(a: u64, b: u64, c: u64) -> LaneView<Three, ()> {
  LaneView::new(())
    .with_lane(Three::A, 1, Some(()), a)
    .with_lane(Three::B, 1, Some(()), b)
    .with_lane(Three::C, 1, Some(()), c)
}

fn ms(n: u64) -> Moment {
  Moment::from_nanos(n * 1_000_000)
}

#[test]
fn view_reads_what_it_was_given() {
  let view = LaneView::<Three, Moment>::new(ms(9))
    .with_lane(Three::C, 4, Some(ms(2)), 3)
    .with_lane(Three::A, 1, None, 0);
  assert_eq!(view.lanes().collect::<Vec<_>>(), [Three::A, Three::C]);
  assert_eq!(view.queued(Three::C), 4);
  assert_eq!(view.oldest(Three::C), Some(ms(2)));
  assert_eq!(view.passed_over(Three::C), 3);
  assert_eq!(view.queued(Three::B), 0);
  assert_eq!(view.oldest(Three::B), None);
  assert!(view.contains(Three::A));
  assert!(!view.contains(Three::B));
  assert_eq!(view.now(), ms(9));
  let mapped = view.map(|moment| moment.as_nanos() / 1_000_000);
  assert_eq!(mapped.oldest(Three::C), Some(2));
  assert_eq!(mapped.now(), 9);
  assert_eq!(mapped.passed_over(Three::C), 3);
}

#[test]
fn strict_never_owes() {
  assert_eq!(Policy::<Three>::owed(&Strict, &counts(100, 100, 100)), None);
}

#[test]
fn pass_limit_owes_at_its_limit() {
  let policy = PassLimit::<Three>::new(limit(8));
  assert_eq!(policy.owed(&counts(0, 7, 0)), None);
  assert_eq!(policy.owed(&counts(0, 8, 0)), Some(Three::B));
}

#[test]
fn pass_limit_takes_the_largest_excess() {
  let policy = PassLimit::<Three>::new(limit(8)).with_lane(Three::C, Some(limit(2)));
  assert_eq!(policy.owed(&counts(0, 9, 4)), Some(Three::C));
  assert_eq!(policy.owed(&counts(0, 11, 4)), Some(Three::B));
}

#[test]
fn pass_limit_breaks_a_tie_toward_the_higher_lane() {
  let policy = PassLimit::<Three>::new(limit(8));
  assert_eq!(policy.owed(&counts(0, 10, 10)), Some(Three::B));
}

#[test]
fn pass_limit_compares_the_full_excess_past_a_limit_of_u32_max() {
  let policy = PassLimit::<Three>::new(limit(1)).with_lane(Three::C, Some(limit(u32::MAX)));
  let at_its_limit = u64::from(u32::MAX);
  assert_eq!(
    policy.owed(&counts(1, 0, at_its_limit)),
    Some(Three::A),
    "a tie at no excess goes to the higher lane"
  );
  assert_eq!(
    policy.owed(&counts(1, 0, at_its_limit + 1)),
    Some(Three::C),
    "one pass past `u32::MAX` is an excess of one"
  );
  assert_eq!(policy.owed(&counts(2, 0, at_its_limit + 1)), Some(Three::A));
}

#[test]
fn pass_limit_lets_a_lane_without_a_limit_starve() {
  let policy = PassLimit::<Three>::new(limit(8)).with_lane(Three::C, None);
  assert_eq!(policy.limit(Three::C), None);
  assert_eq!(policy.owed(&counts(0, 0, 1_000)), None);
}

#[test]
fn pass_limit_is_live() {
  let policy = PassLimit::<Three>::new(limit(8));
  assert_eq!(policy.owed(&counts(0, 3, 0)), None);
  policy.set_limit(Three::B, Some(limit(3)));
  assert_eq!(policy.limit(Three::B), Some(limit(3)));
  assert_eq!(policy.owed(&counts(0, 3, 0)), Some(Three::B));
}

#[test]
fn pass_limit_default_is_the_constant() {
  let policy = PassLimit::<Three>::default();
  for lane in Three::ALL {
    assert_eq!(policy.limit(lane), Some(DEFAULT_PASS_LIMIT));
  }
  assert_eq!(DEFAULT_PASS_LIMIT.get(), 8);
}

fn waits(now: u64, b: Option<u64>, c: Option<u64>) -> LaneView<Three, Moment> {
  LaneView::new(ms(now))
    .with_lane(Three::A, 1, Some(ms(now)), 0)
    .with_lane(Three::B, 1, b.map(ms), 0)
    .with_lane(Three::C, 1, c.map(ms), 0)
}

#[test]
fn elapsed_owes_past_max_wait() {
  let policy =
    Elapsed::<Three, Monotonic>::with_clock(Duration::from_millis(100), Monotonic::new());
  assert_eq!(policy.owed(&waits(1_000, Some(950), None)), None);
  assert_eq!(policy.owed(&waits(1_000, Some(850), None)), Some(Three::B));
}

#[test]
fn elapsed_is_not_owed_at_exactly_max_wait() {
  let policy = Elapsed::<Three>::new(Duration::from_millis(100));
  assert_eq!(policy.owed(&waits(1_000, Some(900), None)), None);
}

#[test]
fn elapsed_takes_the_most_overdue() {
  let policy = Elapsed::<Three>::new(Duration::from_millis(100))
    .with_lane(Three::C, Some(Duration::from_millis(10)));
  // B waited 300 (200 over); C waited 50 (40 over).
  assert_eq!(
    policy.owed(&waits(1_000, Some(700), Some(950))),
    Some(Three::B)
  );
  // B waited 150 (50 over); C waited 100 (90 over).
  assert_eq!(
    policy.owed(&waits(1_000, Some(850), Some(900))),
    Some(Three::C)
  );
}

#[test]
fn elapsed_breaks_a_tie_toward_the_higher_lane() {
  let policy = Elapsed::<Three>::new(Duration::from_millis(100));
  assert_eq!(
    policy.owed(&waits(1_000, Some(800), Some(800))),
    Some(Three::B)
  );
}

#[test]
fn elapsed_never_owes_without_a_wait_or_a_stamp() {
  let policy = Elapsed::<Three>::new(Duration::from_millis(100)).with_lane(Three::B, None);
  assert_eq!(policy.max_wait(Three::B), None);
  assert_eq!(policy.owed(&waits(1_000, Some(0), None)), None);
}

#[test]
fn elapsed_is_live() {
  let policy = Elapsed::<Three>::new(Duration::from_secs(10));
  assert_eq!(policy.owed(&waits(1_000, Some(500), None)), None);
  policy.set_max_wait(Three::B, Some(Duration::from_millis(100)));
  assert_eq!(policy.max_wait(Three::B), Some(Duration::from_millis(100)));
  assert_eq!(policy.owed(&waits(1_000, Some(500), None)), Some(Three::B));
}

#[test]
fn elapsed_saturates_a_huge_wait() {
  let policy = Elapsed::<Three>::new(Duration::MAX);
  let wait = policy.max_wait(Three::A).unwrap();
  assert_eq!(wait, Duration::from_nanos(u64::MAX));
  assert_eq!(
    policy.owed(&waits(u64::MAX / 1_000_000, Some(0), None)),
    None
  );
}

/// The largest wait is kept as it was set, apart from having none: a wait of exactly `u64::MAX`
/// nanoseconds is not exceeded by a wait of `u64::MAX` nanoseconds, and reads back unchanged.
#[test]
fn elapsed_keeps_the_largest_wait_apart_from_none() {
  let largest = Duration::from_nanos(u64::MAX);
  let policy = Elapsed::<Three>::new(largest);
  assert_eq!(policy.max_wait(Three::B), Some(largest));
  let view = LaneView::new(Moment::from_nanos(u64::MAX))
    .with_lane(Three::B, 1, Some(Moment::from_nanos(u64::MAX)), 0)
    .with_lane(Three::C, 1, Some(Moment::from_nanos(0)), 0);
  assert_eq!(
    policy.owed(&view),
    None,
    "C waited exactly its wait, which is not more"
  );
  policy.set_max_wait(Three::C, None);
  assert_eq!(policy.max_wait(Three::C), None);
  policy.set_max_wait(Three::C, Some(Duration::from_nanos(u64::MAX - 1)));
  assert_eq!(
    policy.max_wait(Three::C),
    Some(Duration::from_nanos(u64::MAX - 1))
  );
  assert_eq!(policy.owed(&view), Some(Three::C), "one nanosecond over");
}

/// One writer's settings `Some(20 ns)`, then `None`, then `Some(10 ns)` are never seen out of
/// order: neither by `max_wait`, nor by the choices, which serve B, waited 15 ns, only under the
/// last.
#[test]
fn elapsed_settings_are_seen_in_the_order_they_were_made() {
  let rounds = if cfg!(miri) { 8 } else { 2_000 };
  let view = LaneView::<Three, Moment>::new(Moment::from_nanos(15))
    .with_lane(Three::A, 1, Some(Moment::from_nanos(15)), 0)
    .with_lane(Three::B, 1, Some(Moment::from_nanos(0)), 0);
  let stage = |wait: Option<Duration>| match wait {
    Some(wait) if wait == Duration::from_nanos(20) => 0,
    None => 1,
    Some(wait) if wait == Duration::from_nanos(10) => 2,
    Some(other) => panic!("no call set {other:?}"),
  };
  for _ in 0..rounds {
    let policy = std::sync::Arc::new(Elapsed::<Three>::new(Duration::from_nanos(20)));
    let writer = {
      let policy = policy.clone();
      std::thread::spawn(move || {
        policy.set_max_wait(Three::B, None);
        policy.set_max_wait(Three::B, Some(Duration::from_nanos(10)));
      })
    };
    let (mut read, mut served) = (0, false);
    while read < 2 || !served {
      let now = stage(policy.max_wait(Three::B));
      assert!(
        now >= read,
        "`max_wait` went back from stage {read} to {now}"
      );
      assert!(
        !served || now == 2,
        "`max_wait` read behind a choice already served"
      );
      read = now;
      let owed = policy.owed(&view) == Some(Three::B);
      assert!(owed || !served, "a choice went back from the 10 ns wait");
      served |= owed;
      std::thread::yield_now();
    }
    writer.join().unwrap();
  }
}

#[test]
fn elapsed_default_is_the_constant() {
  let policy = Elapsed::<Three>::default();
  assert_eq!(policy.max_wait(Three::C), Some(DEFAULT_MAX_WAIT));
  assert_eq!(DEFAULT_MAX_WAIT, Duration::from_secs(2));
}

#[cfg(feature = "test-util")]
#[test]
fn elapsed_stamps_from_its_clock() {
  let clock = crate::clock::ManualClock::new();
  let policy = Elapsed::<Three, _>::with_clock(DEFAULT_MAX_WAIT, clock.clone());
  clock.set(ms(42));
  assert_eq!(policy.stamp(), ms(42));
}

type Both = FirstOf<PassLimit<Three>, Elapsed<Three>>;

fn both() -> Both {
  FirstOf::new(
    PassLimit::new(limit(2)),
    Elapsed::new(Duration::from_millis(100)),
  )
}

fn stamped(passed_b: u64, passed_c: u64, b: u64, c: u64) -> LaneView<Three, Stamps<(), Moment>> {
  let at = |millis: u64| Stamps {
    first: (),
    second: ms(millis),
  };
  LaneView::new(at(1_000))
    .with_lane(Three::B, 1, Some(at(b)), passed_b)
    .with_lane(Three::C, 1, Some(at(c)), passed_c)
}

#[test]
fn first_of_takes_the_first_operands_lane() {
  // B is owed by count, C by time: the count is asked first.
  assert_eq!(both().owed(&stamped(2, 0, 990, 500)), Some(Three::B));
}

#[test]
fn first_of_asks_the_second_where_the_first_names_none() {
  assert_eq!(both().owed(&stamped(0, 0, 990, 500)), Some(Three::C));
  assert_eq!(both().owed(&stamped(0, 0, 990, 950)), None);
}

#[test]
fn first_of_stamps_one_of_each_and_lends_both_parts() {
  let policy = both();
  let stamp = policy.stamp();
  assert_eq!(stamp.first(), ());
  assert!(stamp.second() <= policy.second().stamp());
  policy.first().set_limit(Three::B, None);
  assert_eq!(policy.first().limit(Three::B), None);
}

#[test]
fn default_policy_is_both_defaults() {
  let policy = DefaultPolicy::<Three>::default();
  assert_eq!(policy.first().limit(Three::A), Some(DEFAULT_PASS_LIMIT));
  assert_eq!(policy.second().max_wait(Three::A), Some(DEFAULT_MAX_WAIT));
}

#[cfg(feature = "either")]
mod either_tests {
  use either::Either;

  use super::*;

  #[test]
  fn either_asks_only_its_side() {
    let left: Either<PassLimit<Three>, Strict> = Either::Left(PassLimit::new(limit(2)));
    let view = counts(0, 2, 0).map(Either::Left);
    assert_eq!(left.owed(&view), Some(Three::B));
    assert_eq!(left.stamp(), Either::Left(()));
    let right: Either<PassLimit<Three>, Strict> = Either::Right(Strict);
    let view = counts(0, 2, 0).map(Either::Right);
    assert_eq!(right.owed(&view), None);
  }

  #[test]
  fn either_reads_a_stamp_of_the_other_side_as_now() {
    let left: Either<Elapsed<Three>, Strict> = Either::Left(Elapsed::new(Duration::from_millis(1)));
    let view = LaneView::<Three, Either<Moment, ()>>::new(Either::Left(ms(1_000))).with_lane(
      Three::B,
      1,
      Some(Either::Right(())),
      0,
    );
    assert_eq!(left.owed(&view), None);
  }
}

#[test]
fn debug_names_the_lanes() {
  let policy = PassLimit::<Three>::new(limit(3)).with_lane(Three::C, None);
  assert_eq!(
    format!("{policy:?}"),
    r#"PassLimit { limits: {"a": Some(3), "b": Some(3), "c": None} }"#
  );
  let view = counts(1, 2, 3);
  assert!(format!("{view:?}").contains(r#""b": queued 1, oldest Some(()), passed over 2"#));
}
