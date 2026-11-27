use super::*;

#[test]
fn moment_saturating_since() {
  let early = Moment::from_nanos(5);
  let late = Moment::from_nanos(2_000_000_005);
  assert_eq!(late.saturating_since(early), Duration::from_secs(2));
  assert_eq!(early.saturating_since(late), Duration::ZERO);
  assert_eq!(late.as_nanos(), 2_000_000_005);
  assert!(early < late);
}

#[test]
fn saturating_nanos_caps_at_u64_max() {
  assert_eq!(saturating_nanos(Duration::MAX), u64::MAX);
  assert_eq!(saturating_nanos(Duration::from_nanos(7)), 7);
}

#[test]
fn monotonic_never_goes_back() {
  let clock = Monotonic::new();
  let mut last = clock.now();
  for _ in 0..1000 {
    let now = clock.now();
    assert!(now >= last);
    last = now;
  }
}

#[cfg(feature = "test-util")]
#[test]
fn manual_clock_moves_only_when_told() {
  let clock = ManualClock::new();
  let shared = clock.clone();
  assert_eq!(clock.now(), Moment::from_nanos(0));
  clock.advance(Duration::from_millis(3));
  assert_eq!(shared.now(), Moment::from_nanos(3_000_000));
  shared.set(Moment::from_nanos(u64::MAX - 1));
  clock.advance(Duration::from_secs(1));
  assert_eq!(clock.now(), Moment::from_nanos(u64::MAX));
}
