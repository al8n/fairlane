use fairlane::{Lane, PerLane};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Lane)]
enum Urgency {
  #[fairlane(express)]
  Wake,
  Interactive,
  #[fairlane(name = "bulk")]
  BatchJob,
}

mod reexport {
  pub use fairlane;
}

/// The paths of the output are rooted at `crate = "…"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Lane)]
#[fairlane(crate = "crate::reexport::fairlane")]
enum Band {
  One,
  Two,
}

/// A where clause with no generic parameter is kept on the impl.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Lane)]
enum Bounded
where
  u8: Copy,
{
  Only,
}

#[test]
fn derived_lanes_round_trip() {
  let lanes = [Urgency::Wake, Urgency::Interactive, Urgency::BatchJob];
  for (index, lane) in lanes.into_iter().enumerate() {
    assert_eq!(lane.index(), index);
    assert_eq!(Urgency::from_index(index), Some(lane));
  }
  assert_eq!(Urgency::from_index(3), None);
  assert_eq!(Urgency::Interactive.as_str(), "interactive");
  assert_eq!(Urgency::BatchJob.as_str(), "bulk");
  assert!(Urgency::Wake.is_express());
  assert!(!Urgency::Interactive.is_express());
  assert_eq!(Band::Two.index(), 1);
  assert_eq!(Band::from_index(1), Some(Band::Two));
  assert_eq!(Bounded::from_index(0), Some(Bounded::Only));
}

#[test]
fn derived_lanes_size_their_tables() {
  let table = PerLane::<u8, Urgency>::from_fn(|lane| match lane {
    Urgency::Wake => 1,
    Urgency::Interactive => 2,
    Urgency::BatchJob => 3,
  });
  assert_eq!(table.iter().count(), 3);
  assert_eq!(*table.get(Urgency::BatchJob), 3);
}
