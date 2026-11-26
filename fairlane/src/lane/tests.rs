use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Urgency {
  Interactive,
  Batch,
  Bulk,
}

impl Lane for Urgency {
  type N = typenum::U3;

  fn index(self) -> usize {
    self as usize
  }

  fn from_index(index: usize) -> Option<Self> {
    [Self::Interactive, Self::Batch, Self::Bulk]
      .get(index)
      .copied()
  }

  fn as_str(self) -> &'static str {
    match self {
      Self::Interactive => "interactive",
      Self::Batch => "batch",
      Self::Bulk => "bulk",
    }
  }
}

/// `from_index` is `None` at 1, below `N`: a broken vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Gapped;

impl Lane for Gapped {
  type N = typenum::U2;

  fn index(self) -> usize {
    0
  }

  fn from_index(index: usize) -> Option<Self> {
    (index == 0).then_some(Self)
  }

  fn as_str(self) -> &'static str {
    "gapped"
  }
}

#[test]
fn single_round_trips() {
  assert_eq!(lane_count::<Single>(), 1);
  assert_eq!(Single.index(), 0);
  assert_eq!(Single::from_index(0), Some(Single));
  assert_eq!(Single::from_index(1), None);
  assert_eq!(Single.as_str(), "single");
  assert!(!Single.is_express());
}

#[test]
fn per_lane_reads_by_lane() {
  let mut table = PerLane::<usize, Urgency>::from_fn(|lane| lane.index() * 10);
  assert_eq!(*table.get(Urgency::Bulk), 20);
  *table.get_mut(Urgency::Batch) = 7;
  let entries: Vec<(Urgency, usize)> = table.iter().map(|(lane, value)| (lane, *value)).collect();
  assert_eq!(
    entries,
    [
      (Urgency::Interactive, 0),
      (Urgency::Batch, 7),
      (Urgency::Bulk, 20)
    ]
  );
  assert_eq!(table.as_slice(), &[0, 7, 20]);
  assert_eq!(*table.at(1), 7);
}

#[test]
fn per_lane_debug_is_a_map_by_name() {
  let table = PerLane::<u8, Urgency>::from_fn(|lane| lane.index() as u8);
  assert_eq!(
    format!("{table:?}"),
    r#"{"interactive": 0, "batch": 1, "bulk": 2}"#
  );
}

#[test]
fn per_lane_filled_and_default_never_call_the_lane() {
  let filled = PerLane::<u8, Gapped>::filled(3);
  assert_eq!(filled.as_slice(), &[3, 3]);
  let default = PerLane::<u8, Gapped>::default();
  assert_eq!(default.as_slice(), &[0, 0]);
  assert_eq!(filled.clone(), filled);
  assert_ne!(filled, default);
}

#[test]
#[should_panic(expected = "breaks `Lane`'s contract")]
fn per_lane_from_fn_panics_only_for_a_broken_lane() {
  let _ = PerLane::<u8, Gapped>::from_fn(|_| 0);
}

#[test]
fn per_lane_iter_skips_what_a_broken_lane_cannot_name() {
  let table = PerLane::<u8, Gapped>::filled(1);
  assert_eq!(table.iter().count(), 1);
}

/// Not under miri: they reach no `unsafe` of this crate, and serde_json's own error path trips
/// miri's symbolic alignment check on some targets, which miri itself calls a possible false
/// positive.
#[cfg(all(feature = "serde", not(miri)))]
mod serde_tests {
  use super::*;

  #[test]
  fn serde_round_trips_by_name() {
    let table = PerLane::<u32, Urgency>::from_fn(|lane| lane.index() as u32 + 1);
    let json = serde_json::to_string(&table).unwrap();
    assert_eq!(json, r#"{"interactive":1,"batch":2,"bulk":3}"#);
    let back: PerLane<u32, Urgency> = serde_json::from_str(&json).unwrap();
    assert_eq!(back, table);
  }

  #[test]
  fn serde_reads_lanes_in_any_order() {
    let back: PerLane<u32, Urgency> =
      serde_json::from_str(r#"{"bulk":3,"interactive":1,"batch":2}"#).unwrap();
    assert_eq!(back.as_slice(), &[1, 2, 3]);
  }

  #[test]
  fn serde_refuses_a_missing_lane() {
    let error =
      serde_json::from_str::<PerLane<u32, Urgency>>(r#"{"interactive":1,"bulk":3}"#).unwrap_err();
    assert!(
      error.to_string().contains("missing field `batch`"),
      "{error}"
    );
  }

  #[test]
  fn serde_refuses_an_unknown_lane() {
    let error = serde_json::from_str::<PerLane<u32, Urgency>>(
      r#"{"interactive":1,"batch":2,"bulk":3,"urgent":4}"#,
    )
    .unwrap_err();
    assert!(
      error
        .to_string()
        .contains("unknown lane `urgent`, expected one of `interactive`, `batch`, `bulk`"),
      "{error}"
    );
  }

  #[test]
  fn serde_refuses_a_duplicate_lane() {
    let error = serde_json::from_str::<PerLane<u32, Urgency>>(
      r#"{"interactive":1,"batch":2,"batch":5,"bulk":3}"#,
    )
    .unwrap_err();
    assert!(
      error.to_string().contains("duplicate lane `batch`"),
      "{error}"
    );
  }
}
