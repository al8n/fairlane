use super::*;

const fn cap(n: usize) -> NonZeroUsize {
  match NonZeroUsize::new(n) {
    Some(n) => n,
    None => panic!("zero"),
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
  Wake,
  Interactive,
  Batch,
}

impl Lane for Tier {
  type N = typenum::U3;

  fn index(self) -> usize {
    self as usize
  }

  fn from_index(index: usize) -> Option<Self> {
    [Self::Wake, Self::Interactive, Self::Batch]
      .get(index)
      .copied()
  }

  fn as_str(self) -> &'static str {
    match self {
      Self::Wake => "wake",
      Self::Interactive => "interactive",
      Self::Batch => "batch",
    }
  }

  fn is_express(self) -> bool {
    matches!(self, Self::Wake)
  }
}

/// No lane at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Nothing {}

impl Lane for Nothing {
  type N = typenum::U0;

  fn index(self) -> usize {
    match self {}
  }

  fn from_index(_: usize) -> Option<Self> {
    None
  }

  fn as_str(self) -> &'static str {
    match self {}
  }
}

/// Two lanes whose second index answers the first lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Folded;

impl Lane for Folded {
  type N = typenum::U2;

  fn index(self) -> usize {
    0
  }

  fn from_index(index: usize) -> Option<Self> {
    (index < 2).then_some(Self)
  }

  fn as_str(self) -> &'static str {
    "folded"
  }
}

/// One lane, and `from_index(1)` is `Some` as well.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Overlong;

impl Lane for Overlong {
  type N = typenum::U1;

  fn index(self) -> usize {
    0
  }

  fn from_index(index: usize) -> Option<Self> {
    (index < 2).then_some(Self)
  }

  fn as_str(self) -> &'static str {
    "overlong"
  }
}

fn tiers() -> Options<Tier> {
  Options::from_fn(|lane| match lane {
    Tier::Wake => LaneOptions::new(cap(1)),
    Tier::Interactive => LaneOptions::new(cap(64)),
    Tier::Batch => LaneOptions::new(cap(1024)),
  })
}

#[test]
fn lane_options_builders() {
  let options = LaneOptions::new(cap(8));
  assert_eq!(options.capacity(), cap(8));
  assert_eq!(options.express(), None);
  assert_eq!(options.with_express(true).express(), Some(true));
  assert_eq!(options.with_express(false).express(), Some(false));
}

#[test]
fn options_read_and_replace_by_lane() {
  let options = tiers().with_lane(Tier::Batch, LaneOptions::new(cap(9)));
  assert_eq!(options.lane(Tier::Batch).capacity(), cap(9));
  assert_eq!(options.lane(Tier::Interactive).capacity(), cap(64));
}

#[test]
fn check_accepts_a_good_vocabulary() {
  assert_eq!(tiers().check(), Ok(()));
}

#[test]
fn check_follows_the_declaration_unless_the_options_say_otherwise() {
  let options = tiers();
  assert!(options.is_express_at(0));
  assert!(!options.is_express_at(1));
  let options = options.with_lane(Tier::Wake, LaneOptions::new(cap(1)).with_express(false));
  assert!(!options.is_express_at(0));
  assert_eq!(options.check(), Ok(()));
}

#[test]
fn check_refuses_no_lanes() {
  let options = Options::<Nothing>::from_fn(|lane| match lane {});
  assert_eq!(options.check(), Err(Malformed::NoLanes));
}

#[test]
fn check_refuses_more_lanes_than_a_channel_holds() {
  use crate::testing::Wide;

  let options = Options::<Wide<257>>::from_fn(|_| LaneOptions::new(cap(1)));
  assert_eq!(options.check(), Err(Malformed::TooManyLanes));
  let options = Options::<Wide<256>>::from_fn(|_| LaneOptions::new(cap(1)));
  assert_eq!(options.check(), Ok(()));
}

#[test]
fn check_refuses_indices_that_do_not_round_trip() {
  let options = Options::<Folded>::from_fn(|_| LaneOptions::new(cap(1)));
  assert_eq!(options.check(), Err(Malformed::Indices(1)));
}

#[test]
fn check_refuses_a_lane_past_the_count() {
  let options = Options::<Overlong>::from_fn(|_| LaneOptions::new(cap(1)));
  assert_eq!(options.check(), Err(Malformed::Indices(1)));
}

#[test]
fn check_refuses_express_behind_a_plain_lane() {
  let options = tiers().with_lane(Tier::Batch, LaneOptions::new(cap(4)).with_express(true));
  assert_eq!(options.check(), Err(Malformed::ExpressBehind(2)));
  let options = tiers().with_lane(Tier::Wake, LaneOptions::new(cap(1)).with_express(false));
  let options = options.with_lane(
    Tier::Interactive,
    LaneOptions::new(cap(4)).with_express(true),
  );
  assert_eq!(options.check(), Err(Malformed::ExpressBehind(1)));
}

#[test]
fn check_refuses_a_capacity_above_the_word() {
  let options = tiers().with_lane(Tier::Interactive, LaneOptions::new(cap(MAX_CAPACITY + 1)));
  assert_eq!(options.check(), Err(Malformed::Capacity(1)));
  let options = tiers().with_lane(Tier::Interactive, LaneOptions::new(cap(MAX_CAPACITY)));
  assert_eq!(options.check(), Ok(()));
}

#[test]
fn malformed_says_which_lane() {
  assert_eq!(
    Malformed::ExpressBehind(2).to_string(),
    "lane 2 is express behind a lane that is not"
  );
  assert_eq!(
    Malformed::NoLanes.to_string(),
    "the lane vocabulary has no lane"
  );
  assert_eq!(
    Malformed::TooManyLanes.to_string(),
    format!("the lane vocabulary has more than {MAX_LANES} lanes, the most a channel has")
  );
  assert_eq!(MAX_LANES, 256, "the number the docs write out");
}

/// Not under miri: they reach no `unsafe` of this crate, and serde_json's own error path trips
/// miri's symbolic alignment check on some targets, which miri itself calls a possible false
/// positive.
#[cfg(all(feature = "serde", not(miri)))]
mod serde_tests {
  use super::*;

  #[test]
  fn serde_lane_options_round_trip() {
    let plain = LaneOptions::new(cap(8));
    assert_eq!(serde_json::to_string(&plain).unwrap(), r#"{"capacity":8}"#);
    let express = plain.with_express(true);
    let json = serde_json::to_string(&express).unwrap();
    assert_eq!(json, r#"{"capacity":8,"express":true}"#);
    assert_eq!(serde_json::from_str::<LaneOptions>(&json).unwrap(), express);
  }

  /// A format that is not self-describing, which reads a struct by position, round-trips every
  /// presence of `express` and whole options.
  #[test]
  fn serde_round_trips_a_binary_format() {
    let plain = LaneOptions::new(cap(8));
    for options in [plain, plain.with_express(true), plain.with_express(false)] {
      let bytes = postcard::to_allocvec(&options).unwrap();
      assert_eq!(
        postcard::from_bytes::<LaneOptions>(&bytes).unwrap(),
        options
      );
    }
    let options = tiers().with_lane(Tier::Batch, LaneOptions::new(cap(9)).with_express(false));
    let bytes = postcard::to_allocvec(&options).unwrap();
    assert_eq!(
      postcard::from_bytes::<Options<Tier>>(&bytes).unwrap(),
      options
    );
  }

  /// Every presence of `express` round-trips through serde's own tokens, readable and compact: a
  /// present `express` is written as the `Option` it is, which is what the reader asks for.
  #[test]
  fn serde_tokens_round_trip_every_express() {
    use serde_test::{Configure, Token, assert_tokens};

    let plain = LaneOptions::new(cap(8));
    let head = |len| {
      [
        Token::Struct {
          name: "LaneOptions",
          len,
        },
        Token::Str("capacity"),
        Token::U64(8),
      ]
    };
    assert_tokens(
      &plain.readable(),
      &[&head(1)[..], &[Token::StructEnd]].concat(),
    );
    for express in [true, false] {
      let options = plain.with_express(express);
      let tail = [
        Token::Str("express"),
        Token::Some,
        Token::Bool(express),
        Token::StructEnd,
      ];
      assert_tokens(&options.readable(), &[&head(2)[..], &tail].concat());
      assert_tokens(&options.compact(), &[&head(2)[..], &tail].concat());
    }
    let none = [Token::Str("express"), Token::None, Token::StructEnd];
    assert_tokens(&plain.compact(), &[&head(2)[..], &none].concat());
  }

  /// A field is read by its name, by its index, as a packed format writes it, or by its name in
  /// bytes, as serde's own identifiers are; any other field is still refused.
  #[test]
  fn serde_fields_are_read_by_name_index_or_bytes() {
    use serde_test::{Token, assert_de_tokens, assert_de_tokens_error};

    let express = LaneOptions::new(cap(3)).with_express(true);
    let head = Token::Struct {
      name: "LaneOptions",
      len: 2,
    };
    assert_de_tokens(
      &express,
      &[
        head,
        Token::U64(0),
        Token::U64(3),
        Token::U64(1),
        Token::Some,
        Token::Bool(true),
        Token::StructEnd,
      ],
    );
    assert_de_tokens(
      &express,
      &[
        head,
        Token::Bytes(b"capacity"),
        Token::U64(3),
        Token::Bytes(b"express"),
        Token::Some,
        Token::Bool(true),
        Token::StructEnd,
      ],
    );
    assert_de_tokens_error::<LaneOptions>(
      &[head, Token::U64(2)],
      "invalid value: integer `2`, expected field index 0 <= i < 2",
    );
    assert_de_tokens_error::<LaneOptions>(
      &[head, Token::Bytes(b"urgent")],
      "unknown field `urgent`, expected `capacity` or `express`",
    );
  }

  #[test]
  fn serde_express_may_be_missing_or_null() {
    let missing: LaneOptions = serde_json::from_str(r#"{"capacity":8}"#).unwrap();
    let null: LaneOptions = serde_json::from_str(r#"{"capacity":8,"express":null}"#).unwrap();
    assert_eq!(missing.express(), None);
    assert_eq!(null.express(), None);
  }

  #[test]
  fn serde_capacity_is_required_and_never_zero() {
    let missing = serde_json::from_str::<LaneOptions>(r#"{"express":true}"#).unwrap_err();
    assert!(
      missing.to_string().contains("missing field `capacity`"),
      "{missing}"
    );
    assert!(serde_json::from_str::<LaneOptions>(r#"{"capacity":0}"#).is_err());
  }

  #[test]
  fn serde_refuses_an_unknown_field() {
    let error = serde_json::from_str::<LaneOptions>(r#"{"capacity":8,"priority":1}"#).unwrap_err();
    assert!(
      error.to_string().contains("unknown field `priority`"),
      "{error}"
    );
  }

  #[test]
  fn serde_options_are_a_map_by_lane_name() {
    let options = tiers();
    let json = serde_json::to_string(&options).unwrap();
    assert_eq!(
      json,
      r#"{"wake":{"capacity":1},"interactive":{"capacity":64},"batch":{"capacity":1024}}"#
    );
    assert_eq!(
      serde_json::from_str::<Options<Tier>>(&json).unwrap(),
      options
    );
  }

  #[test]
  fn serde_options_refuse_a_missing_lane() {
    let error =
      serde_json::from_str::<Options<Tier>>(r#"{"wake":{"capacity":1},"batch":{"capacity":1024}}"#)
        .unwrap_err();
    assert!(
      error.to_string().contains("missing field `interactive`"),
      "{error}"
    );
  }
}
