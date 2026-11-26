use core::{fmt, num::NonZeroUsize};

use serde_core::{
  Deserialize, Deserializer, Serialize, Serializer,
  de::{self, MapAccess, Visitor},
  ser::SerializeStruct,
};

use super::{LaneOptions, Options};
use crate::lane::{Lane, PerLane};

const FIELDS: &[&str] = &["capacity", "express"];

impl Serialize for LaneOptions {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    let fields = 1 + usize::from(self.express.is_some());
    let mut state = serializer.serialize_struct("LaneOptions", fields)?;
    state.serialize_field("capacity", &self.capacity)?;
    match self.express {
      Some(express) => state.serialize_field("express", &express)?,
      None => state.skip_field("express")?,
    }
    state.end()
  }
}

/// `capacity` is required; `express` may be missing or null, and then the lane's declaration
/// decides. An unknown field is refused.
impl<'de> Deserialize<'de> for LaneOptions {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_struct("LaneOptions", FIELDS, LaneOptionsVisitor)
  }
}

struct LaneOptionsVisitor;

impl<'de> Visitor<'de> for LaneOptionsVisitor {
  type Value = LaneOptions;

  fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("a lane's options: `capacity`, and optionally `express`")
  }

  fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<LaneOptions, A::Error> {
    let mut capacity: Option<NonZeroUsize> = None;
    let mut express: Option<Option<bool>> = None;
    while let Some(field) = map.next_key::<Field>()? {
      match field {
        Field::Capacity => {
          if capacity.is_some() {
            return Err(de::Error::duplicate_field("capacity"));
          }
          capacity = Some(map.next_value()?);
        }
        Field::Express => {
          if express.is_some() {
            return Err(de::Error::duplicate_field("express"));
          }
          express = Some(map.next_value()?);
        }
      }
    }
    let capacity = capacity.ok_or_else(|| de::Error::missing_field("capacity"))?;
    Ok(LaneOptions {
      capacity,
      express: express.flatten(),
    })
  }
}

enum Field {
  Capacity,
  Express,
}

impl<'de> Deserialize<'de> for Field {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_identifier(FieldVisitor)
  }
}

struct FieldVisitor;

impl Visitor<'_> for FieldVisitor {
  type Value = Field;

  fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("`capacity` or `express`")
  }

  fn visit_str<E: de::Error>(self, name: &str) -> Result<Field, E> {
    match name {
      "capacity" => Ok(Field::Capacity),
      "express" => Ok(Field::Express),
      _ => Err(E::unknown_field(name, FIELDS)),
    }
  }
}

impl<L: Lane> Serialize for Options<L> {
  #[inline]
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    self.lanes.serialize(serializer)
  }
}

/// A map keyed by each lane's [`as_str`](Lane::as_str), every lane required. Whether the options
/// make a channel is [`channel`](crate::channel)'s to say.
impl<'de, L: Lane> Deserialize<'de> for Options<L> {
  #[inline]
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    PerLane::deserialize(deserializer).map(|lanes| Self { lanes })
  }
}
