use core::{fmt, num::NonZeroUsize};

use serde_core::{
  Deserialize, Deserializer, Serialize, Serializer,
  de::{self, MapAccess, SeqAccess, Visitor},
  ser::SerializeStruct,
};

use super::{LaneOptions, Options};
use crate::lane::{Lane, PerLane};

const FIELDS: &[&str] = &["capacity", "express"];

/// A human-readable format writes a map: `capacity`, and `express` only where the options say,
/// as the `Option` it is, which the reader asks for. A format that is not self-describing reads a
/// struct by position, so there both fields are always written, `express` as an explicit option.
impl Serialize for LaneOptions {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
      let fields = 1 + usize::from(self.express.is_some());
      let mut state = serializer.serialize_struct("LaneOptions", fields)?;
      state.serialize_field("capacity", &self.capacity)?;
      match self.express {
        Some(_) => state.serialize_field("express", &self.express)?,
        None => state.skip_field("express")?,
      }
      state.end()
    } else {
      let mut state = serializer.serialize_struct("LaneOptions", FIELDS.len())?;
      state.serialize_field("capacity", &self.capacity)?;
      state.serialize_field("express", &self.express)?;
      state.end()
    }
  }
}

/// `capacity` is required; `express` may be missing or null, and then the lane's declaration
/// decides. An unknown field is refused. In a sequence, the fields come in their order.
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

  fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<LaneOptions, A::Error> {
    let capacity = seq
      .next_element::<NonZeroUsize>()?
      .ok_or_else(|| de::Error::invalid_length(0, &self))?;
    let express = seq.next_element::<Option<bool>>()?.flatten();
    Ok(LaneOptions { capacity, express })
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

  /// A field by its index, as a packed format writes it: serde's own identifier shape.
  fn visit_u64<E: de::Error>(self, index: u64) -> Result<Field, E> {
    match index {
      0 => Ok(Field::Capacity),
      1 => Ok(Field::Express),
      _ => Err(E::invalid_value(
        de::Unexpected::Unsigned(index),
        &"field index 0 <= i < 2",
      )),
    }
  }

  /// A field by its name in bytes.
  fn visit_bytes<E: de::Error>(self, name: &[u8]) -> Result<Field, E> {
    match name {
      b"capacity" => Ok(Field::Capacity),
      b"express" => Ok(Field::Express),
      _ => Err(E::unknown_field(&String::from_utf8_lossy(name), FIELDS)),
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
