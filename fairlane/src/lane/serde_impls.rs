use core::{fmt, marker::PhantomData};

use hybrid_array::Array;
use serde_core::{
  Deserialize, Deserializer, Serialize, Serializer,
  de::{self, DeserializeSeed, MapAccess, Visitor},
  ser::{self, SerializeMap},
};

use super::{Lane, PerLane, lane_count};

impl<T: Serialize, L: Lane> Serialize for PerLane<T, L> {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(lane_count::<L>()))?;
    for (index, value) in self.as_slice().iter().enumerate() {
      let lane =
        L::from_index(index).ok_or_else(|| ser::Error::custom(BrokenLane::<L>::at(index)))?;
      map.serialize_entry(lane.as_str(), value)?;
    }
    map.end()
  }
}

impl<'de, T: Deserialize<'de>, L: Lane> Deserialize<'de> for PerLane<T, L> {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_map(PerLaneVisitor(PhantomData))
  }
}

struct PerLaneVisitor<T, L>(PhantomData<fn() -> (T, L)>);

impl<'de, T: Deserialize<'de>, L: Lane> Visitor<'de> for PerLaneVisitor<T, L> {
  type Value = PerLane<T, L>;

  fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("a map with one entry for each lane, keyed by the lane's name")
  }

  fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
    let mut slots: Array<Option<T>, L::N> = Array::from_fn(|_| None);
    while let Some(index) = map.next_key_seed(LaneKey::<L>(PhantomData))? {
      if slots[index].is_some() {
        return Err(de::Error::custom(DuplicateLane::<L>::at(index)));
      }
      slots[index] = Some(map.next_value()?);
    }
    match Array::try_from_fn(|index| slots[index].take().ok_or(index)) {
      Ok(values) => Ok(PerLane::from_array(values)),
      Err(index) => Err(match L::from_index(index) {
        Some(lane) => de::Error::missing_field(lane.as_str()),
        None => de::Error::custom(BrokenLane::<L>::at(index)),
      }),
    }
  }
}

/// Reads a map key as the index of the lane it names.
struct LaneKey<L>(PhantomData<fn() -> L>);

impl<'de, L: Lane> DeserializeSeed<'de> for LaneKey<L> {
  type Value = usize;

  fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<usize, D::Error> {
    deserializer.deserialize_str(self)
  }
}

impl<L: Lane> Visitor<'_> for LaneKey<L> {
  type Value = usize;

  fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("the name of a lane")
  }

  fn visit_str<E: de::Error>(self, name: &str) -> Result<usize, E> {
    (0..lane_count::<L>())
      .find(|&index| L::from_index(index).is_some_and(|lane| lane.as_str() == name))
      .ok_or_else(|| {
        E::custom(UnknownLane::<L> {
          name,
          lane: PhantomData,
        })
      })
  }

  fn visit_bytes<E: de::Error>(self, name: &[u8]) -> Result<usize, E> {
    match core::str::from_utf8(name) {
      Ok(name) => self.visit_str(name),
      Err(_) => Err(E::invalid_value(de::Unexpected::Bytes(name), &self)),
    }
  }
}

struct UnknownLane<'a, L> {
  name: &'a str,
  lane: PhantomData<L>,
}

impl<L: Lane> fmt::Display for UnknownLane<'_, L> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "unknown lane `{}`, expected one of", self.name)?;
    for (position, lane) in (0..lane_count::<L>()).filter_map(L::from_index).enumerate() {
      let separator = if position == 0 { " " } else { ", " };
      write!(f, "{separator}`{}`", lane.as_str())?;
    }
    Ok(())
  }
}

struct DuplicateLane<L> {
  index: usize,
  lane: PhantomData<L>,
}

impl<L> DuplicateLane<L> {
  fn at(index: usize) -> Self {
    Self {
      index,
      lane: PhantomData,
    }
  }
}

impl<L: Lane> fmt::Display for DuplicateLane<L> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match L::from_index(self.index) {
      Some(lane) => write!(f, "duplicate lane `{}`", lane.as_str()),
      None => write!(f, "duplicate lane at index {}", self.index),
    }
  }
}

struct BrokenLane<L> {
  index: usize,
  lane: PhantomData<L>,
}

impl<L> BrokenLane<L> {
  fn at(index: usize) -> Self {
    Self {
      index,
      lane: PhantomData,
    }
  }
}

impl<L: Lane> fmt::Display for BrokenLane<L> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "`{}::from_index({})` is `None` below its `Lane::N`",
      core::any::type_name::<L>(),
      self.index
    )
  }
}
