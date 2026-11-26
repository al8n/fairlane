use core::{fmt, marker::PhantomData};

use hybrid_array::Array;
use serde_core::{
  Deserialize, Deserializer, Serialize, Serializer,
  de::{self, DeserializeSeed, MapAccess, Visitor},
  ser::{self, SerializeMap},
};

use super::{Lane, PerLane, lane_count};

impl<X: Serialize, P: Lane> Serialize for PerLane<X, P> {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(lane_count::<P>()))?;
    for (index, value) in self.as_slice().iter().enumerate() {
      let lane =
        P::from_index(index).ok_or_else(|| ser::Error::custom(BrokenLane::<P>::at(index)))?;
      map.serialize_entry(lane.as_str(), value)?;
    }
    map.end()
  }
}

impl<'de, X: Deserialize<'de>, P: Lane> Deserialize<'de> for PerLane<X, P> {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    deserializer.deserialize_map(PerLaneVisitor(PhantomData))
  }
}

struct PerLaneVisitor<X, P>(PhantomData<fn() -> (X, P)>);

impl<'de, X: Deserialize<'de>, P: Lane> Visitor<'de> for PerLaneVisitor<X, P> {
  type Value = PerLane<X, P>;

  fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("a map with one entry for each lane, keyed by the lane's name")
  }

  fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
    let mut slots: Array<Option<X>, P::N> = Array::from_fn(|_| None);
    while let Some(index) = map.next_key_seed(LaneKey::<P>(PhantomData))? {
      if slots[index].is_some() {
        return Err(de::Error::custom(DuplicateLane::<P>::at(index)));
      }
      slots[index] = Some(map.next_value()?);
    }
    match Array::try_from_fn(|index| slots[index].take().ok_or(index)) {
      Ok(values) => Ok(PerLane::from_array(values)),
      Err(index) => Err(match P::from_index(index) {
        Some(lane) => de::Error::missing_field(lane.as_str()),
        None => de::Error::custom(BrokenLane::<P>::at(index)),
      }),
    }
  }
}

/// Reads a map key as the index of the lane it names.
struct LaneKey<P>(PhantomData<fn() -> P>);

impl<'de, P: Lane> DeserializeSeed<'de> for LaneKey<P> {
  type Value = usize;

  fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<usize, D::Error> {
    deserializer.deserialize_str(self)
  }
}

impl<P: Lane> Visitor<'_> for LaneKey<P> {
  type Value = usize;

  fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("the name of a lane")
  }

  fn visit_str<E: de::Error>(self, name: &str) -> Result<usize, E> {
    (0..lane_count::<P>())
      .find(|&index| P::from_index(index).is_some_and(|lane| lane.as_str() == name))
      .ok_or_else(|| {
        E::custom(UnknownLane::<P> {
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

struct UnknownLane<'a, P> {
  name: &'a str,
  lane: PhantomData<P>,
}

impl<P: Lane> fmt::Display for UnknownLane<'_, P> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "unknown lane `{}`, expected one of", self.name)?;
    for (position, lane) in (0..lane_count::<P>()).filter_map(P::from_index).enumerate() {
      let separator = if position == 0 { " " } else { ", " };
      write!(f, "{separator}`{}`", lane.as_str())?;
    }
    Ok(())
  }
}

struct DuplicateLane<P> {
  index: usize,
  lane: PhantomData<P>,
}

impl<P> DuplicateLane<P> {
  fn at(index: usize) -> Self {
    Self {
      index,
      lane: PhantomData,
    }
  }
}

impl<P: Lane> fmt::Display for DuplicateLane<P> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match P::from_index(self.index) {
      Some(lane) => write!(f, "duplicate lane `{}`", lane.as_str()),
      None => write!(f, "duplicate lane at index {}", self.index),
    }
  }
}

struct BrokenLane<P> {
  index: usize,
  lane: PhantomData<P>,
}

impl<P> BrokenLane<P> {
  fn at(index: usize) -> Self {
    Self {
      index,
      lane: PhantomData,
    }
  }
}

impl<P: Lane> fmt::Display for BrokenLane<P> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "`{}::from_index({})` is `None` below its `Lane::N`",
      core::any::type_name::<P>(),
      self.index
    )
  }
}
