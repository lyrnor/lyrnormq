//! Bounds are enforced while parsing persistent metadata, before growing vectors.
use serde::{
    Deserialize, Deserializer,
    de::{self, IgnoredAny, SeqAccess, Visitor},
};
use std::{fmt, marker::PhantomData};
fn bounded<'de, D, T, const N: usize>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T, const N: usize>(PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for Bounded<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "at most {N} journal metadata elements")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            if seq.size_hint().is_some_and(|n| n > N) {
                return Err(de::Error::custom("journal metadata count exceeds bound"));
            }
            let mut values = Vec::new();
            while values.len() < N {
                match seq.next_element()? {
                    Some(value) => values.push(value),
                    None => return Ok(values),
                }
            }
            if seq.next_element::<IgnoredAny>()?.is_some() {
                return Err(de::Error::custom("journal metadata count exceeds bound"));
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Bounded::<T, N>(PhantomData))
}
pub(crate) fn records<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<crate::model::StoredRecord>, D::Error> {
    bounded::<D, _, 64>(d)
}
pub(crate) fn bindings<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<crate::model::StoredBinding>, D::Error> {
    bounded::<D, _, 32>(d)
}
pub(crate) fn ids<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
    bounded::<D, _, 64>(d)
}
pub(crate) fn descriptor<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    bounded::<D, _, 65536>(d)
}
pub(crate) fn empty_entries<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<crate::model::Entry>, D::Error> {
    bounded::<D, _, 0>(d)
}

pub(crate) fn results<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<crate::model::StoredResult>, D::Error> {
    bounded::<D, _, 64>(d)
}
pub(crate) fn consumers<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<crate::model::StoredConsumer>, D::Error> {
    bounded::<D, _, 32>(d)
}
