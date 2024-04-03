pub mod actor;
pub mod channel;
pub mod join;
pub mod store;
pub mod window;

use bytes::Bytes;
use nom::AsBytes;
use samsa::prelude::{ConsumeMessage, PartitionOffsets};
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;
use tokio_stream::{Stream, StreamExt};

#[derive(Clone, PartialEq, Debug)]
pub struct ParsedMessage<T: Clone> {
    pub key: String,
    pub value: T,
}

pub fn from_bytes<T>(data: Bytes) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(data.as_bytes())
}

pub fn to_bytes<T>(data: T) -> Result<Bytes, serde_json::Error>
where
    T: Serialize,
{
    Ok(Bytes::from(serde_json::to_string(&data)?))
}

pub fn within_window(a: i64, b: i64, window: Duration) -> bool {
    let t = window.as_millis() as i64;
    (a - b).abs() < t
}

pub fn into_flat_stream(
    stream: impl Stream<Item = samsa::prelude::Result<(Vec<ConsumeMessage>, PartitionOffsets)>>,
) -> impl Stream<Item = ConsumeMessage> {
    futures::StreamExt::flat_map(
        stream
            .filter(|batch| batch.is_ok())
            .map(|batch| batch.unwrap())
            .map(|(batch, _)| batch),
        futures::stream::iter,
    )
}

#[test]
fn test_fuzzy_time_compare() {
    let right_now: i64 = 1701122006000;
    let yesterday: i64 = 1701035606000;

    let one_day = Duration::from_secs(24 * 60 * 60);
    assert_eq!((right_now - yesterday).abs(), one_day.as_millis() as i64);
    assert!(!within_window(right_now, yesterday, one_day / 2));
    assert!(within_window(right_now, yesterday + 1, one_day));
    assert!(within_window(right_now, yesterday, one_day * 2));
}
