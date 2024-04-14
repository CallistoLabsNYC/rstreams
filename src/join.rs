// use std::collections::HashMap;
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;

use async_stream::stream;
use tokio_stream::{Stream, StreamExt};

use crate::{store::KVStore, table::RTable, within_window, ParsedMessage};

enum Either<L, R> {
    Left(L),
    Right(R),
}

//
// This funciton joins 2 streams. It uses a keys with a simple time window
// to match up the stream values. If a message with key A comes in on a stream,
// we yield all A messages from the other stream that are within the window.
//
pub async fn inner_join_streams<L, R, F>(
    stream_left: impl Stream<Item = ParsedMessage<L>> + 'static + Send,
    stream_right: impl Stream<Item = ParsedMessage<R>> + 'static + Send,
    high_water_mark: Duration,
    timestamp_accessor: F,
    mut stream_store_left: impl KVStore,
    mut stream_store_right: impl KVStore,
) -> impl Stream<Item = ParsedMessage<(L, R)>>
where
    F: (Fn(&L, &R) -> (i64, i64)),
    L: Clone + Send + Serialize + DeserializeOwned + 'static,
    R: Clone + Send + Serialize + DeserializeOwned + 'static,
{
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let sender_clone = sender.clone();

    // process B
    tokio::spawn(async move {
        tokio::pin!(stream_right);
        while let Some(r) = stream_right.next().await {
            if sender.send(Either::Right(r)).await.is_err() {
                tracing::warn!("Right Side of join hung up channel");
            }
        }
    });

    // process A
    tokio::spawn(async move {
        tokio::pin!(stream_left);
        while let Some(left) = stream_left.next().await {
            if sender_clone.send(Either::Left(left)).await.is_err() {
                tracing::warn!("Left Side of join hung up channel");
            }
        }
    });

    stream! {
        while let Some(message) = receiver.recv().await {
            match message {
                Either::Left(left) => {
                    // insert into Left store
                    match stream_store_left.get::<Vec<L>>(&left.key).unwrap() {
                        None => {
                            stream_store_left.insert(&left.key, vec![left.clone().value]).unwrap();
                        }
                        Some(mut left_events) => {
                            left_events.push(left.clone().value);
                            stream_store_left.insert(&left.key, &left_events).unwrap();
                        }
                    }

                    // check against all Rs
                    if let Some(mut right_events) = stream_store_right.get::<Vec<R>>(&left.key).unwrap() {
                        // prune step
                        right_events.retain(|right| {
                            let (left_timestamp, right_timestamp) = timestamp_accessor(&left.value, right);
                            within_window(right_timestamp, left_timestamp, high_water_mark)
                                || left_timestamp < right_timestamp
                        });
                        for right in right_events.iter() {
                            let (left_timestamp, right_timestamp) = timestamp_accessor(&left.value, right);

                            if within_window(right_timestamp, left_timestamp, high_water_mark) {
                                yield ParsedMessage {
                                    key: left.key.to_string(),
                                    value: (left.value.clone(), right.clone())
                                }
                            }

                            if right_timestamp > left_timestamp
                                && !within_window(right_timestamp, left_timestamp, high_water_mark)
                            {
                                break;
                            }
                        }
                        stream_store_right.insert(&left.key, &right_events).unwrap();
                    }
                },
                Either::Right(right) => {
                        // insert into R store
                        match stream_store_right.get::<Vec<R>>(&right.key).unwrap() {
                            None => {
                                stream_store_right.insert(&right.key, &[right.value.clone()]).unwrap();
                            }
                            Some(mut right_events) => {
                                right_events.push(right.value.clone());
                                stream_store_right.insert(&right.key, &right_events).unwrap();
                            }
                        }

                        // check against all Ls
                        if let Some(mut left_events) = stream_store_left.get::<Vec<L>>(&right.key).unwrap() {
                            left_events.retain(|a| {
                                let (left_timestamp, right_timestamp) = timestamp_accessor(a, &right.value);
                                within_window(right_timestamp, left_timestamp, high_water_mark)
                                    || left_timestamp > right_timestamp
                            });
                            for left in left_events.clone() {
                                let (left_timestamp, right_timestamp) = timestamp_accessor(&left, &right.value);

                                if within_window(right_timestamp, left_timestamp, high_water_mark) {
                                    yield ParsedMessage {
                                        key: right.key.to_string(),
                                        value: (left.clone(), right.value.clone())
                                    }
                                }

                                if left_timestamp > right_timestamp
                                    && !within_window(right_timestamp, left_timestamp, high_water_mark)
                                {
                                    break;
                                }
                            }
                            stream_store_left.insert(&right.key, &left_events).unwrap();
                        }

                }
            }
        }
    }
}

pub async fn join_stream_table<S, T>(
    stream: impl Stream<Item = ParsedMessage<S>> + 'static + Send,
    table: RTable<impl KVStore + Send + 'static>,
) -> impl Stream<Item = (ParsedMessage<S>, Option<T>)>
where
    S: Clone + Send + Serialize + DeserializeOwned + 'static,
    T: Copy + Clone + std::fmt::Debug + DeserializeOwned + Send + 'static,
{
    let table = table.clone();
    stream.map(move |m| {
        let value = table.lock().unwrap().get(&m.key).unwrap();

        (m, value)
    })
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, time::Duration};

    use crate::store::Store;

    use super::*;

    // test from https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/
    #[tokio::test]
    async fn test_inner_join() {
        let stream_a = futures::stream::iter(vec![
            to_key("a", 0),
            to_key("b", 1),
            to_key("c", 3),
            to_key("d", 4),
            to_key("f", 6),
            to_key("f", 6),
            to_key("g", 8),
        ]);

        let stream_b = futures::stream::iter(vec![
            to_key("a", 1),
            to_key("c", 2),
            to_key("e", 5),
            to_key("f", 7),
            to_key("g", 9),
            to_key("g", 9),
            to_key("b", 11),
        ]);

        let joined_stream = inner_join_streams(
            stream_a,
            stream_b,
            Duration::from_millis(10),
            |a, b| (*a, *b),
            HashMap::new(),
            HashMap::new(),
        )
        .await;

        tokio::pin!(joined_stream);

        assert_eq!(joined_stream.next().await, Some(to_joined("a", (0, 1))));
        assert_eq!(joined_stream.next().await, Some(to_joined("c", (3, 2))));
        assert_eq!(joined_stream.next().await, Some(to_joined("f", (6, 7))));
        assert_eq!(joined_stream.next().await, Some(to_joined("f", (6, 7))));
        assert_eq!(joined_stream.next().await, Some(to_joined("g", (8, 9))));
        assert_eq!(joined_stream.next().await, Some(to_joined("g", (8, 9))));
    }

    #[tokio::test]
    async fn test_inner_join2() {
        let stream_a = futures::stream::iter(vec![
            to_message("a".to_string(), 3),
            to_message("b".to_string(), 5),
            to_message("c".to_string(), 9),
            to_message("d".to_string(), 15),
        ]);

        let stream_b = futures::stream::iter(vec![
            to_message("a".to_string(), 4),
            to_message("b".to_string(), 6),
            to_message("c".to_string(), 10),
            to_message("d".to_string(), 14),
        ]);

        let joined_stream = inner_join_streams(
            stream_a,
            stream_b,
            Duration::from_millis(1000),
            |a, b| (a.1, b.1),
            HashMap::new(),
            HashMap::new(),
        )
        .await;

        tokio::pin!(joined_stream);

        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("a".to_string(), 3), ("a".to_string(), 4))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("a".to_string(), 3), ("b".to_string(), 6))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("a".to_string(), 3), ("c".to_string(), 10))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("a".to_string(), 3), ("d".to_string(), 14))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("b".to_string(), 5), ("a".to_string(), 4))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("b".to_string(), 5), ("b".to_string(), 6))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("b".to_string(), 5), ("c".to_string(), 10))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("b".to_string(), 5), ("d".to_string(), 14))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("c".to_string(), 9), ("a".to_string(), 4))
            ))
        );

        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("c".to_string(), 9), ("b".to_string(), 6))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("c".to_string(), 9), ("c".to_string(), 10))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("c".to_string(), 9), ("d".to_string(), 14))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("d".to_string(), 15), ("a".to_string(), 4))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("d".to_string(), 15), ("b".to_string(), 6))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("d".to_string(), 15), ("c".to_string(), 10))
            ))
        );
        assert_eq!(
            joined_stream.next().await,
            Some(to_joined2(
                "key",
                (("d".to_string(), 15), ("d".to_string(), 14))
            ))
        );
    }

    #[tokio::test]
    async fn test_inner_join_with_persistence() {
        let stream_a = futures::stream::iter(vec![
            to_key("a", 0),
            to_key("b", 1),
            to_key("c", 3),
            to_key("d", 4),
            to_key("f", 6),
            to_key("f", 6),
            to_key("g", 8),
        ]);

        let stream_b = futures::stream::iter(vec![
            to_key("a", 1),
            to_key("c", 2),
            to_key("e", 5),
            to_key("f", 7),
            to_key("g", 9),
            to_key("g", 9),
            to_key("b", 11),
        ]);

        let joined_stream = inner_join_streams(
            stream_a,
            stream_b,
            Duration::from_millis(10),
            |a, b| (*a, *b),
            Store::new("test-a").unwrap(),
            Store::new("test-b").unwrap(),
        )
        .await;

        tokio::pin!(joined_stream);

        assert_eq!(joined_stream.next().await, Some(to_joined("a", (0, 1))));
        assert_eq!(joined_stream.next().await, Some(to_joined("c", (3, 2))));
        assert_eq!(joined_stream.next().await, Some(to_joined("f", (6, 7))));
        assert_eq!(joined_stream.next().await, Some(to_joined("f", (6, 7))));
        assert_eq!(joined_stream.next().await, Some(to_joined("g", (8, 9))));
        assert_eq!(joined_stream.next().await, Some(to_joined("g", (8, 9))));
    }

    fn to_message(value: String, timestamp: i64) -> ParsedMessage<(String, i64)> {
        ParsedMessage {
            key: "key".to_string(),
            value: (value, timestamp),
        }
    }

    fn to_key(key: &str, timestamp: i64) -> ParsedMessage<i64> {
        ParsedMessage {
            key: key.to_string(),
            value: timestamp,
        }
    }

    fn to_joined(key: &str, value: (i64, i64)) -> ParsedMessage<(i64, i64)> {
        ParsedMessage {
            key: key.to_string(),
            value,
        }
    }

    fn to_joined2(
        key: &str,
        value: ((String, i64), (String, i64)),
    ) -> ParsedMessage<((String, i64), (String, i64))> {
        ParsedMessage {
            key: key.to_string(),
            value,
        }
    }
}
