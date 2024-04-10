// use std::collections::HashMap;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, time::Duration};

use async_stream::stream;
use tokio_stream::{Stream, StreamExt};

use crate::{
    store::{KVStore, Store},
    within_window, ParsedMessage,
};

enum JoinMessage<A, B> {
    A(A),
    B(B),
}

pub async fn inner_join_streams<A, B, F>(
    stream_a: impl Stream<Item = ParsedMessage<A>> + 'static + std::marker::Send,
    stream_b: impl Stream<Item = ParsedMessage<B>> + 'static + std::marker::Send,
    high_water_mark: Duration,
    timestamp_accessor: F,
    name: &'static str,
    mut stream_store_a: impl KVStore,
    mut stream_store_b: impl KVStore,
) -> impl Stream<Item = ParsedMessage<(A, B)>>
where
    F: (Fn(&A, &B) -> (i64, i64)),
    A: Clone + std::marker::Send + Serialize + DeserializeOwned + 'static,
    B: Clone + std::marker::Send + Serialize + DeserializeOwned + 'static,
{
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let sender_clone = sender.clone();

    // process B
    tokio::spawn(async move {
        tokio::pin!(stream_b);
        while let Some(b) = stream_b.next().await {
            if sender.send(JoinMessage::B(b)).await.is_err() {
                tracing::warn!("B Side of join hung up channel");
            }
        }
    });

    // process A
    tokio::spawn(async move {
        tokio::pin!(stream_a);
        while let Some(a) = stream_a.next().await {
            if sender_clone.send(JoinMessage::A(a)).await.is_err() {
                tracing::warn!("A Side of join hung up channel");
            }
        }
    });

    stream! {
        let a_name = format!("{}-a", name);
        let b_name = format!("{}-b", name);
        // let mut stream_store_a = Store::new(&a_name).unwrap();
        // let mut stream_store_b = Store::new(&b_name).unwrap();

        while let Some(message) = receiver.recv().await {
            match message {
                JoinMessage::A(a) => {
                    // insert into A store
                    match stream_store_a.get::<Vec<A>>(&a.key).unwrap() {
                        None => {
                            stream_store_a.insert(&a.key, vec![a.clone().value]).unwrap();
                        }
                        Some(mut a_events) => {
                            a_events.push(a.clone().value);
                            stream_store_a.insert(&a.key, &a_events).unwrap();
                        }
                    }

                    // check against all Bs
                    if let Some(mut b_events) = stream_store_b.get::<Vec<B>>(&a.key).unwrap() {
                        // prune step
                        b_events.retain(|b| {
                            let (a_timestamp, b_timestamp) = timestamp_accessor(&a.value, b);
                            within_window(b_timestamp, a_timestamp, high_water_mark)
                                || a_timestamp < b_timestamp
                        });
                        for b in b_events.iter() {
                            let (a_timestamp, b_timestamp) = timestamp_accessor(&a.value, b);

                            if within_window(b_timestamp, a_timestamp, high_water_mark) {
                                yield ParsedMessage {
                                    key: a.key.to_string(),
                                    value: (a.value.clone(), b.clone())
                                }
                            }

                            if b_timestamp > a_timestamp
                                && !within_window(b_timestamp, a_timestamp, high_water_mark)
                            {
                                break;
                            }
                        }
                        stream_store_b.insert(&a.key, &b_events).unwrap();
                    }
                },
                JoinMessage::B(b) => {
                        // insert into B store
                        match stream_store_b.get::<Vec<B>>(&b.key).unwrap() {
                            None => {
                                stream_store_b.insert(&b.key, &[b.value.clone()]).unwrap();
                            }
                            Some(mut b_events) => {
                                b_events.push(b.value.clone());
                                stream_store_b.insert(&b.key, &b_events).unwrap();
                            }
                        }

                        // check against all As
                        if let Some(mut a_events) = stream_store_a.get::<Vec<A>>(&b.key).unwrap() {
                            a_events.retain(|a| {
                                let (a_timestamp, b_timestamp) = timestamp_accessor(a, &b.value);
                                within_window(b_timestamp, a_timestamp, high_water_mark)
                                    || a_timestamp > b_timestamp
                            });
                            for a in a_events.clone() {
                                let (a_timestamp, b_timestamp) = timestamp_accessor(&a, &b.value);

                                if within_window(b_timestamp, a_timestamp, high_water_mark) {
                                    yield ParsedMessage {
                                        key: b.key.to_string(),
                                        value: (a.clone(), b.value.clone())
                                    }
                                }

                                if a_timestamp > b_timestamp
                                    && !within_window(b_timestamp, a_timestamp, high_water_mark)
                                {
                                    break;
                                }
                            }
                            stream_store_a.insert(&b.key, &a_events).unwrap();
                        }

                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

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
            "tester-stores",
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
            "tester-stores",
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
