use std::{collections::HashMap, time::Duration};

use tokio_stream::{Stream, StreamExt};

use crate::{Dated, ParsedMessage};

pub fn lag_window<A>(
    stream: impl Stream<Item = ParsedMessage<A>> + 'static,
    lag: usize,
) -> impl Stream<Item = ParsedMessage<Vec<A>>>
where
    A: Clone + 'static,
{
    async_stream::stream! {
        let mut stream_store = HashMap::new();

        tokio::pin!(stream);

        while let Some(new_event) = stream.next().await {
            match stream_store.get_mut(&new_event.key) {
                None => {
                    stream_store.insert(new_event.key, vec![new_event.value]);
                }
                Some(events) => {
                    events.push(new_event.clone().value);
                    if events.len() == lag {
                        yield (ParsedMessage {
                            key: new_event.key.clone(),
                            value: events.clone()
                        });

                        events.remove(0);
                    }
                }
            }
        }
    }
}

pub fn tumbling_window<A, F>(
    stream: impl Stream<Item = ParsedMessage<A>> + 'static,
    s: Duration,
) -> impl Stream<Item = ParsedMessage<Vec<A>>>
where
    A: Clone + Dated + 'static,
{
    async_stream::stream! {
        let mut stream_store = HashMap::new();

        tokio::pin!(stream);

        while let Some(new_event) = stream.next().await {
            match stream_store.get_mut(&new_event.key) {
                None => {
                    stream_store.insert(new_event.key, vec![new_event.value]);
                }
                Some(events) => {
                    if !events.is_empty() {
                        let earliest_window_index = events[0].timestamp() / (s.as_millis() as i64);
                        let latest_window_index = &new_event.value.timestamp() / (s.as_millis() as i64);
                        // if we have left the window
                        if earliest_window_index != latest_window_index {
                            // produce our array of fields in the window
                            yield (ParsedMessage {
                                key: new_event.key.clone(),
                                value: events.clone()
                            });

                            let window_difference = latest_window_index - earliest_window_index;
                            // if there are any empty tumble windows
                            if window_difference > 1 {
                                // close the gap with empties
                                for _ in 0..window_difference {
                                    yield (ParsedMessage {
                                        key: new_event.key.clone(),
                                        value: vec![]
                                    });
                                }
                            }
                            events.clear();
                        }
                    }
                    events.push(new_event.clone().value);
                }
            }
        }
    }
}

#[must_use = "stream does nothing by itself"]
pub fn hopping_window<A>(
    stream: impl Stream<Item = ParsedMessage<A>> + 'static,
    s: Duration,
    h: Duration,
) -> impl Stream<Item = ParsedMessage<(i64, Vec<A>)>>
where
    A: Clone + Dated + 'static,
{
    async_stream::stream! {
        let mut stream_store = HashMap::new();
        let mut time_store = HashMap::new();

        tokio::pin!(stream);

        while let Some(new_event) = stream.next().await {
            // tracing::info!("Incoming {}", timestamp_accessor(&new_event.value));
            match stream_store.get_mut(&new_event.key) {
                None => {
                    // tracing::info!("Initializing");
                    stream_store.insert(new_event.key, vec![new_event.value]);
                }
                Some(events) => {
                    // tracing::info!("len {}", events.len());
                    events.push(new_event.clone().value);
                    if events.len() > 1 {
                        let mut working_window_time = match time_store.get(&new_event.key) {
                            None => events[0].timestamp(),
                            Some(working_window_time) => *working_window_time,
                        };

                        let earliest_window_index = working_window_time / (s.as_millis() as i64);
                        let latest_event_time = new_event.value.timestamp();
                        let latest_window_index = latest_event_time / (s.as_millis() as i64);

                        // if we have left the window
                        // tracing::info!("taking action! {earliest_window_index} {latest_window_index}");
                        if earliest_window_index != latest_window_index {
                            // find out how many hops have closed
                            // tracing::info!("active window {time} {latest_event_time}");
                            // tracing::info!("while {} {}",working_window_time / (s.as_millis() as i64), latest_event_time / (s.as_millis() as i64));
                            while working_window_time + (s.as_millis() as i64) <= latest_event_time {

                                // tracing::info!("active window {} to {}", working_window_time, working_window_time + (s.as_millis() as i64));
                                // events.sort_by(|a, b| timestamp_accessor(a).cmp(&timestamp_accessor(b)));
                                let events_in_window = events.iter().filter(|event| {
                                    let event_time = event.timestamp();
                                    // tracing::info!("{} is {}",
                                    //     event_time,
                                    //     event_time >= working_window_time && event_time < working_window_time + (s.as_millis() as i64)
                                    // );
                                    event_time >= working_window_time && event_time < working_window_time + (s.as_millis() as i64)
                                })
                                .map(|event| event.to_owned())
                                .collect::<Vec<A>>();

                                // produce our array of fields in the window
                                yield (ParsedMessage {
                                    key: new_event.key.clone(),
                                    value: (working_window_time, events_in_window)
                                });

                                // move window forward
                                working_window_time += h.as_millis() as i64;

                                // keep anything that could be in the next window
                                // tracing::info!("keeping above {time}");
                                events.retain(|event| {
                                    let event_time = event.timestamp();
                                    // tracing::info!("{} is {}", event_time, event_time >= working_window_time);
                                    event_time >= working_window_time
                                });
                            }
                        }

                        time_store.insert(new_event.key, working_window_time);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // #[tokio::test]
    // async fn tumbling() {
    //     let stream = futures::stream::iter(vec![
    //         to_message(0),
    //         to_message(1),
    //         to_message(3),
    //         to_message(4),
    //         to_message(6),
    //         to_message(6),
    //         to_message(8),
    //         to_message(8),
    //         to_message(8),
    //         to_message(9),
    //         to_message(10),
    //         to_message(11),
    //         to_message(11),
    //         to_message(13),
    //         to_message(14),
    //     ]);

    //     let windowed = tumbling_window(stream, Duration::from_millis(3), |a| *a);
    //     tokio::pin!(windowed);

    //     assert_eq!(windowed.next().await, Some(to_messages(vec![0, 1])));
    //     assert_eq!(windowed.next().await, Some(to_messages(vec![3, 4])));
    //     assert_eq!(
    //         windowed.next().await,
    //         Some(to_messages(vec![6, 6, 8, 8, 8]))
    //     );
    //     assert_eq!(
    //         windowed.next().await,
    //         Some(to_messages(vec![9, 10, 11, 11]))
    //     );
    //     // assert_eq!(windowed.next().await, Some(to_messages(vec![13,14,11,11])));
    // }

    impl Dated for i64 {
        fn timestamp(&self) -> i64 {
            *self
        }
    }

    #[tokio::test]
    async fn tumbling_as_hopping() {
        let stream = futures::stream::iter(vec![
            to_message(0),
            to_message(1),
            to_message(3),
            to_message(4),
            to_message(6),
            to_message(6),
            to_message(8),
            to_message(8),
            to_message(8),
            to_message(9),
            to_message(10),
            to_message(11),
            to_message(11),
            to_message(13),
            to_message(14),
        ]);

        let windowed = hopping_window(stream, Duration::from_millis(3), Duration::from_millis(3));
        tokio::pin!(windowed);

        assert_eq!(windowed.next().await, Some(to_messages((0, vec![0, 1]))));
        assert_eq!(windowed.next().await, Some(to_messages((3, vec![3, 4]))));
        assert_eq!(
            windowed.next().await,
            Some(to_messages((6, vec![6, 6, 8, 8, 8])))
        );
        assert_eq!(
            windowed.next().await,
            Some(to_messages((9, vec![9, 10, 11, 11])))
        );
        // assert_eq!(windowed.next().await, to_messages((Some(vec![13,14,11,11]))));
    }

    #[tokio::test]
    async fn hopping() {
        let stream = futures::stream::iter(vec![
            to_message(0),
            to_message(1),
            to_message(3),
            to_message(4),
            to_message(6),
            to_message(6),
            to_message(8),
            to_message(8),
            to_message(8),
            to_message(9),
            to_message(10),
            to_message(11),
            to_message(11),
            to_message(13),
            to_message(14),
            to_message(20),
        ]);

        let windowed = hopping_window(stream, Duration::from_millis(3), Duration::from_millis(1));
        tokio::pin!(windowed);

        assert_eq!(windowed.next().await, Some(to_messages((0, vec![0, 1,]))));
        assert_eq!(windowed.next().await, Some(to_messages((1, vec![1, 3,]))));
        assert_eq!(windowed.next().await, Some(to_messages((2, vec![3, 4]))));
        assert_eq!(windowed.next().await, Some(to_messages((3, vec![3, 4]))));
        assert_eq!(windowed.next().await, Some(to_messages((4, vec![4, 6, 6]))));
        assert_eq!(windowed.next().await, Some(to_messages((5, vec![6, 6,]))));
        assert_eq!(
            windowed.next().await,
            Some(to_messages((6, vec![6, 6, 8, 8, 8])))
        );
        assert_eq!(
            windowed.next().await,
            Some(to_messages((7, vec![8, 8, 8, 9])))
        );
        assert_eq!(
            windowed.next().await,
            Some(to_messages((8, vec![8, 8, 8, 9, 10])))
        );
        assert_eq!(
            windowed.next().await,
            Some(to_messages((9, vec![9, 10, 11, 11])))
        );
        assert_eq!(
            windowed.next().await,
            Some(to_messages((10, vec![10, 11, 11])))
        );
        assert_eq!(
            windowed.next().await,
            Some(to_messages((11, vec![11, 11, 13])))
        );
        assert_eq!(windowed.next().await, Some(to_messages((12, vec![13, 14]))));
        assert_eq!(windowed.next().await, Some(to_messages((13, vec![13, 14]))));
        assert_eq!(windowed.next().await, Some(to_messages((14, vec![14]))));
        assert_eq!(windowed.next().await, Some(to_messages((15, vec![]))));
        assert_eq!(windowed.next().await, Some(to_messages((16, vec![]))));
        assert_eq!(windowed.next().await, Some(to_messages((17, vec![]))));
    }

    fn to_message(timestamp: i64) -> ParsedMessage<i64> {
        ParsedMessage {
            key: "key".to_string(),
            value: timestamp,
        }
    }

    fn to_messages(v: (i64, Vec<i64>)) -> ParsedMessage<(i64, Vec<i64>)> {
        ParsedMessage {
            key: "key".to_string(),
            value: v,
        }
    }
}
