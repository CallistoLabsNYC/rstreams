//! Changelog stream

use crate::{
    actor::Actor,
    error::{Error, Result},
    store::KVStore,
    ParsedMessage,
};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tokio_stream::{Stream, StreamExt};

pub type RTable<T> = Arc<Mutex<T>>;

pub struct Table;

impl Table {
    pub async fn spawn<V>(
        name: &'static str,
        stream: impl Stream<Item = ParsedMessage<V>> + Send + 'static,
        store: impl KVStore + Send + 'static,
    ) -> (
        impl Stream<Item = Result<ParsedMessage<V>>> + Send + 'static,
        RTable<impl KVStore + Send + 'static>,
    )
    where
        V: Copy + Clone + std::fmt::Debug + Serialize + Send + 'static,
    {
        let table = Arc::new(Mutex::new(store));
        let t = table.clone();
        let stream = stream.map(move |message| {
            table
                .lock()
                .map_err(|err| {
                    tracing::error!("{:?}", err);
                    Error::MutexPoisoned
                })?
                .insert(&message.key, message.value)?;
            Ok(message)
        });
        let stream = Actor::spawn(stream, 1, name).await;
        (stream, t)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn it_works_as_expected() {
        tracing_subscriber::fmt()
            // filter spans/events with level TRACE or higher.
            .with_max_level(tracing::Level::INFO)
            .compact()
            // Display source code file paths
            .with_file(true)
            // Display source code line numbers
            .with_line_number(true)
            // Display the thread ID an event was recorded on
            .with_thread_ids(true)
            // Don't display the event's target (module path)
            .with_target(false)
            // Build the subscriber
            .init();
        let stream = futures::stream::iter(vec![
            to_message("a", 0),
            to_message("a", 1),
            to_message("a", 2),
            to_message("a", 3),
            to_message("b", 0),
            to_message("b", 1),
            to_message("b", 2),
            to_message("b", 3), // last b
            to_message("a", 4), // last a
            to_message("c", 0),
            to_message("c", 1),
            to_message("c", 2),
            to_message("d", 0), // last d
            to_message("c", 3),
            to_message("c", 4), // last c
        ]);

        let (output, table) = Table::spawn("tester", stream, HashMap::new()).await;
        tokio::pin!(output);
        // you can manually read and look at the store
        output.next().await;
        // but we will consume the whole stream
        while output.next().await.is_some() {}

        let a: i64 = table.lock().unwrap().get("a").unwrap().unwrap();
        assert_eq!(a, 4);

        let b: i64 = table.lock().unwrap().get("b").unwrap().unwrap();
        assert_eq!(b, 3);

        let c: i64 = table.lock().unwrap().get("c").unwrap().unwrap();
        assert_eq!(c, 4);

        let d: i64 = table.lock().unwrap().get("d").unwrap().unwrap();
        assert_eq!(d, 0);
    }

    fn to_message(key: &str, value: i64) -> ParsedMessage<i64> {
        ParsedMessage {
            key: key.to_string(),
            value,
        }
    }
}
