use crate::{store::KVStore, ParsedMessage};
use tokio_stream::{Stream, StreamExt};
use serde::Serialize;

pub struct Table<T: KVStore> {
    store: T
}

impl<T> Table<T> where T: KVStore + Send + 'static {
    pub async fn new<V>(stream: impl Stream<Item = ParsedMessage<V>>  + std::marker::Send + 'static, mut store: T)
    where V: Clone + Serialize + Send {
        tokio::spawn(async move {
            tokio::pin!(stream);
            tracing::info!("Table coming online");
            while let Some(message) = stream.next().await {
                store.insert(&message.key, message.value).unwrap();
            }
            tracing::info!("Table finished stream");
        });
    }
}