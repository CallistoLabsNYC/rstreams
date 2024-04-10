use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::{Stream, StreamExt};
use tracing::instrument;
use async_stream::stream;

pub struct Actor;

impl Actor {
    pub async fn spawn<T>(
        stream: impl Stream<Item = T> + std::marker::Send + 'static,
        buffer: usize,
        name: &'static str,
    ) -> impl Stream<Item = T>
    where T: std::fmt::Debug + std::marker::Send + 'static {
        let (sender, mut receiver) = channel(buffer);

        // execute stream in background task
        tokio::spawn(actor(stream, sender.clone(), name));

        stream! {
            while let Some(message) = receiver.recv().await {
                yield message;
            }
        }
    }
}

#[instrument(skip(stream, sender))]
async fn actor<T: std::marker::Send + std::fmt::Debug + 'static>(
    stream: impl Stream<Item = T> + std::marker::Send + 'static,
    sender: Sender<T>,
    name: &'static str,
) {
    tokio::pin!(stream);
    tracing::info!("Actor coming online");
    while let Some(message) = stream.next().await {
        tracing::info!("{:?}", message);
        if sender.send(message).await.is_err() {
            // This will occur whenever the sender is deallocated
            tracing::warn!("Stream channel has been hung up");
            return;
        } else {
            // tracing::info!("Sending message to output channel")
        }
    }
    tracing::info!("Actor finished stream");
}
