use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::{Stream, StreamExt};
use tracing::instrument;

use crate::channel::Channel;

pub struct Actor;

impl Actor {
    pub async fn spawn<T: Clone + std::fmt::Debug + std::marker::Send + 'static>(
        stream: impl Stream<Item = T> + std::marker::Send + 'static,
        buffer: usize,
        name: &'static str,
    ) -> Channel<T> {
        let (sender, receiver) = channel(buffer);

        // execute stream in background task
        tokio::spawn(actor(stream, sender.clone(), name));

        // where the results go
        Channel { receiver, sender }
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
