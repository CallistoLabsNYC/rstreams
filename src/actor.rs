//! Execute streams in asynchronous Tokio tasks.

use async_stream::stream;
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::{Stream, StreamExt};
use tracing::instrument;

/// An Actor is an asynchronous Tokio task that executes a stream.
/// 
/// The buffer size relates to how many executed messages it can put in
/// its output box. If the buffer is 10, the Actor will process 10 messages
/// and then wait until a message is read from the output.
pub struct Actor;

impl Actor {
    /// Generate an Actor given a stream to execute, a buffer size,
    /// and a name for logging purposes.
    /// 
    /// This will spawn an async tokio task.
    pub async fn spawn<T: Clone + std::fmt::Debug + Send + 'static>(
        stream: impl Stream<Item = T> + Send + 'static,
        buffer: usize,
        name: &'static str,
    ) -> impl Stream<Item = T> {
        // how we will communicate with the thread
        let (sender, mut receiver) = channel(buffer);

        // execute stream in background task
        tokio::spawn(actor(stream, sender.clone(), name));

        // receive the actor output and make it a stream
        stream! {
            while let Some(message) = receiver.recv().await {
                yield message;
            }
        }
    }
}

/// The async function for the tokio task to execute
#[instrument(skip(stream, sender))]
async fn actor<T: Send + std::fmt::Debug + 'static>(
    stream: impl Stream<Item = T> + Send + 'static,
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
        }
    }
    tracing::info!("Actor finished stream");
}
