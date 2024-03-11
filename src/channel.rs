use async_stream::stream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::Stream;

pub struct Channel<T: Clone + std::marker::Send + 'static> {
    pub receiver: Receiver<T>,
    pub sender: Sender<T>,
}

impl<T: Clone + std::marker::Send + 'static> Channel<T> {
    #[must_use = "stream does nothingby itself"]
    pub fn to_stream(mut self) -> impl Stream<Item = T> {
        stream! {
            while let Some(message) = self.receiver.recv().await {
                // tracing::info!("Channel receiving");
                yield message;
            }
            // tracing::info!("Channel Receiver going out");
        }
    }

    // pub fn split(&self) -> Channel<T> {
    //     let new_receiver = self.sender.subscribe();
    //     let new_sender = self.sender.clone();

    //     Channel {
    //         sender: new_sender,
    //         receiver: new_receiver,
    //     }
    // }
}
