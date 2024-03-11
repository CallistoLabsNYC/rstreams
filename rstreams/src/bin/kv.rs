use tokio_stream::StreamExt;

use std::time::Duration;

use rstreams::{window::hopping_window, ParsedMessage};

#[tokio::main]
async fn main() -> Result<(), ()> {
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

    let windowed = hopping_window(
        stream,
        Duration::from_millis(3),
        Duration::from_millis(1),
        |a| *a,
    );
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

    Ok(())
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
