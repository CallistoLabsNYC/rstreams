use std::collections::HashMap;

use nom::AsBytes;
use rstreams::actor::Actor;
use samsa::prelude::ConsumerBuilder;
use tokio_stream::StreamExt;

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

    let bootstrap_addrs = vec!["0.0.0.0:9092".to_string()];

    let src_topic = "shakespeare".to_string();

    let buffer_size = 100000;

    let input_stream1 = ConsumerBuilder::new(
        bootstrap_addrs.clone(),
        HashMap::from([(src_topic.clone(), vec![0])]),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build()
    .into_flat_stream();

    let input_stream2 = ConsumerBuilder::new(
        bootstrap_addrs.clone(),
        HashMap::from([(src_topic.clone(), vec![1])]),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build()
    .into_flat_stream();

    let input_stream3 = ConsumerBuilder::new(
        bootstrap_addrs.clone(),
        HashMap::from([(src_topic.clone(), vec![2])]),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build()
    .into_flat_stream();

    let input_stream4 = ConsumerBuilder::new(
        bootstrap_addrs.clone(),
        HashMap::from([(src_topic.clone(), vec![3])]),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build()
    .into_flat_stream();

    tracing::info!("Starting!");

    let input1 = Actor::spawn(input_stream1, buffer_size, "first-partition-input").await;
    let input2 = Actor::spawn(input_stream2, buffer_size, "second-partition-input").await;
    let input3 = Actor::spawn(input_stream3, buffer_size, "third-partition-input").await;
    let input4 = Actor::spawn(input_stream4, buffer_size, "fourth-partition-input").await;

    let translator_stream1 = input1.map(|record| {
        std::str::from_utf8(record.value.as_bytes())
            .unwrap()
            .replace(&['(', ')', ',', '\"', '.', ';', ':', '\''][..], "")
            .to_lowercase()
    });

    let translator_stream2 = input2.map(|record| {
        std::str::from_utf8(record.value.as_bytes())
            .unwrap()
            .replace(&['(', ')', ',', '\"', '.', ';', ':', '\''][..], "")
            .to_lowercase()
    });

    let translator_stream3 = input3.map(|record| {
        std::str::from_utf8(record.value.as_bytes())
            .unwrap()
            .replace(&['(', ')', ',', '\"', '.', ';', ':', '\''][..], "")
            .to_lowercase()
    });

    let translator_stream4 = input4.map(|record| {
        std::str::from_utf8(record.value.as_bytes())
            .unwrap()
            .replace(&['(', ')', ',', '\"', '.', ';', ':', '\''][..], "")
            .to_lowercase()
    });

    let translator1 = Actor::spawn(translator_stream1, buffer_size, "first-translator").await;
    let translator2 = Actor::spawn(translator_stream2, buffer_size, "first-translator").await;
    let translator3 = Actor::spawn(translator_stream3, buffer_size, "first-translator").await;
    let translator4 = Actor::spawn(translator_stream4, buffer_size, "first-translator").await;

    let counter1 = translator1
        .take(200000)
        .fold(HashMap::new(), |mut counter, word| {
            if let Some(count) = counter.get(&word) {
                counter.insert(word, count + 1);
            } else {
                counter.insert(word, 1);
            }
            counter
        })
        .await;

    let counter2 = translator2
        .take(200000)
        .fold(HashMap::new(), |mut counter, word| {
            if let Some(count) = counter.get(&word) {
                counter.insert(word, count + 1);
            } else {
                counter.insert(word, 1);
            }
            counter
        })
        .await;

    let counter3 = translator3
        .take(200000)
        .fold(HashMap::new(), |mut counter, word| {
            if let Some(count) = counter.get(&word) {
                counter.insert(word, count + 1);
            } else {
                counter.insert(word, 1);
            }
            counter
        })
        .await;

    let counter4 = translator4
        .take(200000)
        .fold(HashMap::new(), |mut counter, word| {
            if let Some(count) = counter.get(&word) {
                counter.insert(word, count + 1);
            } else {
                counter.insert(word, 1);
            }
            counter
        })
        .await;

    tracing::info!("Done!");

    let mut hash_vec: Vec<(&String, &u32)> = counter1.iter().collect();
    hash_vec.sort_by(|a, b| b.1.cmp(a.1));
    for (word, count) in hash_vec.iter().take(100) {
        tracing::info!("{word}: {count}");
    }

    let mut hash_vec: Vec<(&String, &u32)> = counter2.iter().collect();
    hash_vec.sort_by(|a, b| b.1.cmp(a.1));
    for (word, count) in hash_vec.iter().take(100) {
        tracing::info!("{word}: {count}");
    }

    let mut hash_vec: Vec<(&String, &u32)> = counter3.iter().collect();
    hash_vec.sort_by(|a, b| b.1.cmp(a.1));
    for (word, count) in hash_vec.iter().take(100) {
        tracing::info!("{word}: {count}");
    }

    let mut hash_vec: Vec<(&String, &u32)> = counter4.iter().collect();
    hash_vec.sort_by(|a, b| b.1.cmp(a.1));
    for (word, count) in hash_vec.iter().take(100) {
        tracing::info!("{word}: {count}");
    }
    Ok(())
}
