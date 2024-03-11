use std::fs::{read_dir, File};
use std::io::{BufRead, BufReader};
use std::os::unix::ffi::OsStrExt;

use bytes::Bytes;
use samsa::prelude::{ProduceMessage, ProducerBuilder};

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt::init();
    let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];

    let topics = vec!["shakespeare".to_string()];

    tracing::info!("Connecting to cluster");
    let producer_client = ProducerBuilder::new(bootstrap_addrs, topics)
        .await
        .map_err(|e| tracing::error!("{:?}", e))?
        .build()
        .await;

    let topic_name = "shakespeare".to_string();
    let mut partition_id = 0;

    let dir = "./data";
    let files = read_dir(dir).unwrap();

    for file in files {
        let file = file.unwrap();
        let filename = file.file_name();
        // println!("{:?}", file.path());
        let reader = BufReader::new(File::open(file.path()).expect("Cannot open file.txt"));

        if partition_id <= 2 {
            partition_id += 1
        } else {
            partition_id = 0
        };

        println!("Writing for {:?}", filename);
        let mut i = 0;

        for line in reader.lines() {
            for word in line.unwrap().split_whitespace() {
                producer_client
                    .produce(ProduceMessage {
                        topic: topic_name.to_string(),
                        partition_id,
                        key: Some(Bytes::copy_from_slice(filename.as_bytes())),
                        value: Some(Bytes::copy_from_slice(word.as_bytes())),
                    })
                    .await;

                i += 1;
            }
        }

        println!("Wrote {i} words");
    }

    Ok(())
}
