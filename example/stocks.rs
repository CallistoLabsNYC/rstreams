use bytes::Bytes;
use fork_stream::StreamExt as _;
use nom::AsBytes;
use rstreams::{
    actor::Actor,
    from_bytes, into_flat_stream, to_bytes,
    window::{hopping_window, lag_window},
    Dated, ParsedMessage,
};
use samsa::prelude::{ConsumerBuilder, ProduceMessage, ProducerBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;

/*
{
"symbol":"TSLA"
"timestamp":1672441140000
"open":122.9
"high":122.9
"low":122.87
"close":122.87
"volume":3403
"trade_count":45
"vwap":122.889033
"data_provider":"alpaca"
}
 */

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct Candle {
    symbol: String,
    open: f32,
    high: f32,
    low: f32,
    close: f32,
    volume: f32,
    timestamp: i64,
}

impl Candle {
    pub fn top(&self) -> f32 {
        if self.open > self.close {
            self.open
        } else {
            self.close
        }
    }

    pub fn bottom(&self) -> f32 {
        if self.open < self.close {
            self.open
        } else {
            self.close
        }
    }

    pub fn color(&self) -> Color {
        if self.open < self.close {
            Color::Green
        } else {
            Color::Red
        }
    }
}

impl Dated for Candle {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum StratClass {
    Inside = 1,
    Up = 2,
    Down = 3,
    Outside = 4,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub enum Color {
    Green = 0,
    Red = 1,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct StratCandle {
    symbol: String,
    open: f32,
    high: f32,
    low: f32,
    close: f32,
    volume: f32,
    timestamp: i64,
    strat_class: StratClass,
    color: Color,
}

impl StratCandle {
    pub fn new(candle: &Candle, strat_class: StratClass) -> Self {
        Self {
            symbol: candle.symbol.clone(),
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
            timestamp: candle.timestamp,
            strat_class,
            color: candle.color(),
        }
    }
}

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
    let _buffer_size = 100000;

    let stocks_topic = "price-updates".to_string();
    let _news_topic = "market-news".to_string();

    let consumer_stream = ConsumerBuilder::new(
        bootstrap_addrs.clone(),
        HashMap::from([(stocks_topic, vec![0])]),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build()
    .into_stream();
    // .throttle(Duration::from_secs(1));

    // read in 1 batch at a time
    let stock_batches = Actor::spawn(consumer_stream, 1, "stocks-consumer").await;

    let parser_stream = into_flat_stream(stock_batches).map(|record| ParsedMessage::<Candle> {
        key: std::str::from_utf8(record.key.as_bytes())
            .unwrap()
            .to_owned(),
        value: from_bytes::<Candle>(record.value).unwrap(),
    });

    let timeframes = vec![
        // (60 * 15, "15min-candles"),
        (60 * 30, "30min-candles"),
        (60 * 60, "1hr-candles"),
        (60 * 60 * 4, "4hr-candles"),
        (60 * 60 * 6, "6hr-candles"),
        (60 * 60 * 12, "12hr-candles"),
        (60 * 60 * 24, "day-candles"),
        (60 * 60 * 24 * 7, "week-candles"),
        (60 * 60 * 24 * 30, "month-candles"),
        (60 * 60 * 24 * 365, "year-candles"),
    ];

    let (seconds, topic) = (60 * 15, "15min-candles");

    let mut stream = Actor::spawn(
        hopping_window(
            parser_stream,
            Duration::from_secs(seconds),
            Duration::from_secs(seconds),
        )
        .filter_map(|message| {
            let message = aggregate_candles(message.key, message.value.0, message.value.1);
            if message.value.volume != 0.0 {
                Some(message)
            } else {
                None
            }
        }),
        1,
        topic,
    )
    .await
    .fork();

    ProducerBuilder::new(bootstrap_addrs.clone(), vec![topic.to_string()])
        .await
        .map_err(|err| tracing::error!("{:?}", err))?
        .build_from_stream(
            lag_window(stream.clone(), 2)
                .map(|message| classify_candle_strat(message.key, message.value))
                .map(|message| ProduceMessage {
                    key: Some(Bytes::from(message.key)),
                    value: Some(to_bytes(message.value).unwrap()),
                    topic: topic.to_string(),
                    partition_id: 0,
                    headers: vec![],
                }),
        )
        .await;

    for (seconds, topic) in timeframes {
        stream = Actor::spawn(
            hopping_window(
                stream,
                Duration::from_secs(seconds),
                Duration::from_secs(seconds),
            )
            .filter_map(|message| {
                let message = aggregate_candles(message.key, message.value.0, message.value.1);
                if message.value.volume != 0.0 {
                    Some(message)
                } else {
                    None
                }
            }),
            1,
            topic,
        )
        .await
        .fork();

        ProducerBuilder::new(bootstrap_addrs.clone(), vec![topic.to_string()])
            .await
            .map_err(|err| tracing::error!("{:?}", err))?
            .build_from_stream(
                lag_window(stream.clone(), 2)
                    .map(|message| classify_candle_strat(message.key, message.value))
                    .map(|message| ProduceMessage {
                        key: Some(Bytes::from(message.key)),
                        value: Some(to_bytes(message.value).unwrap()),
                        topic: topic.to_string(),
                        partition_id: 0,
                        headers: vec![],
                    }),
            )
            .await;
    }

    tokio::pin!(stream);

    while let Some(message) = stream.next().await {
        tracing::info!("main! {:?}", message);
    }

    tracing::info!("Main out!");

    Ok(())
}

fn aggregate_candles(key: String, timestamp: i64, window: Vec<Candle>) -> ParsedMessage<Candle> {
    tracing::info!("aggregating {} items", window.len());
    let (open, close) = if !window.is_empty() {
        (window[0].open, window[window.len() - 1].close)
    } else {
        (0.0, 0.0)
    };

    let candle = Candle {
        timestamp,
        symbol: key.clone(),
        open,
        high: 0.0,
        low: f32::MAX,
        close,
        volume: 0.0,
    };

    let candle = window.iter().fold(candle, |mut acc, new_candle| {
        acc.high = new_candle.high.max(acc.high);
        acc.low = new_candle.low.min(acc.low);
        acc.volume += new_candle.volume;

        acc
    });

    ParsedMessage { key, value: candle }
}

fn classify_candle_strat(key: String, window: Vec<Candle>) -> ParsedMessage<StratCandle> {
    if window.len() != 2 {
        panic!("Ahhh!");
    }

    let event = &window[1];
    let behind = &window[0];

    let above = event.top() > behind.top();
    let below = event.bottom() < behind.bottom();

    let strat_class = if above && below {
        StratClass::Outside
    } else if above {
        StratClass::Up
    } else if below {
        StratClass::Down
    } else {
        StratClass::Inside
    };

    ParsedMessage {
        key,
        value: StratCandle::new(event, strat_class),
    }
}
