# Rstreams
A lightweight, rust-native, stream processing library in the spirit of Kstreams.

## Shakespeare Demo
This demo shows both producing and simple stream processing. We load each word of Shakespeares works into a topic with 4 partitions, and then a consumer counts each word in each partition and ranks by occurrance. Here is how to run:
1. run `docker-compose up` to start redpanda brokers
1. run `alias rpk="docker exec -ti redpanda-1 rpk"` to make commands easier
1. run `rpk topic create shakespeare -p 4` to make a topic with 4 partitions
1. run `cargo run --bin loader` to load all of the shakespeare words into the topics
1. in a new tab run `cargo run --bin tpipe` to count the words.

## Stock Data Demo
This demo will show more advanced stream processing capabilities. We take stock data of TSLA and aggregate the 1min 'candles' over 15min, 30min, 1hr, 4hr, 1week, up to 1year, then categorize each 'candle' based on the one preceded it. Here is how to run:

We need to set up the redpanda cluster and topics
1. run `docker-compose up` to start redpanda brokers
1. run `alias rpk="docker exec -ti redpanda-1 rpk"` to make commands easier
1. run `rpk topic create price-updates -p 4`

Now we need to set up the topic data. You must have the `.env` with the Alpaca keys!
1. run `cd ../demo`
1. run `python3 -m venv .`
1. run `source bin/activate`
1. `pip install -r requirements.txt`
1. `python -m examples.alpaca.historical_prices`

In another terminal in the `rstream` directory:
1. run `cargo --bin stocks`