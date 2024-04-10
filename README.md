# Rstreams
A lightweight, rust-native, stream processing library in the spirit of Kstreams.

## Design
This library is designed to bring Stream Processing capabilities to the asynchronous iterator Stream trait, found in the [futures-core](https://docs.rs/futures-core/0.3.30/futures_core/stream/trait.Stream.html). We provide a number of stream processing features such as windowing, joins, tables, and more. We also aim to provide a number of Sinks and Sources to make it easier to build with.

### Taxonomy 
Here we will go over the different abstractions found in this library.

#### Stream 
A stream is an asyncronous iterator. It is any structure that implements the Stream trait. You build up logic on the stream through mapping, filtered, joining, splitting, and so on. As records go through the stream, they will get mutated and filtered. These streams are lazy, and need an executor to actually do work. Without an executor, like an actor, the stream is just a defition that will do nothing.

#### Actor 
An actor is a `tokio` task that executes a stream. It accepts a input stream and returns an output stream. Actors do the work that is defined on a stream. 

These are useful when you want to build complex stream processing pipelines. One use case is to break up processing into multiple steps so that you can do things with the partial results.

#### Sink
Sinks accept streams and dump the data elsewhere. Typically these are where the flow of the data in an rstreams program terminate.

#### Source
Sources get data from elsewhere and return that data in a stream. Typically these are at the beginning of flow in an rstreams program.


## Current supported features

### Stateless operators
Everything supported by the
[tokio-stream/StreamExt](https://docs.rs/tokio-stream/0.1.14/tokio_stream/trait.StreamExt.html) and
[futures/StreamExt](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html)
traits, and anything that extends those traits...
- filter
- map
- fold
- trottle
- chunk
- timeout
- skip
- cycle
- split
- AND MORE...

### Stateful operators
- LAG window
- Tumbling window
- Hopping window
- Inner join

## Features TBD
- Persistence
- Left join
- Rtables

## Building

```shell
$ make
```
