# Rstreams
A lightweight, rust-native, stream processing library in the spirit of Kstreams.

## Current supported features

### Stateless operators
Everything supported by these two traits:
https://docs.rs/tokio-stream/0.1.14/tokio_stream/trait.StreamExt.html
https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html
and anything that extends those traits...
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