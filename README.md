# malobu

## MALoBu

Multiplexed Atomic Log Buffer

## Use Case

This crate provides a data structure that is able to enforce global resource limits across a set of multiple logs.
This is done using global FIFO across many underlyings treams to evict the oldest records from the cache based on a given target total weight at any point in time.
A `LogBuffer`` contains a set of underlying logs, which each have independent sets of writers and readers.
Readers subscribe to logs, and are guaranteed to poll a gapless record of all events pushed to the corresponding log, even when they have fallen behind.

## Example

### Code
```rust
use malobu::LogBuffer;

let buffer: LogBuffer<String> = LogBuffer::new();
let log = buffer.create_log();
let reader = log.subscribe_beginning();

// write two messages to the log, weighting them based on their size
log.write("hello, world!".to_owned(), 13);
log.write("message number 2!".to_owned(), 17);

// poll a record from the reader
println!("reader polled: {:?}", reader.poll());

// trim to weight=0, which will expire all records
buffer.trim(0);
println!("buffer total weight: {}", buffer.total_weight());

// reader will still poll the last record, even though it has been trimmed from the parent buffer
println!("reader polled: {:?}", reader.poll());

// no more events to poll
println!("reader polled: {:?}", reader.poll());
```

### Output
```
reader polled: Some("hello, world!")
buffer total weight: 0
reader polled: Some("message number 2!")
reader polled: None
```
