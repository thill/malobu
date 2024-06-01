//! # MALoBu
//!
//! Multiplexed Atomic Log Buffer
//!
//! # Use Case
//!
//! This crate provides a data structure that is able to enforce global resource limits across a set of multiple logs.
//! This is done using global FIFO across many underlyings treams to evict the oldest records from the cache based on a given target total weight at any point in time.
//! A `LogBuffer`` contains a set of underlying logs, which each have independent sets of writers and readers.
//! Readers subscribe to logs, and are guaranteed to poll a gapless record of all events pushed to the corresponding log, even when they have fallen behind.
//!
//! # Example
//!
//! ## Code
//! ```
//! use malobu::LogBuffer;
//!
//! let buffer: LogBuffer<String> = LogBuffer::new();
//! let log = buffer.create_log();
//! let reader = log.subscribe_beginning();
//!
//! // write two messages to the log, weighting them based on their size
//! log.write("hello, world!".to_owned(), 13);
//! log.write("message number 2!".to_owned(), 17);
//!
//! // poll a record from the reader
//! println!("reader polled: {:?}", reader.poll());
//!
//! // trim to weight=0, which will expire all records
//! buffer.trim(0);
//! println!("buffer total weight: {}", buffer.total_weight());
//!
//! // reader will still poll the last record, even though it has been trimmed from the parent buffer
//! println!("reader polled: {:?}", reader.poll());
//!
//! // no more events to poll
//! println!("reader polled: {:?}", reader.poll());
//! ```
//!
//! ## Output
//! ```text
//! reader polled: Some("hello, world!")
//! buffer total weight: 0
//! reader polled: Some("message number 2!")
//! reader polled: None
//! ```

use std::sync::{atomic::Ordering, Arc};

use arc_swap::{ArcSwap, ArcSwapOption};
use internal::{EntryLink, LogBufferContext, LogContext};

mod internal;

/// `LogBuffer` contains an atomic, lock-free list of entries for a set of underlying logs, where each log is independently appendable and iterable.
///
/// # Logs
///
/// Individual [`Log`]s exist within a `LogBuffer`, each with their own set of iterable entries.
/// A [`Log`] is used to push new entries or iterate over records within that log.
/// Every record within a log defines an associated global weight for that record.
///
/// # Reclaiming Memory
///
/// The [`LogBuffer::trim`] function will drop entries from memory on a global FIFO priority until a given target total weight is reached.
/// [`Log`] entries are dropped based on when they were added to the parent `LogBuffer`.
///
/// # Data Structure
///
/// You can consider a `LogBuffer` as a LinkedList of all entries, which contains other LinkedLists for individual logs with itself.
/// Each "Node" in the LinkedList contains a "global next" **and** "log next" link, allowing global and per-log iteration.
pub struct LogBuffer<T> {
    context: Arc<LogBufferContext<T>>,
}
impl<T> LogBuffer<T> {
    /// Create a new empty LogBuffer
    pub fn new() -> Self {
        Self {
            context: Arc::new(LogBufferContext::new()),
        }
    }

    /// Create a new [`Log`] within this [`LogBuffer`].
    /// The returned log, which can produce and subscribe records.
    pub fn create_log(&self) -> Log<T> {
        Log::new(Arc::new(LogContext::new(Arc::clone(&self.context))))
    }

    /// Constant time lookup of the total weight of all entries in all logs within this data structure.
    pub fn total_weight(&self) -> u64 {
        self.context.total_weight.load(Ordering::Relaxed)
    }

    /// Trim entries from underlying logs based on a FIFO priority until the given target `target_weight` is reached.
    pub fn trim(&self, target_weight: u64) {
        while self.context.total_weight.load(Ordering::Relaxed) > target_weight {
            // shift buffer beginning
            let mut removed_buffer_beginning = None;
            self.context.beginning.rcu(|old_beginning_link| {
                match old_beginning_link.buffer_next.load_full() {
                    None => {
                        // empty, keep old beginning
                        removed_buffer_beginning = None;
                        Arc::clone(old_beginning_link)
                    }
                    Some(x) => {
                        // shift to next beginning
                        removed_buffer_beginning = Some(Arc::clone(&x));
                        Arc::clone(&x.link)
                    }
                }
            });
            let removed_buffer_beginning = match removed_buffer_beginning {
                None => break, // empty, nothing else to do
                Some(x) => x,
            };

            // adjust buffer total weight
            self.context
                .total_weight
                .fetch_sub(removed_buffer_beginning.weight, Ordering::Relaxed);

            // remove log beginning for removed entry
            let log = &removed_buffer_beginning.log;
            let mut removed_log_beginning = None;
            log.beginning.rcu(|old_beginning_link| {
                match old_beginning_link.buffer_next.load_full() {
                    None => {
                        // empty, keep old beginning
                        removed_log_beginning = None;
                        Arc::clone(old_beginning_link)
                    }
                    Some(x) => {
                        // shift to next beginning
                        removed_log_beginning = Some(Arc::clone(&x));
                        Arc::clone(&x.link)
                    }
                }
            });

            // adjust log total weight
            if let Some(entry) = removed_log_beginning {
                log.total_weight.fetch_sub(entry.weight, Ordering::Relaxed);
            }
        }
    }
}
impl<T> Default for LogBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// An independent log of records within a [`LogBuffer`].
///
/// # Clone
///
/// A cloned `Log` will point to the same underlying log, allow writes, created writers, and created readers across multiple cloned
/// instances to share the same set of records.
///
/// # Writes
///
/// You may write directly to a `Log` using [`Log::write`].
/// To limit a utility to write-only, you may also choose to use a [`LogWriter`], which can be created with [`Log::create_writer`].
///
/// # Reads
///
/// Records can be streamed using one of the following funcions:
/// - [`Log::subscribe_beginning`]
/// - [`Log::subscribe_beginning_mut`]
/// - [`Log::subscribe_end`]
/// - [`Log::subscribe_end_mut`]
///
/// A reader is guaranteed to receive all entries in the order they are written, free of any race conditions with [`LogBuffer::trim`].
/// This is accomplished through the use of [`Arc`], ensuring that a entry is not dropped until all readers have caught up to it.
/// This means that [`LogBuffer::trim`] will drop its reference to entries by weight, but they will not actually be dropped from memory until
/// all active readerss have read the trimmed entries.
#[derive(Clone)]
pub struct Log<T> {
    context: Arc<LogContext<T>>,
    writer: Arc<LogWriter<T>>,
}
impl<T> Log<T> {
    fn new(context: Arc<LogContext<T>>) -> Self {
        Self {
            writer: Arc::new(LogWriter::new(Arc::clone(&context))),
            context,
        }
    }

    /// Create a new writer that can atomically append to this log
    pub fn create_writer(&self) -> LogWriter<T> {
        LogWriter::new(Arc::clone(&self.context))
    }

    /// Write a new record to the log, returning the inserted [`Entry`].
    pub fn write<A: Into<Arc<T>>>(&self, record: A, weight: u64) -> Arc<Entry<T>> {
        self.writer.write(record, weight)
    }

    /// Constant time lookup of the total weight of all entries within this log.
    pub fn total_weight(&self) -> u64 {
        self.context.total_weight.load(Ordering::Relaxed)
    }

    /// Get the first record in the log, or `None` if the log is currently empty.
    pub fn head(&self) -> Option<Arc<T>> {
        self.context
            .beginning
            .load()
            .log_next
            .load_full()
            .map(|x| Arc::clone(&x.record))
    }

    /// Create an atomic reader, which is [`Sync`] and can be shared between threads.
    /// The reader will start from the first available record at the beginning of the log.
    pub fn subscribe_beginning(&self) -> AtomicReader<T> {
        AtomicReader::new(self.context.beginning.load_full())
    }

    /// Create a mutable reader, which is slightly faster than the atomic variant and is an [`Iterator`], but is not [`Sync`].
    /// The reader will start from the first available record at the beginning of the log.
    pub fn subscribe_beginning_mut(&self) -> MutReader<T> {
        MutReader::new(self.context.beginning.load_full())
    }

    /// Create an atomic reader, which is [`Sync`] and can be shared between threads.
    /// The reader will start from the end of the log and will only receive new records pushed after the reader was created.
    pub fn subscribe_end(&self) -> AtomicReader<T> {
        AtomicReader::new(self.context.end.load_full())
    }

    /// Create a mutable reader, which is slightly faster than the atomic variant and is an [`Iterator`], but is not [`Sync`].
    /// The reader will start from the end of the log and will only receive new records pushed after the reader was created.
    pub fn subscribe_end_mut(&self) -> MutReader<T> {
        MutReader::new(self.context.end.load_full())
    }
}

/// Write to an underlying [`Log`]
///
/// # Clone
///
/// Cloning a [`LogWriter`] will result in a new writer that is able to send on the same underlying [`Log`].
#[derive(Clone)]
pub struct LogWriter<T> {
    context: Arc<LogContext<T>>,
}
impl<T> LogWriter<T> {
    fn new(context: Arc<LogContext<T>>) -> Self {
        Self { context }
    }

    /// Append a new record to the log, returning the inserted [`Entry`].
    pub fn write<A: Into<Arc<T>>>(&self, record: A, weight: u64) -> Arc<Entry<T>> {
        // create new end entry
        let new_tail = Arc::new(Entry::new_tail(
            record.into(),
            weight,
            Arc::clone(&self.context),
        ));
        // update log
        {
            // update log end
            let new_tail = Arc::clone(&new_tail);
            let old_end = self.context.end.swap(Arc::clone(&new_tail.link));
            old_end.log_next.store(Some(Arc::clone(&new_tail)));
            // lastly, update log weight
            self.context
                .total_weight
                .fetch_add(weight, Ordering::Relaxed);
        }
        // update buffer
        {
            // push to new buffer end
            let new_tail = Arc::clone(&new_tail);
            let old_end = self.context.parent.end.swap(Arc::clone(&new_tail.link));
            old_end.buffer_next.store(Some(Arc::clone(&new_tail)));
            // lastly, update buffer weight
            self.context
                .parent
                .total_weight
                .fetch_add(weight, Ordering::Relaxed);
        }
        new_tail
    }
}

/// A [`Sync`] reader that can be shared between threads, where each record will be delivered exactly once.
///
/// # Clone
///
/// Cloning an `AtomicReader` will create a new reader at an identical position to the original.
/// In other words, if there are 10 pending events to consume and the reader is cloned, the cloned reader
/// will also have 10 pending events, and they will each act as new independent readers.
///
/// **Note:** This behavior means that cloning an [`AtomicReader`] results in different behavior than cloning
/// an [`Arc<AtomicReader>`]. The former will create a new reader at the same point in the record log,
/// while the latter will poll from the same reader to share the load.
///
/// # Performance
///
/// The performance will be slightly worse than [`MutReader`], but allows for multiple workers to share the load from
/// multiple threads by taking records from a shared reader as they are available.
pub struct AtomicReader<T> {
    link: ArcSwap<EntryLink<T>>,
}
impl<T> AtomicReader<T> {
    fn new(link: Arc<EntryLink<T>>) -> Self {
        Self {
            link: ArcSwap::new(link),
        }
    }

    /// Peek the next available record without advancing the reader, returning immediately
    pub fn peek(&mut self) -> Option<Arc<T>> {
        match self.link.load_full().log_next.load_full() {
            None => None,
            Some(next) => Some(Arc::clone(&next.record)),
        }
    }

    /// Get the next available record and advance the reader, returning immediately
    pub fn poll(&self) -> Option<Arc<T>> {
        match self.link.load_full().log_next.load_full() {
            None => None,
            Some(next) => {
                let value = Arc::clone(&next.record);
                self.link.store(Arc::clone(&next.link));
                Some(value)
            }
        }
    }
}
impl<T> Clone for AtomicReader<T> {
    fn clone(&self) -> Self {
        Self::new(self.link.load_full())
    }
}

/// A mutable reader that utilizes `&mut self` to track the current position
///
/// # Clone
///
/// Cloning a `MutReader` will create a new reader at an identical position to the original.
/// In other words, if there are 10 pending events to consume and the reader is cloned, the cloned reader
/// will also have 10 pending events, and they will each act as new independent readers.
///
/// # Iterator
///
/// This structure implements [`Iterator`] to allow for rust-native iteration of available entries in the buffer.
///
/// # Performance
///
/// While providing identical functionality to [`AtomicReader`], it is slightly faster at the expense of not being [`Sync`].
#[derive(Clone)]
pub struct MutReader<T> {
    link: Arc<EntryLink<T>>,
}
impl<T> MutReader<T> {
    fn new(link: Arc<EntryLink<T>>) -> Self {
        Self { link }
    }
    /// Peek the next available record without advancing the reader, returning immediately
    pub fn peek(&mut self) -> Option<Arc<T>> {
        match self.link.log_next.load_full() {
            None => None,
            Some(next) => Some(Arc::clone(&next.record)),
        }
    }
    /// Get the next available record and advance the reader, returning immediately
    pub fn poll(&mut self) -> Option<Arc<T>> {
        match self.link.log_next.load_full() {
            None => None,
            Some(next) => {
                let value = Arc::clone(&next.record);
                self.link = Arc::clone(&next.link);
                Some(value)
            }
        }
    }
}
impl<T> Iterator for MutReader<T> {
    type Item = Arc<T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.poll()
    }
}

/// A entry in a [`LogBuffer`] and [`Log`], which can provide the record and weight, as well as generate readers starting from this record onward.
pub struct Entry<T> {
    record: Arc<T>,
    log: Arc<LogContext<T>>,
    weight: u64,
    link: Arc<EntryLink<T>>,
}
impl<T> Entry<T> {
    fn new_tail(record: Arc<T>, weight: u64, log: Arc<LogContext<T>>) -> Self {
        Self {
            record,
            log,
            weight,
            link: Arc::new(EntryLink::default()),
        }
    }

    /// Get the underlying record for this entry
    pub fn record<'a>(&'a self) -> &'a Arc<T> {
        &self.record
    }

    /// Get the weight associated with the record
    pub fn weight(&self) -> u64 {
        self.weight
    }

    /// Create an [`AtomicReader`] starting immediately after, but not including, this entry
    pub fn subscribe_after(&self) -> AtomicReader<T> {
        AtomicReader::new(Arc::clone(&self.link))
    }

    /// Create a [`MutReader`] starting immediately after, but not including, this entry
    pub fn subscribe_after_mut(&self) -> MutReader<T> {
        MutReader::new(Arc::clone(&self.link))
    }

    /// Create an [`AtomicReader`] from, and including, this entry
    pub fn subscribe_from(&self) -> AtomicReader<T> {
        AtomicReader::new(self.link_from())
    }

    /// Create a [`MutReader`] from, and including, this entry
    pub fn subscribe_from_mut(&self) -> MutReader<T> {
        MutReader::new(self.link_from())
    }

    fn link_from(&self) -> Arc<EntryLink<T>> {
        let record_ref = Self {
            record: Arc::clone(&self.record),
            log: Arc::clone(&self.log),
            weight: self.weight,
            link: Arc::clone(&self.link),
        };
        Arc::new(EntryLink {
            log_next: ArcSwapOption::new(Some(Arc::new(record_ref))),
            buffer_next: ArcSwapOption::new(None),
        })
    }
}
