//! # MALoBu
//!
//! Multiplexed Atomic Log Buffer

use std::sync::{atomic::Ordering, Arc};

use arc_swap::ArcSwapOption;
use internal::{EntryLink, LogBufferContext, StreamContext};

mod internal;

/// `LogBuffer` contains an atomic list of entries for a set of underlying streams, where each stream is independently appendable and iterable.
///
/// # Use Case
///
/// This primary use-case of this structure is to enforce global resource limits across a set of multiple streams.
/// This is done using global FIFO to evict the oldest global records from the cache based on a given target total weight,
/// while guaranteeing that Subscriptions of each stream poll a gapless record of all events pushed to its corresponding stream.
///
/// # Streams
///
/// Individual [`LogStream`]s exist within a `LogBuffer`, each with their own set of iterable entries.
/// A [`LogStream`] is used to push new entries or iterate over records from a specific stream.
/// Every record within a stream defines an associated global weight for that record.
///
/// [`LogStream`] is [`Clone`] so that multiple producers can write to the same underlying stream.
///
/// # Reclaiming Memory
///
/// The [`LogBuffer::trim`] function will drop entries from memory on a global FIFO priority until a given target total weight is reached.
/// [`LogStream`] entries are dropped based on when they were added to the parent `LogBuffer`.
///
/// # Data Structure
///
/// You can consider a `LogBuffer` as a LinkedList that contains other LinkedLists within itself.
/// Every entry in the `LogBuffer` will point to the next global entry (across all streams within the `LogBuffer``) **and** the next entry within that stream.
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

    /// Create a new [`LogStream`] within this [`LogBuffer`].
    /// The returned stream, which can produce and subscribe records.
    pub fn create_stream(&self) -> LogStream<T> {
        LogStream::new(Arc::new(StreamContext::new(Arc::clone(&self.context))))
    }

    /// Constant time lookup of the total weight of all entries in all streams within this data structure.
    pub fn total_weight(&self) -> u64 {
        self.context.total_weight.load(Ordering::Relaxed)
    }

    /// Trim entries from underlying streams based on a FIFO priority until the given target `target_weight` is reached.
    pub fn trim(&self, target_weight: u64) {
        while self.context.total_weight.load(Ordering::Relaxed) > target_weight {
            // shift LogBuffer beginning
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

            // adjust LogBuffer total weight
            self.context
                .total_weight
                .fetch_sub(removed_buffer_beginning.weight, Ordering::Relaxed);

            // remove Stream beginning for removed entry
            let stream = &removed_buffer_beginning.stream;
            let mut removed_stream_beginning = None;
            stream.beginning.rcu(|old_beginning_link| {
                match old_beginning_link.buffer_next.load_full() {
                    None => {
                        // empty, keep old beginning
                        removed_stream_beginning = None;
                        Arc::clone(old_beginning_link)
                    }
                    Some(x) => {
                        // shift to next beginning
                        removed_stream_beginning = Some(Arc::clone(&x));
                        Arc::clone(&x.link)
                    }
                }
            });

            // adjust Stream total weight
            if let Some(entry) = removed_stream_beginning {
                stream
                    .total_weight
                    .fetch_sub(entry.weight, Ordering::Relaxed);
            }
        }
    }
}
impl<T> Default for LogBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// An independent stream of records within a [`LogBuffer`].
///
/// # Clone
///
/// A cloned `LogStream` will point to the same underlying stream, allow writes, created writers, and created subscriptions across multiple cloned
/// instances to share the same set of records.
///
/// # Writes
///
/// You may write directly to a `LogStream` using [`LogStream::write`].
/// To limit a utility to write-only, you may also choose to use a [`LogStreamWriter`], which can be created with [`LogStream::create_writer`].
///
/// # Reads
///
/// Entries can be streamed using one of the following:
/// - [`LogStream::subscribe_beginning`]
/// - [`LogStream::subscribe_beginning_mut`]
/// - [`LogStream::subscribe_end`]
/// - [`LogStream::subscribe_end_mut`]
///
/// A subscription is guaranteed to receive all entries in the order they are published, free of any race conditions with [`LogBuffer::trim`].
/// This is accomplished through the use of [`Arc`], ensuring that a entry is not dropped until all readers have caught up to it.
/// This means that [`LogBuffer::trim`] will drop its reference to entries by weight, but they will not actually be dropped from memory until
/// all active subscriptionss have read the trimmed entries.
#[derive(Clone)]
pub struct LogStream<T> {
    context: Arc<StreamContext<T>>,
    writer: Arc<LogStreamWriter<T>>,
}
impl<T> LogStream<T> {
    fn new(context: Arc<StreamContext<T>>) -> Self {
        Self {
            writer: Arc::new(LogStreamWriter::new(Arc::clone(&context))),
            context,
        }
    }
    ///
    pub fn create_writer(&self) -> LogStreamWriter<T> {
        LogStreamWriter::new(Arc::clone(&self.context))
    }
    /// Write a new record to the stream, returning the inserted [`Entry`].
    pub fn write<A: Into<Arc<T>>>(&self, record: A, weight: u64) -> Arc<Entry<T>> {
        self.writer.write(record, weight)
    }

    /// Constant time lookup of the total weight of all entries within this stream.
    pub fn total_weight(&self) -> u64 {
        self.context.total_weight.load(Ordering::Relaxed)
    }

    /// Get the first record in the stream, or `None` if the stream is currently empty.
    pub fn head(&self) -> Option<Arc<T>> {
        self.context
            .beginning
            .load()
            .stream_next
            .load_full()
            .map(|x| Arc::clone(&x.record))
    }

    /// Create an atomic subscription, which is [`Sync`] and can be shared between threads.
    /// The subscription will start from the first available record at the beginning of the stream.
    pub fn subscribe_beginning(&self) -> AtomicSubscription<T> {
        AtomicSubscription::new(self.context.beginning.load_full())
    }

    /// Create a mutable subscription, which is slightly faster than the atomic variant and is an [`Iterator`], but is not [`Sync`].
    /// The subscription will start from the first available record at the beginning of the stream.
    pub fn subscribe_beginning_mut(&self) -> MutSubscription<T> {
        MutSubscription::new(self.context.beginning.load_full())
    }

    /// Create an atomic subscription, which is [`Sync`] and can be shared between threads.
    /// The subscription will start from the end of the stream and will only receive new records pushed after the subscription was created.
    pub fn subscribe_end(&self) -> AtomicSubscription<T> {
        AtomicSubscription::new(self.context.end.load_full())
    }

    /// Create a mutable subscription, which is slightly faster than the atomic variant and is an [`Iterator`], but is not [`Sync`].
    /// The subscription will start from the end of the stream and will only receive new records pushed after the subscription was created.
    pub fn subscribe_end_mut(&self) -> MutSubscription<T> {
        MutSubscription::new(self.context.end.load_full())
    }
}

/// Write to an underlying [`LogStream`]
///
/// # Clone
///
/// Cloning a [`LogStreamWriter`] will result in a new writer that is able to send on the same underlying [`LogStream`].
#[derive(Clone)]
pub struct LogStreamWriter<T> {
    context: Arc<StreamContext<T>>,
}
impl<T> LogStreamWriter<T> {
    fn new(context: Arc<StreamContext<T>>) -> Self {
        Self { context }
    }
    /// Write a new record to the stream, returning the inserted [`Entry`].
    pub fn write<A: Into<Arc<T>>>(&self, record: A, weight: u64) -> Arc<Entry<T>> {
        // create new end entry
        let new_tail = Arc::new(Entry::new_tail(
            record.into(),
            weight,
            Arc::clone(&self.context),
        ));
        // update Stream
        {
            // update Stream end
            let new_tail = Arc::clone(&new_tail);
            let old_end = self.context.end.swap(Arc::clone(&new_tail.link));
            old_end.stream_next.store(Some(Arc::clone(&new_tail)));
            // lastly, update Stream weight
            self.context
                .total_weight
                .fetch_add(weight, Ordering::Relaxed);
        }
        // update LogBuffer
        {
            // push to new LogBuffer end
            let new_tail = Arc::clone(&new_tail);
            let old_end = self.context.parent.end.swap(Arc::clone(&new_tail.link));
            old_end.buffer_next.store(Some(Arc::clone(&new_tail)));
            // lastly, update LogBuffer weight
            self.context
                .parent
                .total_weight
                .fetch_add(weight, Ordering::Relaxed);
        }
        new_tail
    }
}

/// A [`Sync`] subscription that can be shared between threads, where each record will be delivered exactly once.
///
/// # Clone
///
/// Cloning an `AtomicSubscription` will create a new subscription at an identical position to the original.
/// In other words, if there are 10 pending events to consume and the subscription is cloned, the cloned subscription
/// will also have 10 pending events.
///
/// **Note:** This behavior means that cloning an [`AtomicSubscription`] results in different behavior than cloning
/// an [`Arc<AtomicSubscription>`]. The former will create a new subscription at the same point in the record stream,
/// while the latter will poll from the same subscription to share the load.
///
/// # Performance
///
/// The performance will be slightly worse than [`MutSubscription`], but allows for multiple workers to share the load from
/// multiple threads by taking records from a shared subscription as they are available.
#[derive(Clone)]
pub struct AtomicSubscription<T> {
    link: Arc<EntryLink<T>>,
}
impl<T> AtomicSubscription<T> {
    fn new(link: Arc<EntryLink<T>>) -> Self {
        Self { link }
    }
    /// Peek the next available record without advancing the subscription, returning immediately
    pub fn peek(&mut self) -> Option<Arc<T>> {
        match self.link.stream_next.load_full() {
            None => None,
            Some(next) => Some(Arc::clone(&next.record)),
        }
    }
    /// Get the next available record and advance the subscription, returning immediately
    pub fn poll(&mut self) -> Option<Arc<T>> {
        match self.link.stream_next.load_full() {
            None => None,
            Some(next) => {
                let value = Arc::clone(&next.record);
                self.link = Arc::clone(&next.link);
                Some(value)
            }
        }
    }
}

/// A mutable subscription that utilizes `&mut self` to track the current position
///
/// # Clone
///
/// Cloning a `MutSubscription` will create a new subscription at an identical position to the original.
/// In other words, if there are 10 pending events to consume and the subscription is cloned, the cloned subscription
/// will also have 10 pending events.
///
/// # Iterator
///
/// This structure implements [`Iterator`] to allow for rust-native iteration of available entries in the buffer.
///
/// # Performance
///
/// While providing identical functionality to [`AtomicSubscription`], it is slightly faster at the expense of not being [`Sync`]
#[derive(Clone)]
pub struct MutSubscription<T> {
    link: Arc<EntryLink<T>>,
}
impl<T> MutSubscription<T> {
    fn new(link: Arc<EntryLink<T>>) -> Self {
        Self { link }
    }
    /// Peek the next available record without advancing the subscription, returning immediately
    pub fn peek(&mut self) -> Option<Arc<T>> {
        match self.link.stream_next.load_full() {
            None => None,
            Some(next) => Some(Arc::clone(&next.record)),
        }
    }
    /// Get the next available record and advance the subscription, returning immediately
    pub fn poll(&mut self) -> Option<Arc<T>> {
        match self.link.stream_next.load_full() {
            None => None,
            Some(next) => {
                let value = Arc::clone(&next.record);
                self.link = Arc::clone(&next.link);
                Some(value)
            }
        }
    }
}
impl<T> Iterator for MutSubscription<T> {
    type Item = Arc<T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.poll()
    }
}

/// A entry in the [`LogBuffer`], which can provide the record and weight, as well as generate subscriptions starting from this record onward.
pub struct Entry<T> {
    record: Arc<T>,
    stream: Arc<StreamContext<T>>,
    weight: u64,
    link: Arc<EntryLink<T>>,
}
impl<T> Entry<T> {
    fn new_tail(record: Arc<T>, weight: u64, stream: Arc<StreamContext<T>>) -> Self {
        Self {
            record,
            stream,
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

    /// Create an [`AtomicSubscription`] starting immediately after, but not including, this entry
    pub fn subscribe_after(&self) -> AtomicSubscription<T> {
        AtomicSubscription::new(Arc::clone(&self.link))
    }

    /// Create a [`MutSubscription`] starting immediately after, but not including, this entry
    pub fn subscribe_after_mut(&self) -> MutSubscription<T> {
        MutSubscription::new(Arc::clone(&self.link))
    }

    /// Create an [`AtomicSubscription`] from, and including, this entry
    pub fn subscribe_from(&self) -> AtomicSubscription<T> {
        AtomicSubscription::new(self.link_from())
    }

    /// Create a [`MutSubscription`] from, and including, this entry
    pub fn subscribe_from_mut(&self) -> MutSubscription<T> {
        MutSubscription::new(self.link_from())
    }

    fn link_from(&self) -> Arc<EntryLink<T>> {
        let record_ref = Self {
            record: Arc::clone(&self.record),
            stream: Arc::clone(&self.stream),
            weight: self.weight,
            link: Arc::clone(&self.link),
        };
        Arc::new(EntryLink {
            stream_next: ArcSwapOption::new(Some(Arc::new(record_ref))),
            buffer_next: ArcSwapOption::new(None),
        })
    }
}
