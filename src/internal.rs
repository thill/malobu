/// Internal
use arc_swap::{ArcSwap, ArcSwapOption};

use std::sync::{atomic::AtomicU64, Arc};

use crate::Entry;

/// Context for a [`crate::LogBuffer`], which is kept in an [`Arc`] to be shared with underlying [`crate::Log`]s as their parent.
pub struct LogBufferContext<T> {
    pub beginning: ArcSwap<EntryLink<T>>, // link immediately before head entry
    pub end: ArcSwap<EntryLink<T>>,       // link immediately after tail entry
    pub total_weight: AtomicU64,
}
impl<T> LogBufferContext<T> {
    pub fn new() -> Self {
        let beginning = Arc::new(EntryLink::default());
        Self {
            beginning: ArcSwap::new(Arc::clone(&beginning)),
            end: ArcSwap::new(beginning),
            total_weight: AtomicU64::new(0),
        }
    }
}

/// Context for a [`crate::Log`], which is kept in an [`Arc`] to be shared between readers and readers.
pub struct LogContext<T> {
    pub parent: Arc<LogBufferContext<T>>,
    pub beginning: ArcSwap<EntryLink<T>>, // link immediately before head entry
    pub end: ArcSwap<EntryLink<T>>,       // link immediately after tail entry
    pub total_weight: AtomicU64,
}
impl<T> LogContext<T> {
    pub fn new(parent: Arc<LogBufferContext<T>>) -> Self {
        let beginning = Arc::new(EntryLink::default());
        Self {
            parent,
            beginning: ArcSwap::new(Arc::clone(&beginning)),
            end: ArcSwap::new(beginning),
            total_weight: AtomicU64::new(0),
        }
    }
}

/// [`Entry`] link information, of which a reference can be kept while allowing the value and other data to be dropped
pub struct EntryLink<T> {
    pub log_next: ArcSwapOption<Entry<T>>,
    pub buffer_next: ArcSwapOption<Entry<T>>,
}
impl<T> Default for EntryLink<T> {
    fn default() -> Self {
        Self {
            log_next: ArcSwapOption::new(None),
            buffer_next: ArcSwapOption::new(None),
        }
    }
}
