use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, spawn, JoinHandle},
    time::{Duration, SystemTime},
};

use malobu::{AtomicReader, Log, LogBuffer};

#[test]
fn single_writer_single_reader() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = LogBuffer::new();
    let log = buffer.create_log();
    let writer_messages: Vec<String> = (0..10000).into_iter().map(|x| x.to_string()).collect();
    let writer = TestWriter::new(&log, &context, &writer_messages);
    let reader = TestReader::new(&log, &context);

    // execute test
    writer.spawn();
    let reader_jh = reader.spawn();
    context.start();
    context.join(1, 1, 10000);
    let reader_msgs = reader_jh.join().unwrap();

    // validations
    assert_eq!(10000, reader_msgs.len());
    for i in 0..10000 {
        assert_eq!(reader_msgs.get(i).unwrap().as_ref(), &i.to_string());
    }

    // validate consuming from beginning
    let from_beginning_messages = log.subscribe_beginning_mut().collect::<Vec<Arc<String>>>();
    assert_eq!(10000, from_beginning_messages.len());
    for i in 0..10000 {
        assert_eq!(
            from_beginning_messages.get(i).unwrap().as_ref(),
            &i.to_string()
        );
    }
}

#[test]
fn single_writer_multi_reader() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = LogBuffer::new();
    let log = buffer.create_log();
    let writer_messages: Vec<String> = (0..10000).into_iter().map(|x| x.to_string()).collect();
    let writer = TestWriter::new(&log, &context, &writer_messages);
    let reader1 = TestReader::new(&log, &context);
    let reader2 = TestReader::new(&log, &context);

    // execute test
    writer.spawn();
    let reader1_jh = reader1.spawn();
    let reader2_jh = reader2.spawn();
    context.start();
    context.join(1, 2, 20000);
    let reader1_msgs = reader1_jh.join().unwrap();
    let reader2_msgs = reader2_jh.join().unwrap();

    // validations
    assert_eq!(10000, reader1_msgs.len());
    assert_eq!(10000, reader2_msgs.len());
    for i in 0..10000 {
        assert_eq!(reader1_msgs.get(i).unwrap().as_ref(), &i.to_string());
        assert_eq!(reader2_msgs.get(i).unwrap().as_ref(), &i.to_string());
    }

    // validate consuming from beginning
    let from_beginning_messages = log.subscribe_beginning_mut().collect::<Vec<Arc<String>>>();
    assert_eq!(10000, from_beginning_messages.len());
    for i in 0..10000 {
        assert_eq!(
            from_beginning_messages.get(i).unwrap().as_ref(),
            &i.to_string()
        );
    }
}

#[test]
fn multi_writer_single_reader() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = LogBuffer::new();
    let log = buffer.create_log();
    let writer1_messages: Vec<String> = (0..10000)
        .into_iter()
        .map(|x| format!("p1:{x:05}"))
        .collect();
    let writer2_messages: Vec<String> = (0..10000)
        .into_iter()
        .map(|x| format!("p2:{x:05}"))
        .collect();
    let writer3_messages: Vec<String> = (0..10000)
        .into_iter()
        .map(|x| format!("p3:{x:05}"))
        .collect();
    let writer1 = TestWriter::new(&log, &context, &writer1_messages);
    let writer2 = TestWriter::new(&log, &context, &writer2_messages);
    let writer3 = TestWriter::new(&log, &context, &writer3_messages);
    let reader = TestReader::new(&log, &context);

    // execute test
    writer1.spawn();
    writer2.spawn();
    writer3.spawn();
    let reader_jh = reader.spawn();
    context.start();
    context.join(3, 1, 30000);
    let mut reader_msgs = reader_jh.join().unwrap();
    reader_msgs.sort();

    // validations
    assert_eq!(30000, reader_msgs.len());
    // sorted results, first 10000 will be writer1
    for i in 0..10000 {
        assert_eq!(reader_msgs.get(i).unwrap().as_ref(), &format!("p1:{i:05}"));
    }
    // sorted results, next 10000 will be writer2
    for i in 0..10000 {
        assert_eq!(
            reader_msgs.get(10000 + i).unwrap().as_ref(),
            &format!("p2:{i:05}")
        );
    }
    // sorted results, next 10000 will be writer3
    for i in 0..10000 {
        assert_eq!(
            reader_msgs.get(20000 + i).unwrap().as_ref(),
            &format!("p3:{i:05}")
        );
    }

    // validate consuming from beginning
    let mut from_beginning_messages = log.subscribe_beginning_mut().collect::<Vec<Arc<String>>>();
    from_beginning_messages.sort();
    assert_eq!(30000, from_beginning_messages.len());
    // sorted results, first 10000 will be writer1
    for i in 0..10000 {
        assert_eq!(
            from_beginning_messages.get(i).unwrap().as_ref(),
            &format!("p1:{i:05}")
        );
    }
    // sorted results, next 10000 will be writer2
    for i in 0..10000 {
        assert_eq!(
            from_beginning_messages.get(10000 + i).unwrap().as_ref(),
            &format!("p2:{i:05}")
        );
    }
    // sorted results, next 10000 will be writer3
    for i in 0..10000 {
        assert_eq!(
            from_beginning_messages.get(20000 + i).unwrap().as_ref(),
            &format!("p3:{i:05}")
        );
    }
}

#[test]
fn stress_without_trim() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = Arc::new(LogBuffer::<String>::new());
    let log = buffer.create_log();
    let writer_count = 8;
    let reader_count = 8;
    let record_count_per_writer = 100000;

    // create writers and readers
    for i in 0..writer_count {
        let messages: Vec<String> = (0..record_count_per_writer)
            .into_iter()
            .map(|x| format!("p{i:03}:{x:05}"))
            .collect();
        TestWriter::new(&log, &context, &messages).spawn();
    }
    for _ in 0..reader_count {
        TestReader::new(&log, &context).spawn();
    }

    // execute test, which will verify exact count of messages were received
    context.start();
    context.join(
        writer_count,
        reader_count,
        writer_count * reader_count * record_count_per_writer,
    );
}

#[test]
fn stress_with_trim() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = Arc::new(LogBuffer::<String>::new());
    let log = buffer.create_log();
    let writer_count = 8;
    let reader_count = 8;
    let record_count_per_writer = 100000;

    // create writers and readers
    for i in 0..writer_count {
        let messages: Vec<String> = (0..record_count_per_writer)
            .into_iter()
            .map(|x| format!("p{i:03}:{x:05}"))
            .collect();
        TestWriter::new(&log, &context, &messages).spawn();
    }
    for _ in 0..reader_count {
        TestReader::new(&log, &context).spawn();
    }

    // create 5 trimmers of various intervals
    Trimmer::new(&buffer, &context, 0, Duration::from_millis(1)).spawn();
    Trimmer::new(&buffer, &context, 0, Duration::from_millis(1)).spawn();
    Trimmer::new(&buffer, &context, 100, Duration::from_millis(5)).spawn();
    Trimmer::new(&buffer, &context, 100, Duration::from_millis(5)).spawn();
    Trimmer::new(&buffer, &context, 120, Duration::from_millis(5)).spawn();

    // execute test, which will verify exact count of messages were received
    context.start();
    context.join(
        writer_count,
        reader_count,
        writer_count * reader_count * record_count_per_writer,
    );
}

struct TestContext {
    start_flag: Arc<AtomicBool>,
    done_flag: Arc<AtomicBool>,
    done_count: Arc<AtomicUsize>,
    consumed_count: Arc<AtomicUsize>,
}
impl TestContext {
    fn new() -> Self {
        Self {
            start_flag: Arc::new(AtomicBool::new(false)),
            done_flag: Arc::new(AtomicBool::new(false)),
            done_count: Arc::new(AtomicUsize::new(0)),
            consumed_count: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn join(&self, writer_count: usize, reader_count: usize, consumed_count: usize) {
        let started_at = SystemTime::now();
        let timeout_at = started_at + Duration::from_secs(60);

        // wait for writers to be complete
        while SystemTime::now() < timeout_at
            && self.done_count.load(Ordering::Relaxed) < writer_count
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(self.done_count.load(Ordering::Relaxed), writer_count);

        // wait for reader messages to have all been received
        while SystemTime::now() < timeout_at
            && self.consumed_count.load(Ordering::Relaxed) < consumed_count
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(self.consumed_count.load(Ordering::Relaxed), consumed_count);

        // set done flag for readers
        self.done_flag.store(true, Ordering::Relaxed);

        // wait for readers to be complete
        while SystemTime::now() < timeout_at
            && self.done_count.load(Ordering::Relaxed) < (writer_count + reader_count)
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            self.done_count.load(Ordering::Relaxed),
            writer_count + reader_count
        );

        // calculate and print rate
        let duration = SystemTime::now().duration_since(started_at).unwrap();
        println!(
            "average rate: {}/sec/reader",
            (consumed_count as f64
                / (duration.as_micros() as f64 / 1000000.0)
                / reader_count as f64) as usize
        );
    }
    fn start(&self) {
        self.start_flag.store(true, Ordering::Release);
    }
    fn is_started(&self) -> bool {
        self.start_flag.load(Ordering::Relaxed)
    }
    fn is_done(&self) -> bool {
        self.done_flag.load(Ordering::Relaxed)
    }
    fn increment_consumed_count(&self) {
        self.consumed_count.fetch_add(1, Ordering::Relaxed);
    }
    fn increment_done_count(&self) {
        self.done_count.fetch_add(1, Ordering::Relaxed);
    }
}

struct Trimmer {
    buffer: Arc<LogBuffer<String>>,
    context: Arc<TestContext>,
    target_weight: u64,
    interval: Duration,
}
impl Trimmer {
    fn new(
        buffer: &Arc<LogBuffer<String>>,
        context: &Arc<TestContext>,
        target_weight: u64,
        interval: Duration,
    ) -> Self {
        Self {
            buffer: Arc::clone(&buffer),
            context: Arc::clone(context),
            target_weight,
            interval,
        }
    }
    fn spawn(self) {
        spawn(move || {
            while !self.context.is_started() && !self.context.is_done() {
                thread::yield_now();
            }
            while !self.context.is_done() {
                self.buffer.trim(self.target_weight);
                thread::sleep(self.interval);
            }
        });
    }
}

struct TestWriter {
    log: Log<String>,
    context: Arc<TestContext>,
    messages: VecDeque<String>,
}
impl TestWriter {
    fn new(log: &Log<String>, context: &Arc<TestContext>, messages: &Vec<String>) -> Self {
        Self {
            log: log.clone(),
            context: Arc::clone(context),
            messages: messages.iter().map(|x| x.to_owned()).collect(),
        }
    }
    fn spawn(mut self) {
        spawn(move || {
            while !self.context.is_started() && !self.context.is_done() {
                thread::yield_now();
            }
            while !self.context.is_done() && !self.messages.is_empty() {
                let msg = self.messages.pop_front().unwrap();
                self.log.write(msg, 1);
            }
            self.context.increment_done_count();
        });
    }
}

struct TestReader {
    reader: AtomicReader<String>,
    context: Arc<TestContext>,
    messages: Vec<Arc<String>>,
}
impl TestReader {
    fn new(log: &Log<String>, context: &Arc<TestContext>) -> Self {
        Self {
            reader: log.subscribe_beginning(),
            context: Arc::clone(&context),
            messages: Vec::new(),
        }
    }
    fn spawn(mut self) -> JoinHandle<Vec<Arc<String>>> {
        spawn(move || {
            while !self.context.is_done() {
                match self.reader.poll() {
                    None => thread::yield_now(),
                    Some(x) => {
                        self.messages.push(x);
                        self.context.increment_consumed_count();
                    }
                }
            }
            self.context.increment_done_count();
            self.messages
        })
    }
}
