use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, spawn, JoinHandle},
    time::{Duration, SystemTime},
};

use malobu::{AtomicSubscription, LogBuffer, LogStream};

#[test]
fn single_writer_single_subscription() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = LogBuffer::new();
    let stream = buffer.create_stream();
    let writer_messages: Vec<String> = (0..10000).into_iter().map(|x| x.to_string()).collect();
    let writer = TestWriter::new(&stream, &context, &writer_messages)
        .with_burst_interval(Duration::from_millis(1))
        .with_messages_per_burst(100);
    let subscription = TestSubscription::new(&stream, &context);

    // execute test
    writer.spawn();
    let subscription_jh = subscription.spawn();
    context.start();
    context.join(1, 1, 10000);
    let subscription_msgs = subscription_jh.join().unwrap();

    // validations
    assert_eq!(10000, subscription_msgs.len());
    for i in 0..10000 {
        assert_eq!(subscription_msgs.get(i).unwrap().as_ref(), &i.to_string());
    }

    // validate consuming from beginning
    let from_beginning_messages = stream
        .subscribe_beginning_mut()
        .collect::<Vec<Arc<String>>>();
    assert_eq!(10000, from_beginning_messages.len());
    for i in 0..10000 {
        assert_eq!(
            from_beginning_messages.get(i).unwrap().as_ref(),
            &i.to_string()
        );
    }
}

#[test]
fn single_writer_multi_subscription() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = LogBuffer::new();
    let stream = buffer.create_stream();
    let writer_messages: Vec<String> = (0..10000).into_iter().map(|x| x.to_string()).collect();
    let writer = TestWriter::new(&stream, &context, &writer_messages)
        .with_burst_interval(Duration::from_millis(1))
        .with_messages_per_burst(100);
    let subscription1 = TestSubscription::new(&stream, &context);
    let subscription2 = TestSubscription::new(&stream, &context);

    // execute test
    writer.spawn();
    let subscription1_jh = subscription1.spawn();
    let subscription2_jh = subscription2.spawn();
    context.start();
    context.join(1, 2, 20000);
    let subscription1_msgs = subscription1_jh.join().unwrap();
    let subscription2_msgs = subscription2_jh.join().unwrap();

    // validations
    assert_eq!(10000, subscription1_msgs.len());
    assert_eq!(10000, subscription2_msgs.len());
    for i in 0..10000 {
        assert_eq!(subscription1_msgs.get(i).unwrap().as_ref(), &i.to_string());
        assert_eq!(subscription2_msgs.get(i).unwrap().as_ref(), &i.to_string());
    }

    // validate consuming from beginning
    let from_beginning_messages = stream
        .subscribe_beginning_mut()
        .collect::<Vec<Arc<String>>>();
    assert_eq!(10000, from_beginning_messages.len());
    for i in 0..10000 {
        assert_eq!(
            from_beginning_messages.get(i).unwrap().as_ref(),
            &i.to_string()
        );
    }
}

#[test]
fn multi_writer_single_subscription() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = LogBuffer::new();
    let stream = buffer.create_stream();
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
    let writer1 = TestWriter::new(&stream, &context, &writer1_messages)
        .with_burst_interval(Duration::from_millis(1))
        .with_messages_per_burst(100);
    let writer2 = TestWriter::new(&stream, &context, &writer2_messages)
        .with_burst_interval(Duration::from_millis(1))
        .with_messages_per_burst(200);
    let writer3 = TestWriter::new(&stream, &context, &writer3_messages)
        .with_burst_interval(Duration::from_micros(10))
        .with_messages_per_burst(50);
    let subscription = TestSubscription::new(&stream, &context);

    // execute test
    writer1.spawn();
    writer2.spawn();
    writer3.spawn();
    let subscription_jh = subscription.spawn();
    context.start();
    context.join(3, 1, 30000);
    let mut subscription_msgs = subscription_jh.join().unwrap();
    subscription_msgs.sort();

    // validations
    assert_eq!(30000, subscription_msgs.len());
    // sorted results, first 10000 will be writer1
    for i in 0..10000 {
        assert_eq!(
            subscription_msgs.get(i).unwrap().as_ref(),
            &format!("p1:{i:05}")
        );
    }
    // sorted results, next 10000 will be writer2
    for i in 0..10000 {
        assert_eq!(
            subscription_msgs.get(10000 + i).unwrap().as_ref(),
            &format!("p2:{i:05}")
        );
    }
    // sorted results, next 10000 will be writer3
    for i in 0..10000 {
        assert_eq!(
            subscription_msgs.get(20000 + i).unwrap().as_ref(),
            &format!("p3:{i:05}")
        );
    }

    // validate consuming from beginning
    let mut from_beginning_messages = stream
        .subscribe_beginning_mut()
        .collect::<Vec<Arc<String>>>();
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
    let buffer = LogBuffer::<String>::new();
    let stream = buffer.create_stream();

    // 50 writers of different rates
    for i in 0..50 {
        let messages: Vec<String> = (0..10000)
            .into_iter()
            .map(|x| format!("p{i:03}:{x:05}"))
            .collect();
        TestWriter::new(&stream, &context, &messages)
            .with_burst_interval(Duration::from_micros(100))
            .with_messages_per_burst(10)
            .spawn();
    }
    // 100 subscriptions
    for _ in 0..100 {
        TestSubscription::new(&stream, &context).spawn();
    }

    // execute test, which will verify exact count of messages were received
    context.start();
    context.join(50, 100, 50 * 100 * 10000);
}

#[test]
fn stress_with_trim() {
    // setup test
    let context = Arc::new(TestContext::new());
    let buffer = Arc::new(LogBuffer::<String>::new());
    let stream = buffer.create_stream();

    // 50 writers of different rates
    for i in 0..50 {
        let messages: Vec<String> = (0..10000)
            .into_iter()
            .map(|x| format!("p{i:03}:{x:05}"))
            .collect();
        TestWriter::new(&stream, &context, &messages)
            .with_burst_interval(Duration::from_micros(100))
            .with_messages_per_burst(10)
            .spawn();
    }
    // 100 subscriptions
    for _ in 0..100 {
        TestSubscription::new(&stream, &context).spawn();
    }
    // 5 trimmers
    Trimmer::new(&buffer, &context, 0, Duration::from_millis(10)).spawn();
    Trimmer::new(&buffer, &context, 0, Duration::from_millis(10)).spawn();
    Trimmer::new(&buffer, &context, 100, Duration::from_millis(50)).spawn();
    Trimmer::new(&buffer, &context, 100, Duration::from_millis(50)).spawn();
    Trimmer::new(&buffer, &context, 120, Duration::from_millis(50)).spawn();

    // execute test, which will verify exact count of messages were received
    context.start();
    context.join(50, 100, 50 * 100 * 10000);
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
    fn join(&self, writer_count: usize, subscription_count: usize, consumed_count: usize) {
        let started_at = SystemTime::now();
        let timeout_at = started_at + Duration::from_secs(60);

        // wait for writers to be complete
        while SystemTime::now() < timeout_at
            && self.done_count.load(Ordering::Relaxed) < writer_count
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(self.done_count.load(Ordering::Relaxed), writer_count);

        // wait for subscription messages to have all been received
        while SystemTime::now() < timeout_at
            && self.consumed_count.load(Ordering::Relaxed) < consumed_count
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(self.consumed_count.load(Ordering::Relaxed), consumed_count);

        // set done flag for subscriptions
        self.done_flag.store(true, Ordering::Relaxed);

        // wait for subscriptions to be complete
        while SystemTime::now() < timeout_at
            && self.done_count.load(Ordering::Relaxed) < (writer_count + subscription_count)
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            self.done_count.load(Ordering::Relaxed),
            writer_count + subscription_count
        );

        // calculate and print rate
        let duration = SystemTime::now().duration_since(started_at).unwrap();
        println!(
            "average rate: {}/sec/subscription",
            consumed_count as f64
                / (duration.as_micros() as f64 / 1000000.0)
                / subscription_count as f64
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
    stream: LogStream<String>,
    context: Arc<TestContext>,
    messages: VecDeque<String>,
    messages_per_burst: usize,
    burst_interval: Duration,
}
impl TestWriter {
    fn new(stream: &LogStream<String>, context: &Arc<TestContext>, messages: &Vec<String>) -> Self {
        Self {
            stream: stream.clone(),
            context: Arc::clone(context),
            messages: messages.iter().map(|x| x.to_owned()).collect(),
            messages_per_burst: 1,
            burst_interval: Duration::ZERO,
        }
    }
    fn with_messages_per_burst(mut self, messages_per_burst: usize) -> Self {
        self.messages_per_burst = messages_per_burst;
        self
    }
    fn with_burst_interval(mut self, burst_interval: Duration) -> Self {
        self.burst_interval = burst_interval;
        self
    }
    fn spawn(mut self) {
        spawn(move || {
            while !self.context.is_started() && !self.context.is_done() {
                thread::yield_now();
            }
            while !self.context.is_done() && !self.messages.is_empty() {
                for _ in 0..self.messages_per_burst {
                    let msg = self.messages.pop_front().unwrap();
                    self.stream.write(msg, 1);
                }
                thread::sleep(self.burst_interval);
            }
            self.context.increment_done_count();
        });
    }
}

struct TestSubscription {
    subscription: AtomicSubscription<String>,
    context: Arc<TestContext>,
    messages: Vec<Arc<String>>,
}
impl TestSubscription {
    fn new(stream: &LogStream<String>, context: &Arc<TestContext>) -> Self {
        Self {
            subscription: stream.subscribe_beginning(),
            context: Arc::clone(&context),
            messages: Vec::new(),
        }
    }
    fn spawn(mut self) -> JoinHandle<Vec<Arc<String>>> {
        spawn(move || {
            while !self.context.is_done() {
                match self.subscription.poll() {
                    None => thread::sleep(Duration::from_micros(100)),
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
