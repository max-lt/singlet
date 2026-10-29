use std::cell::Cell;
use std::sync::Once;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Instant;
use std::time::SystemTime;

use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::layer::SubscriberExt;

use super::*;

enum Cmd {
    /// Sets the alarm this far in the future.
    Arm(Duration),
    /// Returns the number of alarm turns that committed.
    Fired,
}

struct Clock;

impl Actor for Clock {
    type Key = String;
    type Msg = Cmd;
    type Reply = u64;

    async fn handle(&mut self, ctx: &mut Ctx, cmd: Cmd) -> Result<u64, Error> {
        if let Cmd::Arm(after) = cmd {
            ctx.set_alarm(SystemTime::now() + after)?;
        }

        fired(ctx)
    }

    async fn alarm(&mut self, ctx: &mut Ctx) -> Result<(), Error> {
        ctx.put("fired", &(fired(ctx)? + 1).to_le_bytes())
    }
}

fn fired(ctx: &Ctx) -> Result<u64, Error> {
    Ok(match ctx.get("fired")? {
        Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
        None => 0,
    })
}

fn open(dir: &tempfile::TempDir) -> Runtime<Clock> {
    Runtime::open(dir.path(), Options::default(), |_| Clock).unwrap()
}

thread_local! {
    static WARNINGS: Cell<usize> = const { Cell::new(0) };
}

struct CountWarnings;

impl<S: tracing::Subscriber> Layer<S> for CountWarnings {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() == tracing::Level::WARN {
            WARNINGS.with(|n| n.set(n.get() + 1));
        }
    }
}

/// Returns the WARN events of the current thread. The scheduler of a test
/// runs on the thread of the test, as the test runtime has one thread.
///
/// Call it before the first WARN: a subscriber per thread misses events, as
/// tracing caches the interest of a callsite for all threads.
fn warnings() -> usize {
    static GLOBAL: Once = Once::new();

    GLOBAL.call_once(|| {
        let subscriber = tracing_subscriber::registry().with(CountWarnings);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });

    WARNINGS.with(Cell::get)
}

#[tokio::test]
async fn an_alarm_fires_after_a_failed_read_of_the_index() {
    assert_eq!(warnings(), 0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    rt.call("job", Cmd::Arm(Duration::from_millis(100)))
        .await
        .unwrap();
    rt.shutdown().await;

    // No write wakes the scheduler, so only the retry reads the index again.
    let rt = open(&dir);
    rt.inner.index.fail_reads.store(1, SeqCst);

    let deadline = Instant::now() + INDEX_RETRY * 3;

    while rt.call("job", Cmd::Fired).await.unwrap() == 0 {
        assert!(Instant::now() < deadline, "the alarm never fired");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(rt.inner.index.fail_reads.load(SeqCst), 0);
    assert_eq!(warnings(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_failing_index_warns_once_per_streak() {
    assert_eq!(warnings(), 0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    let index = &rt.inner.index;
    index.fail_reads.store(u32::MAX, SeqCst);

    tokio::time::sleep(INDEX_RETRY * 5).await;
    assert!(u32::MAX - index.fail_reads.load(SeqCst) >= 5);
    assert_eq!(warnings(), 1);

    index.fail_reads.store(0, SeqCst);
    tokio::time::sleep(INDEX_RETRY * 2).await;

    index.fail_reads.store(u32::MAX, SeqCst);
    index.wake.notify_one();
    tokio::time::sleep(INDEX_RETRY * 5).await;
    assert_eq!(warnings(), 2);
}

#[tokio::test(start_paused = true)]
async fn shutdown_stops_a_scheduler_that_waits_to_retry() {
    assert_eq!(warnings(), 0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    rt.inner.index.fail_reads.store(u32::MAX, SeqCst);

    tokio::time::sleep(INDEX_RETRY / 10).await;
    assert!(rt.inner.index.fail_reads.load(SeqCst) < u32::MAX);

    let shutdown = tokio::time::timeout(INDEX_RETRY / 2, rt.shutdown()).await;
    assert!(shutdown.is_ok(), "shutdown waits for the retry");
}
