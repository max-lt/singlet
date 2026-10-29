use std::cell::Cell;
use std::sync::Once;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::time::SystemTime;

use tokio::sync::Notify;
use tokio::time::Instant;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::layer::SubscriberExt;

use super::control::Point;
use super::schedule::INDEX_RETRY;
use super::*;

const DEADLINE: Duration = Duration::from_millis(100);

enum Cmd {
    /// Sets the alarm this far in the future.
    Arm(Duration),
    /// Returns the number of alarm turns that committed.
    Fired,
    /// Adds 1 to the count and returns it.
    Inc,
    /// Returns the count.
    Get,
    /// Writes the count, then waits for ever.
    Hang,
    /// Writes the count, sets `started`, then runs with no await while
    /// `spin` is set.
    Spin {
        started: &'static AtomicBool,
        spin: &'static AtomicBool,
    },
}

/// Waits for ever in `alarm` if `hang` is set, after a notify on it.
#[derive(Default)]
struct Clock {
    hang: Option<Arc<Notify>>,
}

impl Actor for Clock {
    type Key = String;
    type Msg = Cmd;
    type Reply = u64;

    async fn handle(&mut self, ctx: &mut Ctx, cmd: Cmd) -> Result<u64, Error> {
        match cmd {
            Cmd::Arm(after) => ctx.set_alarm(SystemTime::now() + after)?,
            Cmd::Fired => {}
            Cmd::Inc => {
                let n = read(ctx, "n")? + 1;
                ctx.put("n", &n.to_le_bytes())?;

                return Ok(n);
            }
            Cmd::Get => return read(ctx, "n"),
            Cmd::Hang => {
                ctx.put("n", &99u64.to_le_bytes())?;
                std::future::pending::<()>().await;
            }
            Cmd::Spin { started, spin } => {
                ctx.put("n", &99u64.to_le_bytes())?;
                started.store(true, SeqCst);

                while spin.load(SeqCst) {
                    std::hint::spin_loop();
                }

                return read(ctx, "n");
            }
        }

        read(ctx, "fired")
    }

    async fn alarm(&mut self, ctx: &mut Ctx) -> Result<(), Error> {
        if let Some(hang) = &self.hang {
            hang.notify_one();
            std::future::pending::<()>().await;
        }

        ctx.put("fired", &(read(ctx, "fired")? + 1).to_le_bytes())
    }
}

fn read(ctx: &Ctx, key: &str) -> Result<u64, Error> {
    Ok(match ctx.get(key)? {
        Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
        None => 0,
    })
}

fn open(dir: &tempfile::TempDir) -> Runtime<Clock> {
    Runtime::open(dir.path(), Options::default(), |_| Clock::default()).unwrap()
}

/// Holds the next instance that reaches `at` until `open`.
fn gate(rt: &Runtime<Clock>, at: Point) -> Arc<Gate> {
    let gate = Arc::new(Gate {
        at,
        reached: Notify::new(),
        open: Notify::new(),
    });
    *rt.inner.gate.lock().unwrap() = Some(gate.clone());

    gate
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

// Real time: an alarm is due by SystemTime, which a paused clock does not
// move, so the test clock passes the deadline before the alarm is due.
#[tokio::test]
async fn an_alarm_fires_after_a_failed_read_of_the_index() {
    assert_eq!(warnings(), 0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    rt.call("job", Cmd::Arm(Duration::from_millis(100)))
        .await
        .unwrap();
    rt.shutdown(DEADLINE).await.unwrap();

    // No write wakes the scheduler, so only the retry reads the index again.
    let rt = open(&dir);
    rt.inner.index.fail_reads.store(1, SeqCst);

    let deadline = std::time::Instant::now() + INDEX_RETRY * 3;

    while rt.call("job", Cmd::Fired).await.unwrap() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the alarm never fired"
        );
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

    let shutdown = tokio::time::timeout(INDEX_RETRY / 2, rt.shutdown(DEADLINE)).await;
    assert!(
        matches!(shutdown, Ok(Ok(()))),
        "shutdown waits for the retry"
    );
}

#[tokio::test(start_paused = true)]
async fn a_shutdown_with_no_hung_turn_returns_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    rt.call("a", Cmd::Inc).await.unwrap();

    let start = Instant::now();
    rt.shutdown(Duration::from_secs(3600)).await.unwrap();
    assert_eq!(start.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn shutdown_aborts_a_hung_turn_at_the_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);

    let hung = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.call("a", Cmd::Hang).await })
    };

    // The clock moves only when no task can run, so the call is waiting.
    tokio::time::sleep(Duration::from_millis(1)).await;

    let start = Instant::now();
    let shutdown = rt.shutdown(DEADLINE).await;
    assert!(start.elapsed() >= DEADLINE);
    assert!(matches!(
        shutdown,
        Err(Error::Deadline {
            aborted: 1,
            running: 0
        })
    ));
    assert!(matches!(hung.await.unwrap(), Err(Error::Stopped)));

    let rt = open(&dir);
    assert_eq!(rt.call("a", Cmd::Get).await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_keeps_the_lock_while_a_turn_never_yields() {
    static STARTED: AtomicBool = AtomicBool::new(false);
    static SPIN: AtomicBool = AtomicBool::new(true);

    // Frees the worker thread also when an assert fails.
    struct Release;

    impl Drop for Release {
        fn drop(&mut self) {
            SPIN.store(false, SeqCst);
        }
    }

    let _release = Release;
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);

    let spinning = {
        let rt = rt.clone();
        let cmd = Cmd::Spin {
            started: &STARTED,
            spin: &SPIN,
        };

        tokio::spawn(async move { rt.call("a", cmd).await })
    };

    while !STARTED.load(SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let shutdown = rt.shutdown(DEADLINE).await;
    assert!(matches!(
        shutdown,
        Err(Error::Deadline {
            aborted: 0,
            running: 1
        })
    ));

    let second = Runtime::open(dir.path(), Options::default(), |_| Clock::default());
    assert!(matches!(second, Err(Error::Locked)));

    SPIN.store(false, SeqCst);
    assert!(matches!(spinning.await.unwrap(), Err(Error::Stopped)));

    let shutdown = rt.shutdown(DEADLINE).await;
    assert!(matches!(
        shutdown,
        Err(Error::Deadline {
            aborted: 1,
            running: 0
        })
    ));

    let rt = open(&dir);
    assert_eq!(rt.call("a", Cmd::Get).await.unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_turn_held_before_its_commit_does_not_commit() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    let gate = gate(&rt, Point::BeforeCommit);

    let call = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.call("a", Cmd::Inc).await })
    };

    gate.reached.notified().await;

    let shutdown = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.shutdown(DEADLINE).await })
    };

    // The deadline passes while the turn waits at the gate.
    tokio::time::sleep(DEADLINE * 2).await;
    gate.open.notify_one();

    assert!(matches!(call.await.unwrap(), Err(Error::Stopped)));
    assert!(matches!(
        shutdown.await.unwrap(),
        Err(Error::Deadline {
            aborted: 1,
            running: 0
        })
    ));

    let rt = open(&dir);
    assert_eq!(rt.call("a", Cmd::Get).await.unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_turn_in_its_commit_commits_and_replies() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    let gate = gate(&rt, Point::InCommit);

    let call = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.call("a", Cmd::Inc).await })
    };

    gate.reached.notified().await;

    let shutdown = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.shutdown(DEADLINE).await })
    };

    tokio::time::sleep(DEADLINE * 2).await;
    gate.open.notify_one();

    assert_eq!(call.await.unwrap().unwrap(), 1);
    assert!(matches!(shutdown.await.unwrap(), Ok(())));

    let rt = open(&dir);
    assert_eq!(rt.call("a", Cmd::Get).await.unwrap(), 1);
}

async fn an_aborted_alarm_stays_armed(alarm_retries: u32) {
    let dir = tempfile::tempdir().unwrap();
    let hang = Arc::new(Notify::new());

    let rt = {
        let options = Options {
            alarm_retries,
            ..Options::default()
        };
        let hang = hang.clone();

        Runtime::open(dir.path(), options, move |_| Clock {
            hang: Some(hang.clone()),
        })
        .unwrap()
    };

    rt.call("job", Cmd::Arm(Duration::ZERO)).await.unwrap();
    hang.notified().await;

    assert!(matches!(
        rt.shutdown(DEADLINE).await,
        Err(Error::Deadline {
            aborted: 1,
            running: 0
        })
    ));

    let rt = open(&dir);
    let deadline = Instant::now() + Duration::from_secs(3);

    while rt.call("job", Cmd::Fired).await.unwrap() == 0 {
        assert!(Instant::now() < deadline, "the aborted alarm never fired");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn an_aborted_alarm_with_retries_left_stays_armed() {
    an_aborted_alarm_stays_armed(1).await;
}

// With no retries left, the turn commits the delete of the alarm before it
// runs.
#[tokio::test(start_paused = true)]
async fn an_aborted_alarm_on_its_last_try_stays_armed() {
    an_aborted_alarm_stays_armed(0).await;
}
