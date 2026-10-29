use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Options;
use singlet_core::Runtime;
use tokio::sync::oneshot;

const IDLE: Duration = Duration::from_millis(50);

enum Cmd {
    Inc,
    Get,
    Fail,
    Panic,
    /// Holds the instance until `release`.
    Wait {
        started: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("refused with code {0}")]
struct Refused(u32);

#[derive(Debug, PartialEq)]
struct Count {
    stored: u64,
    served: u64,
}

#[derive(Default)]
struct Counter {
    served: u64,
}

impl Actor for Counter {
    type Key = String;
    type Msg = Cmd;
    type Reply = Count;

    async fn handle(&mut self, ctx: &mut Ctx, cmd: Cmd) -> Result<Count, Error> {
        let n = match ctx.get("n")? {
            Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
            None => 0,
        };

        // Gives another call a chance to run between the read and the write.
        tokio::task::yield_now().await;

        let stored = match cmd {
            Cmd::Inc => {
                ctx.put("n", &(n + 1).to_le_bytes())?;
                n + 1
            }
            Cmd::Get => n,
            Cmd::Fail => {
                ctx.put("n", &1000u64.to_le_bytes())?;

                return Err(Error::Actor(Box::new(Refused(7))));
            }
            Cmd::Panic => panic!("panic"),
            Cmd::Wait { started, release } => {
                started.send(()).ok();
                release.await.ok();
                n
            }
        };

        self.served += 1;

        Ok(Count {
            stored,
            served: self.served,
        })
    }
}

fn open(dir: &tempfile::TempDir) -> Runtime<Counter> {
    Runtime::open(
        dir.path(),
        Options {
            idle: IDLE,
            ..Options::default()
        },
        |_| Counter::default(),
    )
    .unwrap()
}

fn count(stored: u64, served: u64) -> Count {
    Count { stored, served }
}

async fn poll_once<F: Future>(mut call: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(call.as_mut().poll(cx))).await
}

/// Polls `call` once, so it is in the mailbox when this returns.
async fn enqueue<F: Future>(call: Pin<&mut F>) {
    assert!(poll_once(call).await.is_pending());
}

/// Sends a call that holds the instance of `key`, and returns once it runs.
async fn hold(rt: &Runtime<Counter>, key: &str) -> oneshot::Sender<()> {
    let (started, running) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let rt = rt.clone();
    let key = key.to_owned();

    tokio::spawn(async move {
        let cmd = Cmd::Wait {
            started,
            release: wait,
        };

        rt.call(&key, cmd).await.unwrap();
    });

    running.await.unwrap();

    release
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_for_one_key_do_not_interleave() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);

    let tasks: Vec<_> = (0..100)
        .map(|_| {
            let rt = rt.clone();
            tokio::spawn(async move { rt.call("room", Cmd::Inc).await.unwrap() })
        })
        .collect();

    let mut counts = Vec::new();

    for task in tasks {
        counts.push(task.await.unwrap().stored);
    }

    counts.sort();
    assert_eq!(counts, (1..=100).collect::<Vec<_>>());
}

#[tokio::test]
async fn storage_outlives_the_instance() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);

    assert_eq!(rt.call("room", Cmd::Inc).await.unwrap(), count(1, 1));
    assert_eq!(rt.call("room", Cmd::Inc).await.unwrap(), count(2, 2));

    tokio::time::sleep(IDLE * 4).await;

    assert_eq!(rt.call("room", Cmd::Inc).await.unwrap(), count(3, 1));
}

#[tokio::test]
async fn storage_outlives_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    rt.call("room", Cmd::Inc).await.unwrap();
    rt.shutdown().await;

    let rt = open(&dir);
    assert_eq!(rt.call("room", Cmd::Inc).await.unwrap(), count(2, 1));
}

#[tokio::test]
async fn a_failed_turn_discards_its_writes_and_the_instance() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    rt.call("room", Cmd::Inc).await.unwrap();

    let Err(Error::Actor(e)) = rt.call("room", Cmd::Fail).await else {
        panic!("the turn must fail with the error of the actor");
    };

    assert_eq!(e.downcast_ref::<Refused>(), Some(&Refused(7)));
    assert_eq!(rt.call("room", Cmd::Get).await.unwrap(), count(1, 1));
}

#[tokio::test]
async fn a_panic_stops_only_its_call() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    rt.call("room", Cmd::Inc).await.unwrap();

    assert!(matches!(
        rt.call("room", Cmd::Panic).await,
        Err(Error::Stopped)
    ));
    assert_eq!(rt.call("room", Cmd::Inc).await.unwrap(), count(2, 1));
}

#[tokio::test]
async fn a_waiting_turn_does_not_hold_other_keys() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    let _release = hold(&rt, "a").await;

    let b = tokio::time::timeout(Duration::from_secs(5), rt.call("b", Cmd::Inc)).await;
    assert_eq!(b.unwrap().unwrap(), count(1, 1));
}

#[tokio::test]
async fn one_runtime_per_directory() {
    let dir = tempfile::tempdir().unwrap();
    let _rt = open(&dir);

    let second = Runtime::open(
        dir.path(),
        Options {
            idle: IDLE,
            ..Options::default()
        },
        |_| Counter::default(),
    );
    assert!(matches!(second, Err(Error::Locked)));
}

#[tokio::test]
async fn a_full_mailbox_refuses_the_call() {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        idle: IDLE,
        mailbox: 2,
        ..Options::default()
    };
    let rt = Runtime::open(dir.path(), options, |_| Counter::default()).unwrap();
    let release = hold(&rt, "room").await;

    let mut waiting = [
        Box::pin(rt.call("room", Cmd::Inc)),
        Box::pin(rt.call("room", Cmd::Inc)),
    ];

    for call in &mut waiting {
        enqueue(call.as_mut()).await;
    }

    assert!(matches!(
        rt.call("room", Cmd::Inc).await,
        Err(Error::Overloaded)
    ));

    release.send(()).unwrap();

    for (call, n) in waiting.into_iter().zip(1..) {
        assert_eq!(call.await.unwrap().stored, n);
    }
}

#[tokio::test]
async fn shutdown_runs_the_waiting_calls_and_releases_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    let release = hold(&rt, "room").await;

    let mut waiting = [
        Box::pin(rt.call("room", Cmd::Inc)),
        Box::pin(rt.call("room", Cmd::Inc)),
    ];

    for call in &mut waiting {
        enqueue(call.as_mut()).await;
    }

    release.send(()).unwrap();
    rt.shutdown().await;

    for (call, n) in waiting.iter_mut().zip(1..) {
        let Poll::Ready(reply) = poll_once(call.as_mut()).await else {
            panic!("shutdown returns after the waiting calls");
        };

        assert_eq!(reply.unwrap().stored, n);
    }

    assert!(matches!(
        rt.call("room", Cmd::Inc).await,
        Err(Error::Closed)
    ));

    let rt = open(&dir);
    assert_eq!(rt.call("room", Cmd::Get).await.unwrap(), count(2, 1));
}
