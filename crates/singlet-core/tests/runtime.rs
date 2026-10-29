use std::time::Duration;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Options;
use singlet_core::Runtime;

const IDLE: Duration = Duration::from_millis(50);

enum Cmd {
    Inc,
    Get,
    Fail,
    Panic,
    Slow,
}

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

                return Err(Error::Actor("fail".into()));
            }
            Cmd::Panic => panic!("panic"),
            Cmd::Slow => {
                tokio::time::sleep(Duration::from_secs(1)).await;
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
    Runtime::open(dir.path(), Options { idle: IDLE }, |_| Counter::default()).unwrap()
}

fn count(stored: u64, served: u64) -> Count {
    Count { stored, served }
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
    drop(rt);

    tokio::time::sleep(IDLE * 4).await;

    let rt = open(&dir);
    assert_eq!(rt.call("room", Cmd::Inc).await.unwrap(), count(2, 1));
}

#[tokio::test]
async fn a_failed_turn_discards_its_writes_and_the_instance() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    rt.call("room", Cmd::Inc).await.unwrap();

    assert!(matches!(
        rt.call("room", Cmd::Fail).await,
        Err(Error::Actor(_))
    ));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keys_run_in_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);

    let slow = {
        let rt = rt.clone();
        tokio::spawn(async move { rt.call("a", Cmd::Slow).await })
    };

    tokio::time::sleep(Duration::from_millis(100)).await;

    let fast = tokio::time::timeout(Duration::from_millis(500), rt.call("b", Cmd::Inc)).await;
    assert_eq!(fast.unwrap().unwrap(), count(1, 1));
    assert!(!slow.is_finished());
}

#[tokio::test]
async fn one_runtime_per_directory() {
    let dir = tempfile::tempdir().unwrap();
    let _rt = open(&dir);

    let second = Runtime::open(dir.path(), Options { idle: IDLE }, |_| Counter::default());
    assert!(matches!(second, Err(Error::Locked)));
}
