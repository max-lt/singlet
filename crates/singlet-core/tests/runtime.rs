use std::time::Duration;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Runtime;

const IDLE: Duration = Duration::from_millis(50);

/// Replies `<stored count> <calls served by this instance>`.
#[derive(Default)]
struct Counter {
    served: u64,
}

impl Actor for Counter {
    async fn handle(&mut self, ctx: &mut Ctx, msg: Vec<u8>) -> Result<Vec<u8>, Error> {
        let n: u64 = match ctx.get("n")? {
            Some(v) => String::from_utf8(v).unwrap().parse().unwrap(),
            None => 0,
        };

        // Gives another call a chance to run between the read and the write.
        tokio::task::yield_now().await;

        match msg.as_slice() {
            b"inc" => ctx.put("n", (n + 1).to_string().as_bytes())?,
            b"fail" => {
                ctx.put("n", b"1000")?;

                return Err(Error::Actor("fail".into()));
            }
            b"panic" => panic!("panic"),
            b"slow" => tokio::time::sleep(Duration::from_secs(1)).await,
            _ => {}
        }

        self.served += 1;
        let stored = ctx.get("n")?.unwrap_or_default();

        Ok(format!("{} {}", String::from_utf8(stored).unwrap(), self.served).into_bytes())
    }
}

fn open(dir: &tempfile::TempDir) -> Runtime<Counter> {
    Runtime::open(dir.path(), IDLE, |_| Counter::default()).unwrap()
}

async fn call(rt: &Runtime<Counter>, key: &str, msg: &str) -> Result<String, Error> {
    let reply = rt.call(key, msg.as_bytes().to_vec()).await?;

    Ok(String::from_utf8(reply).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_for_one_key_do_not_interleave() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);

    let tasks: Vec<_> = (0..100)
        .map(|_| {
            let rt = rt.clone();
            tokio::spawn(async move { call(&rt, "room", "inc").await.unwrap() })
        })
        .collect();

    let mut counts = Vec::new();

    for task in tasks {
        let reply = task.await.unwrap();
        counts.push(reply.split(' ').next().unwrap().parse::<u64>().unwrap());
    }

    counts.sort();
    assert_eq!(counts, (1..=100).collect::<Vec<_>>());
}

#[tokio::test]
async fn storage_outlives_the_instance() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);

    assert_eq!(call(&rt, "room", "inc").await.unwrap(), "1 1");
    assert_eq!(call(&rt, "room", "inc").await.unwrap(), "2 2");

    tokio::time::sleep(IDLE * 4).await;

    assert_eq!(call(&rt, "room", "inc").await.unwrap(), "3 1");
}

#[tokio::test]
async fn storage_outlives_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    call(&rt, "room", "inc").await.unwrap();
    drop(rt);

    tokio::time::sleep(IDLE * 4).await;

    let rt = open(&dir);
    assert_eq!(call(&rt, "room", "inc").await.unwrap(), "2 1");
}

#[tokio::test]
async fn a_failed_turn_discards_its_writes_and_the_instance() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    call(&rt, "room", "inc").await.unwrap();

    assert!(matches!(
        call(&rt, "room", "fail").await,
        Err(Error::Actor(_))
    ));
    assert_eq!(call(&rt, "room", "get").await.unwrap(), "1 1");
}

#[tokio::test]
async fn a_panic_stops_only_its_call() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);
    call(&rt, "room", "inc").await.unwrap();

    assert!(matches!(
        call(&rt, "room", "panic").await,
        Err(Error::Stopped)
    ));
    assert_eq!(call(&rt, "room", "inc").await.unwrap(), "2 1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keys_run_in_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir);

    let slow = {
        let rt = rt.clone();
        tokio::spawn(async move { call(&rt, "a", "slow").await })
    };

    tokio::time::sleep(Duration::from_millis(100)).await;

    let fast = tokio::time::timeout(Duration::from_millis(500), call(&rt, "b", "inc")).await;
    assert_eq!(fast.unwrap().unwrap(), "1 1");
    assert!(!slow.is_finished());
}

#[test]
fn one_runtime_per_directory() {
    let dir = tempfile::tempdir().unwrap();
    let _rt = open(&dir);

    let second = Runtime::open(dir.path(), IDLE, |_| Counter::default());
    assert!(matches!(second, Err(Error::Locked)));
}
