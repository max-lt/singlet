//! Runs random calls, bursts and pauses against a counter per key, and
//! checks every reply against a model. The pauses are near the idle time,
//! so instances stop while calls arrive.

#![no_main]

use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Runtime;

const KEYS: u64 = 4;
const IDLE: Duration = Duration::from_millis(2);
const MAX_BURST: u8 = 16;

static TOKIO: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap()
});

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Cmd {
    Inc,
    Get,
    Fail,
}

#[derive(Arbitrary, Debug)]
enum Op {
    Call { key: u8, cmd: Cmd },
    Burst { key: u8, n: u8 },
    Sleep { ms: u8 },
}

// A panic aborts the fuzzer, so the counter has no panic command.
struct Counter;

impl Actor for Counter {
    type Key = u64;
    type Msg = Cmd;
    type Reply = u64;

    async fn handle(&mut self, ctx: &mut Ctx, cmd: Cmd) -> Result<u64, Error> {
        let n = match ctx.get("n")? {
            Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
            None => 0,
        };

        tokio::task::yield_now().await;

        match cmd {
            Cmd::Inc => {
                ctx.put("n", &(n + 1).to_le_bytes())?;

                Ok(n + 1)
            }
            Cmd::Get => Ok(n),
            Cmd::Fail => {
                ctx.put("n", &u64::MAX.to_le_bytes())?;

                Err(Error::Actor("fail".into()))
            }
        }
    }
}

fn open(dir: &Path) -> Result<Runtime<Counter>, Error> {
    Runtime::open(dir, IDLE, |_| Counter)
}

fuzz_target!(|ops: Vec<Op>| {
    TOKIO.block_on(check(ops));
});

async fn check(ops: Vec<Op>) {
    let dir = tempfile::tempdir().unwrap();
    let rt = open(dir.path()).unwrap();
    let mut model = [0u64; KEYS as usize];

    for op in ops {
        match op {
            Op::Call { key, cmd } => {
                let key = u64::from(key) % KEYS;
                let count = &mut model[key as usize];
                let reply = rt.call(&key, cmd).await;

                match cmd {
                    Cmd::Inc => {
                        *count += 1;
                        assert_eq!(reply.unwrap(), *count);
                    }
                    Cmd::Get => assert_eq!(reply.unwrap(), *count),
                    Cmd::Fail => assert!(matches!(reply, Err(Error::Actor(_)))),
                }
            }
            Op::Burst { key, n } => {
                let key = u64::from(key) % KEYS;
                let n = u64::from(n % MAX_BURST + 1);
                let count = &mut model[key as usize];

                let tasks: Vec<_> = (0..n)
                    .map(|_| {
                        let rt = rt.clone();
                        tokio::spawn(async move { rt.call(&key, Cmd::Inc).await.unwrap() })
                    })
                    .collect();

                let mut replies = Vec::new();

                for task in tasks {
                    replies.push(task.await.unwrap());
                }

                replies.sort_unstable();
                assert_eq!(replies, (*count + 1..=*count + n).collect::<Vec<_>>());
                *count += n;
            }
            Op::Sleep { ms } => {
                tokio::time::sleep(Duration::from_millis(u64::from(ms % 5))).await;
            }
        }
    }

    drop(rt);

    // The lock goes when the last instance stops.
    let rt = loop {
        match open(dir.path()) {
            Ok(rt) => break rt,
            Err(Error::Locked) => tokio::time::sleep(IDLE).await,
            Err(e) => panic!("{e}"),
        }
    };

    for (key, count) in (0..KEYS).zip(model) {
        assert_eq!(rt.call(&key, Cmd::Get).await.unwrap(), count);
    }
}
