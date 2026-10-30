//! Runs random calls, bursts, alarms, pauses and restarts against a counter
//! per key, and checks every reply against a model. The pauses are near the
//! idle time, so instances stop while calls and alarms arrive. MAX_LIVE is
//! below KEYS, so a new key often stops an idle instance. An abort hangs a
//! call or an alarm turn, and a shutdown with a short deadline aborts it.

#![no_main]

use std::path::Path;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Options;
use singlet_core::Runtime;
use tokio::sync::Notify;

const KEYS: u64 = 4;
const MAX_LIVE: usize = 2;
const IDLE: Duration = Duration::from_millis(2);
const MAX_BURST: u8 = 16;
const ALARM_DEADLINE: Duration = Duration::from_secs(10);
const DEADLINE: Duration = Duration::from_secs(10);
const ABORT_DEADLINE: Duration = Duration::from_millis(5);

// The key whose next alarm turn hangs.
static HANG_ALARM: Mutex<Option<u64>> = Mutex::new(None);
static HUNG: LazyLock<Notify> = LazyLock::new(Notify::new);

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
    /// Writes the count and an alarm, then fails: neither may stay.
    Fail,
    Arm {
        ms: u8,
    },
    Fired,
    Pending,
    /// Writes the count and an alarm, then waits for ever: neither may stay.
    #[arbitrary(skip)]
    Hang,
}

#[derive(Arbitrary, Debug)]
enum Op {
    Call { key: u8, cmd: Cmd },
    Burst { key: u8, n: u8 },
    Sleep { ms: u8 },
    Restart,
    Abort { key: u8, alarm: bool, queued: u8 },
}

// A panic aborts the fuzzer, so the counter has no panic command.
struct Counter {
    key: u64,
}

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
                ctx.set_alarm(SystemTime::now())?;

                Err(Error::Actor("fail".into()))
            }
            Cmd::Arm { ms } => {
                ctx.set_alarm(SystemTime::now() + Duration::from_millis(u64::from(ms % 5)))?;

                Ok(n)
            }
            Cmd::Fired => fired(ctx),
            Cmd::Pending => Ok(u64::from(ctx.alarm()?.is_some())),
            Cmd::Hang => {
                ctx.put("n", &u64::MAX.to_le_bytes())?;
                ctx.set_alarm(SystemTime::now())?;

                hang().await
            }
        }
    }

    async fn alarm(&mut self, ctx: &mut Ctx) -> Result<(), Error> {
        if HANG_ALARM
            .lock()
            .unwrap()
            .take_if(|key| *key == self.key)
            .is_some()
        {
            ctx.put("n", &u64::MAX.to_le_bytes())?;

            return hang().await;
        }

        ctx.put("fired", &(fired(ctx)? + 1).to_le_bytes())
    }
}

async fn hang<T>() -> T {
    HUNG.notify_one();

    std::future::pending().await
}

fn fired(ctx: &Ctx) -> Result<u64, Error> {
    Ok(match ctx.get("fired")? {
        Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
        None => 0,
    })
}

fn open(dir: &Path) -> Result<Runtime<Counter>, Error> {
    Runtime::open(
        dir,
        Options {
            idle: IDLE,
            max_live: MAX_LIVE,
            ..Options::default()
        },
        |key| Counter { key: *key },
    )
}

// An Overloaded call did not run, so the model stays the same.
async fn call(rt: &Runtime<Counter>, key: u64, cmd: Cmd) -> Result<u64, Error> {
    loop {
        match rt.call(&key, cmd).await {
            Err(Error::Overloaded) => tokio::time::sleep(IDLE).await,
            reply => return reply,
        }
    }
}

fn spawn(rt: &Runtime<Counter>, key: u64, cmd: Cmd) -> tokio::task::JoinHandle<Result<u64, Error>> {
    let rt = rt.clone();

    tokio::spawn(async move { call(&rt, key, cmd).await })
}

fuzz_target!(|ops: Vec<Op>| {
    TOKIO.block_on(check(ops));
});

async fn check(ops: Vec<Op>) {
    let dir = tempfile::tempdir().unwrap();
    let mut rt = open(dir.path()).unwrap();
    let mut model = [0u64; KEYS as usize];
    let mut armed = [false; KEYS as usize];

    for op in ops {
        match op {
            Op::Call { key, cmd } => {
                let key = u64::from(key) % KEYS;
                let count = &mut model[key as usize];
                let reply = call(&rt, key, cmd).await;

                match cmd {
                    Cmd::Inc => {
                        *count += 1;
                        assert_eq!(reply.unwrap(), *count);
                    }
                    Cmd::Get => assert_eq!(reply.unwrap(), *count),
                    Cmd::Fail => assert!(matches!(reply, Err(Error::Actor(_)))),
                    Cmd::Arm { .. } => {
                        armed[key as usize] = true;
                        assert_eq!(reply.unwrap(), *count);
                    }
                    Cmd::Fired | Cmd::Pending => {
                        reply.unwrap();
                    }
                    Cmd::Hang => unreachable!("Op::Abort sends Cmd::Hang"),
                }
            }
            Op::Burst { key, n } => {
                let key = u64::from(key) % KEYS;
                let n = u64::from(n % MAX_BURST + 1);
                let count = &mut model[key as usize];

                let tasks: Vec<_> = (0..n)
                    .map(|_| {
                        let rt = rt.clone();
                        tokio::spawn(async move { call(&rt, key, Cmd::Inc).await.unwrap() })
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
            Op::Restart => {
                rt.shutdown(DEADLINE).await.unwrap();
                rt = open(dir.path()).unwrap();
            }
            Op::Abort { key, alarm, queued } => {
                let key = u64::from(key) % KEYS;
                let count = model[key as usize];

                // A due alarm of the key can hang before this call, so the
                // call can wait behind it and get Closed.
                let hung = if alarm {
                    *HANG_ALARM.lock().unwrap() = Some(key);
                    armed[key as usize] = true;

                    spawn(&rt, key, Cmd::Arm { ms: 0 })
                } else {
                    spawn(&rt, key, Cmd::Hang)
                };

                tokio::time::timeout(DEADLINE, HUNG.notified())
                    .await
                    .expect("the turn hangs");

                let waiting: Vec<_> = (0..queued % MAX_BURST)
                    .map(|_| spawn(&rt, key, Cmd::Inc))
                    .collect();

                // An alarm turn of another key can reach its commit after the
                // stop, so more than one turn can abort.
                let shutdown = rt.shutdown(ABORT_DEADLINE).await;
                assert!(
                    matches!(
                        shutdown,
                        Err(Error::Deadline {
                            aborted: 1..,
                            running: 0
                        })
                    ),
                    "{shutdown:?}"
                );

                match hung.await.unwrap() {
                    Ok(reply) => assert!(alarm && reply == count),
                    Err(Error::Stopped) => assert!(!alarm),
                    Err(Error::Closed) => assert!(alarm),
                    Err(e) => panic!("{e:?}"),
                }

                for call in waiting {
                    assert!(matches!(call.await.unwrap(), Err(Error::Closed)));
                }

                rt = open(dir.path()).unwrap();
            }
        }
    }

    rt.shutdown(DEADLINE).await.unwrap();
    let rt = open(dir.path()).unwrap();

    let deadline = Instant::now() + ALARM_DEADLINE;

    for key in 0..KEYS {
        while call(&rt, key, Cmd::Pending).await.unwrap() == 1 {
            assert!(Instant::now() < deadline, "alarm of key {key} never fired");
            tokio::time::sleep(IDLE).await;
        }

        let fired = call(&rt, key, Cmd::Fired).await.unwrap();
        assert_eq!(
            fired > 0,
            armed[key as usize],
            "key {key} fired {fired} times"
        );
        assert_eq!(call(&rt, key, Cmd::Get).await.unwrap(), model[key as usize]);
    }
}
