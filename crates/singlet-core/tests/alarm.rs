use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Options;
use singlet_core::Runtime;

const IDLE: Duration = Duration::from_millis(50);
const BACKOFF: Duration = Duration::from_millis(100);
const RETRIES: u32 = 2;

enum Cmd {
    /// Sets the alarm this far in the future.
    Arm(Duration),
    /// Sets the alarm, then fails the turn.
    ArmAndFail(Duration),
    Disarm,
    /// Returns the number of alarm turns that committed.
    Fired,
    /// Returns 1 if the alarm is set, else 0.
    Pending,
}

/// Fails the alarm turn while `failures` is above zero.
struct Clock {
    failures: &'static AtomicU32,
}

impl Actor for Clock {
    type Key = String;
    type Msg = Cmd;
    type Reply = u64;

    async fn handle(&mut self, ctx: &mut Ctx, cmd: Cmd) -> Result<u64, Error> {
        match cmd {
            Cmd::Arm(after) => ctx.set_alarm(SystemTime::now() + after)?,
            Cmd::ArmAndFail(after) => {
                ctx.set_alarm(SystemTime::now() + after)?;

                return Err(Error::Actor("fail".into()));
            }
            Cmd::Disarm => ctx.delete_alarm()?,
            Cmd::Fired => {}
            Cmd::Pending => return Ok(u64::from(ctx.alarm()?.is_some())),
        }

        fired(ctx)
    }

    async fn alarm(&mut self, ctx: &mut Ctx) -> Result<(), Error> {
        let left = self.failures.load(Ordering::SeqCst);

        if left > 0 {
            self.failures.store(left - 1, Ordering::SeqCst);

            return Err(Error::Actor("alarm failed".into()));
        }

        ctx.put("fired", &(fired(ctx)? + 1).to_le_bytes())?;

        Ok(())
    }
}

fn fired(ctx: &Ctx) -> Result<u64, Error> {
    Ok(match ctx.get("fired")? {
        Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
        None => 0,
    })
}

fn open(dir: &tempfile::TempDir, failures: &'static AtomicU32) -> Result<Runtime<Clock>, Error> {
    Runtime::open(
        dir.path(),
        Options {
            idle: IDLE,
            alarm_backoff: BACKOFF,
            alarm_retries: RETRIES,
            ..Options::default()
        },
        move |_| Clock { failures },
    )
}

async fn reopen(dir: &tempfile::TempDir, failures: &'static AtomicU32) -> Runtime<Clock> {
    loop {
        match open(dir, failures) {
            Ok(rt) => return rt,
            Err(Error::Locked) => tokio::time::sleep(IDLE).await,
            Err(e) => panic!("{e}"),
        }
    }
}

const MS: fn(u64) -> Duration = Duration::from_millis;

#[tokio::test]
async fn an_alarm_wakes_a_stopped_instance() {
    static FAILURES: AtomicU32 = AtomicU32::new(0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir, &FAILURES).unwrap();

    rt.call("job", Cmd::Arm(MS(200))).await.unwrap();

    // No call before the alarm is due, so the instance stops after IDLE.
    tokio::time::sleep(MS(400)).await;
    assert_eq!(rt.call("job", Cmd::Fired).await.unwrap(), 1);
}

#[tokio::test]
async fn an_alarm_survives_a_restart() {
    static FAILURES: AtomicU32 = AtomicU32::new(0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir, &FAILURES).unwrap();

    rt.call("job", Cmd::Arm(MS(400))).await.unwrap();
    drop(rt);

    let rt = reopen(&dir, &FAILURES).await;

    tokio::time::sleep(MS(600)).await;
    assert_eq!(rt.call("job", Cmd::Fired).await.unwrap(), 1);
}

#[tokio::test]
async fn an_alarm_due_while_no_runtime_runs_fires_on_open() {
    static FAILURES: AtomicU32 = AtomicU32::new(0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir, &FAILURES).unwrap();

    rt.call("job", Cmd::Arm(MS(300))).await.unwrap();
    drop(rt);

    // The lock is free, so no runtime runs when the alarm comes due.
    tokio::time::sleep(MS(200)).await;
    drop(open(&dir, &FAILURES).unwrap());
    tokio::time::sleep(MS(300)).await;

    let rt = reopen(&dir, &FAILURES).await;

    tokio::time::sleep(MS(200)).await;
    assert_eq!(rt.call("job", Cmd::Fired).await.unwrap(), 1);
}

#[tokio::test]
async fn an_alarm_from_a_failed_turn_does_not_fire() {
    static FAILURES: AtomicU32 = AtomicU32::new(0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir, &FAILURES).unwrap();

    assert!(rt.call("job", Cmd::ArmAndFail(MS(50))).await.is_err());

    tokio::time::sleep(MS(300)).await;
    assert_eq!(rt.call("job", Cmd::Fired).await.unwrap(), 0);
}

#[tokio::test]
async fn a_deleted_alarm_does_not_fire() {
    static FAILURES: AtomicU32 = AtomicU32::new(0);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir, &FAILURES).unwrap();

    rt.call("job", Cmd::Arm(MS(100))).await.unwrap();
    rt.call("job", Cmd::Disarm).await.unwrap();

    tokio::time::sleep(MS(300)).await;
    assert_eq!(rt.call("job", Cmd::Fired).await.unwrap(), 0);
}

#[tokio::test]
async fn a_failed_alarm_runs_again() {
    static FAILURES: AtomicU32 = AtomicU32::new(1);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir, &FAILURES).unwrap();

    rt.call("job", Cmd::Arm(Duration::ZERO)).await.unwrap();

    // The first run fails; the second comes after BACKOFF.
    tokio::time::sleep(MS(50)).await;
    assert_eq!(rt.call("job", Cmd::Fired).await.unwrap(), 0);

    tokio::time::sleep(MS(300)).await;
    assert_eq!(rt.call("job", Cmd::Fired).await.unwrap(), 1);
    assert_eq!(FAILURES.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_alarm_that_always_fails_is_dropped_after_the_retries() {
    static FAILURES: AtomicU32 = AtomicU32::new(100);
    let dir = tempfile::tempdir().unwrap();
    let rt = open(&dir, &FAILURES).unwrap();

    rt.call("job", Cmd::Arm(Duration::ZERO)).await.unwrap();

    // The tries come at 0, BACKOFF and 3 * BACKOFF.
    tokio::time::sleep(MS(800)).await;
    assert_eq!(FAILURES.load(Ordering::SeqCst), 100 - (RETRIES + 1));
    assert_eq!(rt.call("job", Cmd::Pending).await.unwrap(), 0);

    tokio::time::sleep(MS(500)).await;
    assert_eq!(FAILURES.load(Ordering::SeqCst), 100 - (RETRIES + 1));
    assert_eq!(rt.call("job", Cmd::Fired).await.unwrap(), 0);
}
