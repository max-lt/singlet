use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::File;
use std::fs::TryLockError;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;

use crate::Actor;
use crate::Ctx;
use crate::Error;
use crate::Key;
use crate::Options;
use crate::alarm;
use crate::alarm::Index;

const MAX_BACKOFF: Duration = Duration::from_secs(60);
const INDEX_RETRY: Duration = Duration::from_secs(1);

/// Routes calls to one instance per key, and wakes instances for their
/// alarms.
///
/// One runtime owns its data directory; a second `open` on the same
/// directory fails with [`Error::Locked`], even from another process. The
/// lock stays until the last instance stops, which can be
/// [`Options::idle`] after the runtime drops.
pub struct Runtime<A: Actor> {
    inner: Arc<Inner<A>>,
}

impl<A: Actor> Clone for Runtime<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

type Make<A> = Box<dyn Fn(&<A as Actor>::Key) -> A + Send + Sync>;

struct Inner<A: Actor> {
    objects: PathBuf,
    options: Options,
    make: Make<A>,
    registry: Mutex<Registry<A>>,
    index: Arc<Index>,
    _lock: File,
}

impl<A: Actor> Drop for Inner<A> {
    fn drop(&mut self) {
        // Wakes the scheduler, so it sees that the runtime is gone.
        self.index.wake.notify_one();
    }
}

struct Registry<A: Actor> {
    entries: HashMap<A::Key, Entry<A>>,
    next_id: u64,
}

struct Entry<A: Actor> {
    id: u64,
    tx: mpsc::Sender<Job<A>>,
}

enum Job<A: Actor> {
    Call(A::Msg, oneshot::Sender<Response<A>>),
    // A dropped sender means the alarm did not run.
    Alarm(OnFailure, oneshot::Sender<Result<(), Error>>),
}

#[derive(Clone, Copy, PartialEq)]
enum OnFailure {
    Retry,
    Drop,
}

enum Response<A: Actor> {
    Done(Result<A::Reply, Error>),
    Retry(A::Msg),
}

impl<A: Actor> Runtime<A> {
    /// `make` builds the instance of a key each time it starts.
    ///
    /// Must run inside a Tokio runtime: it starts the alarm scheduler.
    pub fn open(
        dir: impl Into<PathBuf>,
        options: Options,
        make: impl Fn(&A::Key) -> A + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        assert!(options.mailbox > 0, "Options::mailbox must be at least 1");

        let dir = dir.into();
        let objects = dir.join("objects");
        std::fs::create_dir_all(&objects)?;

        let lock = File::create(dir.join("LOCK"))?;

        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(Error::Locked),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }

        let index = Arc::new(Index::open(&dir.join("alarms.sqlite"))?);

        let inner = Arc::new(Inner {
            objects,
            options,
            make: Box::new(make),
            registry: Mutex::new(Registry {
                entries: HashMap::new(),
                next_id: 0,
            }),
            index: index.clone(),
            _lock: lock,
        });

        tokio::spawn(schedule(Arc::downgrade(&inner), index));

        Ok(Self { inner })
    }

    /// Sends `msg` to the instance of `key` and waits for its reply.
    ///
    /// An error means that the turn did not commit, so the call can be sent
    /// again.
    pub async fn call<Q>(&self, key: &Q, mut msg: A::Msg) -> Result<A::Reply, Error>
    where
        A::Key: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = A::Key> + ?Sized,
    {
        loop {
            let (reply, rx) = oneshot::channel();
            self.send(key, Job::Call(msg, reply))?;

            match rx.await {
                Ok(Response::Done(result)) => return result,
                Ok(Response::Retry(m)) => msg = m,
                Err(_) => return Err(Error::Stopped),
            }
        }
    }

    async fn alarm(&self, key: &A::Key, on_failure: OnFailure) -> Result<(), Error> {
        let (done, rx) = oneshot::channel();
        self.send(key, Job::Alarm(on_failure, done))?;

        rx.await.unwrap_or(Err(Error::Stopped))
    }

    // Sends under the registry lock, so an instance that finds its mailbox
    // empty under the same lock can stop without losing a message.
    fn send<Q>(&self, key: &Q, job: Job<A>) -> Result<(), Error>
    where
        A::Key: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = A::Key> + ?Sized,
    {
        let mut registry = self.inner.registry.lock().unwrap();

        if let Some(entry) = registry.entries.get(key) {
            return match entry.tx.try_send(job) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(_)) => Err(Error::Overloaded),
                Err(TrySendError::Closed(_)) => {
                    unreachable!("a mailbox closes after its entry goes")
                }
            };
        }

        let id = registry.next_id;
        registry.next_id += 1;

        let (tx, rx) = mpsc::channel(self.inner.options.mailbox);
        tx.try_send(job).unwrap();
        let key = key.to_owned();
        registry.entries.insert(key.clone(), Entry { id, tx });

        let mailbox = Mailbox {
            key,
            id,
            rx,
            inner: self.inner.clone(),
        };

        tokio::spawn(run(mailbox));

        Ok(())
    }
}

/// Removes its entry and sends back the calls it did not serve when it
/// drops, also on a panic. A new instance of the key can start only after
/// that.
struct Mailbox<A: Actor> {
    key: A::Key,
    id: u64,
    rx: mpsc::Receiver<Job<A>>,
    inner: Arc<Inner<A>>,
}

impl<A: Actor> Mailbox<A> {
    fn remove_entry(&self, registry: &mut Registry<A>) {
        if registry
            .entries
            .get(&self.key)
            .is_some_and(|e| e.id == self.id)
        {
            registry.entries.remove(&self.key);
        }
    }

    /// Removes the entry if no call is waiting.
    fn retire(&mut self) -> bool {
        let mut registry = self.inner.registry.lock().unwrap();

        if !self.rx.is_empty() {
            return false;
        }

        self.remove_entry(&mut registry);

        true
    }
}

impl<A: Actor> Drop for Mailbox<A> {
    fn drop(&mut self) {
        let mut registry = self.inner.registry.lock().unwrap();
        self.remove_entry(&mut registry);
        drop(registry);

        while let Ok(job) = self.rx.try_recv() {
            if let Job::Call(msg, reply) = job {
                reply.send(Response::Retry(msg)).ok();
            }
        }
    }
}

// The mailbox drops after the locals of serve, so the storage of this
// instance is closed before a new instance of the key can open it.
async fn run<A: Actor>(mut mailbox: Mailbox<A>) {
    serve(&mut mailbox).await;
}

async fn serve<A: Actor>(mailbox: &mut Mailbox<A>) {
    let inner = mailbox.inner.clone();

    let mut ctx = match Ctx::open(&inner.objects, mailbox.key.encode(), inner.index.clone()) {
        Ok(ctx) => ctx,
        Err(e) => {
            match mailbox.rx.try_recv().expect("send puts the first job in") {
                Job::Call(_, reply) => reply.send(Response::Done(Err(e))).ok(),
                Job::Alarm(_, done) => done.send(Err(e)).ok(),
            };

            return;
        }
    };

    let mut actor = (inner.make)(&mailbox.key);

    loop {
        let job = match tokio::time::timeout(inner.options.idle, mailbox.rx.recv()).await {
            Ok(job) => job.expect("the entry holds a sender"),
            Err(_) if mailbox.retire() => return,
            Err(_) => continue,
        };

        let failed = match job {
            Job::Call(msg, reply) => {
                let result = match actor.handle(&mut ctx, msg).await {
                    Ok(value) => ctx.commit().await.map(|()| value),
                    Err(e) => Err(e),
                };

                let failed = result.is_err();
                reply.send(Response::Done(result)).ok();

                failed
            }
            Job::Alarm(on_failure, done) => {
                let result = alarm_turn(&mut actor, &mut ctx, on_failure).await;

                let failed = result.is_err();
                done.send(result).ok();

                failed
            }
        };

        if failed {
            return;
        }
    }
}

async fn alarm_turn<A: Actor>(
    actor: &mut A,
    ctx: &mut Ctx,
    on_failure: OnFailure,
) -> Result<(), Error> {
    // The index wakes an object early, never late; the file decides.
    if ctx.alarm_ms()?.is_none_or(|at| at > alarm::now_ms()) {
        return ctx.sync_index();
    }

    ctx.delete_alarm()?;

    // The last try commits the delete first, so its failure drops the alarm.
    if on_failure == OnFailure::Drop {
        ctx.commit().await?;
    }

    actor.alarm(ctx).await?;

    ctx.commit().await
}

// Holds the runtime only while it reads the index, so a dropped runtime can
// go away while the scheduler waits.
async fn schedule<A: Actor>(inner: Weak<Inner<A>>, index: Arc<Index>) {
    loop {
        let next = {
            let Some(strong) = inner.upgrade() else {
                return;
            };

            spawn_due(&strong, &index)
        };

        let wait = match next {
            Ok(Some(at)) => Duration::from_millis((at - alarm::now_ms()).max(0).cast_unsigned()),
            Ok(None) => {
                index.wake.notified().await;
                continue;
            }
            // The index is a hint, so a failed read only delays the alarms.
            Err(_) => INDEX_RETRY,
        };

        tokio::time::timeout(wait, index.wake.notified()).await.ok();
    }
}

/// Starts a delivery for each due alarm, and returns the next alarm.
fn spawn_due<A: Actor>(inner: &Arc<Inner<A>>, index: &Arc<Index>) -> Result<Option<i64>, Error> {
    let now = alarm::now_ms();

    for key in index.take_due(now)? {
        tokio::spawn(deliver(Arc::downgrade(inner), index.clone(), key));
    }

    index.next_after(now)
}

async fn deliver<A: Actor>(inner: Weak<Inner<A>>, index: Arc<Index>, key: Vec<u8>) {
    let mut failures = 0;

    loop {
        let Some(inner) = inner.upgrade() else {
            break;
        };

        let Some(decoded) = A::Key::decode(&key) else {
            index.set(&key, None).ok();
            break;
        };

        let Options {
            alarm_retries,
            alarm_backoff,
            ..
        } = inner.options;

        let on_failure = if failures < alarm_retries {
            OnFailure::Retry
        } else {
            OnFailure::Drop
        };

        let ok = (Runtime { inner })
            .alarm(&decoded, on_failure)
            .await
            .is_ok();

        if ok || on_failure == OnFailure::Drop {
            break;
        }

        let backoff = alarm_backoff.saturating_mul(1 << failures.min(16));
        tokio::time::sleep(backoff.min(MAX_BACKOFF)).await;
        failures += 1;
    }

    index.delivered(&key);
}
