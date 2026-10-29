use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::File;
use std::fs::TryLockError;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::Actor;
use crate::Ctx;
use crate::Error;
use crate::Key;
use crate::Options;
use crate::alarm;
use crate::alarm::Index;
use control::Control;
#[cfg(test)]
use control::Gate;
use control::TurnError;
use schedule::schedule;

const ABORT_WAIT: Duration = Duration::from_secs(1);

/// Routes calls to one instance per key, and wakes instances for their
/// alarms.
///
/// One runtime owns its data directory; a second `open` on the same
/// directory fails with [`Error::Locked`], even from another process. The
/// lock stays until [`Runtime::shutdown`] returns, or until the last
/// instance stops after the runtime drops.
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

impl<A: Actor> std::fmt::Debug for Runtime<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("options", &self.inner.options)
            .finish_non_exhaustive()
    }
}

type Make<A> = Box<dyn Fn(&<A as Actor>::Key) -> A + Send + Sync>;

struct Inner<A: Actor> {
    objects: PathBuf,
    options: Options,
    make: Make<A>,
    registry: Mutex<Registry<A>>,
    index: Arc<Index>,
    // The instances whose task has not ended.
    live: watch::Sender<usize>,
    // The turns that a shutdown aborted.
    aborted: AtomicUsize,
    scheduler: Mutex<Option<JoinHandle<()>>>,
    lock: Mutex<Option<File>>,
    #[cfg(test)]
    gate: Mutex<Option<Arc<Gate>>>,
}

impl<A: Actor> Inner<A> {
    /// Gives the reply of a turn. An aborted turn gets no reply, so its
    /// caller gets [`Error::Stopped`].
    fn reply<T>(&self, result: Result<T, TurnError>) -> Option<Result<T, Error>> {
        match result {
            Ok(value) => Some(Ok(value)),
            Err(TurnError::Failed(e)) => Some(Err(e)),
            Err(TurnError::Aborted) => {
                self.aborted.fetch_add(1, SeqCst);

                None
            }
        }
    }
}

impl<A: Actor> Drop for Inner<A> {
    fn drop(&mut self) {
        // Wakes the scheduler, so it sees that the runtime is gone.
        self.index.wake.notify_one();
    }
}

struct Registry<A: Actor> {
    entries: HashMap<A::Key, Entry<A>>,
    controls: HashMap<u64, Arc<Control>>,
    next_id: u64,
    closed: bool,
}

struct Entry<A: Actor> {
    id: u64,
    tx: mpsc::Sender<Job<A>>,
}

enum Job<A: Actor> {
    Call(A::Msg, oneshot::Sender<Response<A>>),
    // A dropped sender means the turn did not commit.
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
                controls: HashMap::new(),
                next_id: 0,
                closed: false,
            }),
            index: index.clone(),
            live: watch::Sender::new(0),
            aborted: AtomicUsize::new(0),
            scheduler: Mutex::default(),
            lock: Mutex::new(Some(lock)),
            #[cfg(test)]
            gate: Mutex::default(),
        });

        let scheduler = tokio::spawn(schedule(Arc::downgrade(&inner), index));
        *inner.scheduler.lock().unwrap() = Some(scheduler);

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

    /// Stops the runtime and releases the lock of its data directory.
    ///
    /// New calls fail with [`Error::Closed`], also through the clones of this
    /// runtime. The calls that wait run until `deadline`. After `deadline`:
    ///
    /// - A turn in [`Actor::handle`] or [`Actor::alarm`] is aborted. Its
    ///   writes do not commit, and its caller gets [`Error::Stopped`].
    /// - A turn in its commit is not aborted. It commits, and its caller gets
    ///   the reply.
    /// - A call that did not begin gets [`Error::Closed`].
    /// - An aborted alarm stays set, and the abort does not count as a try.
    ///
    /// The abort takes effect at the next `await` of the turn. A turn that
    /// runs with no `await` cannot be stopped.
    ///
    /// After the abort, shutdown waits 1 s more for the instances to stop.
    /// It releases the lock only when no instance runs, so a second runtime
    /// cannot open a file that an old instance can still write. A call to
    /// shutdown again waits again.
    ///
    /// Returns [`Error::Deadline`] if it aborted a turn or if an instance
    /// still runs.
    pub async fn shutdown(&self, deadline: Duration) -> Result<(), Error> {
        {
            let mut registry = self.inner.registry.lock().unwrap();
            registry.closed = true;
            registry.entries.clear();
        }

        self.inner.index.wake.notify_one();

        let mut live = self.inner.live.subscribe();

        if tokio::time::timeout(deadline, live.wait_for(|n| *n == 0))
            .await
            .is_err()
        {
            for control in self.inner.registry.lock().unwrap().controls.values() {
                control.stop();
            }

            tokio::time::timeout(ABORT_WAIT, live.wait_for(|n| *n == 0))
                .await
                .ok();
        }

        let running = *live.borrow();
        let aborted = self.inner.aborted.load(SeqCst);

        if running == 0 {
            let scheduler = self.inner.scheduler.lock().unwrap().take();

            if let Some(scheduler) = scheduler {
                scheduler.await.ok();
            }

            self.inner.lock.lock().unwrap().take();
        }

        if aborted == 0 && running == 0 {
            return Ok(());
        }

        Err(Error::Deadline { aborted, running })
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

        if registry.closed {
            return Err(Error::Closed);
        }

        let id = registry.next_id;
        registry.next_id += 1;

        let (tx, rx) = mpsc::channel(self.inner.options.mailbox);
        tx.try_send(job).unwrap();
        let key = key.to_owned();
        registry.entries.insert(key.clone(), Entry { id, tx });

        let control = Arc::new(Control {
            stop: AtomicBool::new(false),
            cancel: Notify::new(),
            #[cfg(test)]
            gate: self.inner.gate.lock().unwrap().clone(),
        });
        registry.controls.insert(id, control.clone());
        self.inner.live.send_modify(|n| *n += 1);

        let mailbox = Mailbox {
            key,
            id,
            rx,
            control,
            inner: self.inner.clone(),
        };

        tokio::spawn(run(mailbox));

        Ok(())
    }
}

/// Removes its entry and sends back the calls it did not serve when it
/// drops, also on a panic.
struct Mailbox<A: Actor> {
    key: A::Key,
    id: u64,
    rx: mpsc::Receiver<Job<A>>,
    control: Arc<Control>,
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
        registry.controls.remove(&self.id);
        drop(registry);

        while let Ok(job) = self.rx.try_recv() {
            if let Job::Call(msg, reply) = job {
                reply.send(Response::Retry(msg)).ok();
            }
        }

        self.inner.live.send_modify(|n| *n -= 1);
    }
}

// The mailbox drops after serve returns: a failed turn has rolled back
// before a new instance of the key starts, and the file is closed before
// shutdown counts the instance as stopped.
async fn run<A: Actor>(mut mailbox: Mailbox<A>) {
    serve(&mut mailbox).await;
}

// The open of a new file and the close of a file that has writes wait for
// fsync, so they run on a blocking thread.
async fn serve<A: Actor>(mailbox: &mut Mailbox<A>) {
    let objects = mailbox.inner.objects.clone();
    let key = mailbox.key.encode();
    let index = mailbox.inner.index.clone();

    let opened = tokio::task::spawn_blocking(move || Ctx::open(&objects, key, index))
        .await
        .expect("open does not panic");

    let mut ctx = match opened {
        Ok(ctx) => ctx,
        Err(e) => {
            match mailbox.rx.try_recv().expect("send puts the first job in") {
                Job::Call(_, reply) => reply.send(Response::Done(Err(e))).ok(),
                Job::Alarm(_, done) => done.send(Err(e)).ok(),
            };

            return;
        }
    };

    let mut actor = (mailbox.inner.make)(&mailbox.key);
    serve_turns(mailbox, &mut actor, &mut ctx).await;

    tokio::task::spawn_blocking(move || drop(ctx)).await.ok();
}

async fn serve_turns<A: Actor>(mailbox: &mut Mailbox<A>, actor: &mut A, ctx: &mut Ctx) {
    let inner = mailbox.inner.clone();
    let control = mailbox.control.clone();

    loop {
        let job = match tokio::time::timeout(inner.options.idle, mailbox.rx.recv()).await {
            Ok(Some(job)) => job,
            // A shutdown removed the entry, and the mailbox is empty.
            Ok(None) => return,
            Err(_) if mailbox.retire() => return,
            Err(_) => continue,
        };

        if control.stopped() {
            match job {
                // The call did not begin, so the caller sends it again and
                // gets Error::Closed.
                Job::Call(msg, reply) => reply.send(Response::Retry(msg)).ok(),
                Job::Alarm(_, done) => done.send(Err(Error::Closed)).ok(),
            };

            return;
        }

        let failed = match job {
            Job::Call(msg, reply) => {
                let result = call_turn(actor, ctx, &control, msg).await;
                let failed = result.is_err();

                if let Some(result) = inner.reply(result) {
                    reply.send(Response::Done(result)).ok();
                }

                failed
            }
            Job::Alarm(on_failure, done) => {
                let result = alarm_turn(actor, ctx, &control, on_failure).await;
                let failed = result.is_err();

                if let Some(result) = inner.reply(result) {
                    done.send(result).ok();
                }

                failed
            }
        };

        if failed {
            return;
        }
    }
}

async fn call_turn<A: Actor>(
    actor: &mut A,
    ctx: &mut Ctx,
    control: &Control,
    msg: A::Msg,
) -> Result<A::Reply, TurnError> {
    let value = control.run(actor.handle(ctx, msg)).await??;
    control.commit(ctx).await?;

    Ok(value)
}

async fn alarm_turn<A: Actor>(
    actor: &mut A,
    ctx: &mut Ctx,
    control: &Control,
    on_failure: OnFailure,
) -> Result<(), TurnError> {
    // The index wakes an object early, never late; the file decides.
    let Some(at) = ctx.alarm_ms()?.filter(|&at| at <= alarm::now_ms()) else {
        return Ok(ctx.sync_index().await?);
    };

    ctx.delete_alarm()?;

    // The last try commits the delete first, so its failure drops the alarm.
    if on_failure == OnFailure::Drop {
        control.commit(ctx).await?;
    }

    let result = async {
        control.run(actor.alarm(ctx)).await??;
        control.commit(ctx).await
    }
    .await;

    // An abort is not a failure, so the last try sets the alarm again.
    if on_failure == OnFailure::Drop && matches!(result, Err(TurnError::Aborted)) {
        ctx.rollback()?;
        ctx.set_alarm(alarm::from_ms(at))?;
        ctx.commit().await?;
    }

    result
}

mod control;
mod schedule;
#[cfg(test)]
mod tests;
