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
use crate::Error;
use crate::Options;
use crate::alarm::Index;
use control::Control;
#[cfg(test)]
use control::Gate;
use control::TurnError;
use instance::Mailbox;
use instance::run;
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
    // The most instances that ran at once.
    #[cfg(test)]
    peak: AtomicUsize,
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
    control: Arc<Control>,
}

impl<A: Actor> Entry<A> {
    fn is_idle(&self) -> bool {
        self.tx.capacity() == self.tx.max_capacity() && !self.control.busy.load(SeqCst)
    }
}

enum Sent<A: Actor> {
    Done,
    // The runtime runs max_live instances, and one of them stops soon.
    Wait(Job<A>),
}

// A dropped sender means that the turn began and did not commit.
enum Job<A: Actor> {
    Call(A::Msg, oneshot::Sender<Response<A::Reply, A::Msg>>),
    Alarm(OnFailure, oneshot::Sender<Response<(), OnFailure>>),
}

impl<A: Actor> Job<A> {
    /// Gives a job that did not begin back to its sender, which sends it
    /// again.
    fn resend(self) {
        match self {
            Job::Call(msg, reply) => reply.send(Response::Resend(msg)).ok(),
            Job::Alarm(on_failure, done) => done.send(Response::Resend(on_failure)).ok(),
        };
    }
}

#[derive(Clone, Copy, PartialEq)]
enum OnFailure {
    Retry,
    Drop,
}

enum Response<T, M> {
    Done(Result<T, Error>),
    // The job did not begin.
    Resend(M),
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
        assert!(options.max_live > 0, "Options::max_live must be at least 1");

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
            #[cfg(test)]
            peak: AtomicUsize::new(0),
        });

        let scheduler = tokio::spawn(schedule(Arc::downgrade(&inner), index));
        *inner.scheduler.lock().unwrap() = Some(scheduler);

        Ok(Self { inner })
    }

    /// Sends `msg` to the instance of `key` and waits for its reply.
    ///
    /// An error means that the turn did not commit, so the call can be sent
    /// again, except [`Error::Commit`]. A call from [`Actor::handle`] to its
    /// own key waits for ever.
    pub async fn call<Q>(&self, key: &Q, msg: A::Msg) -> Result<A::Reply, Error>
    where
        A::Key: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = A::Key> + ?Sized,
    {
        self.request(key, msg, Job::Call).await
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
        self.request(key, on_failure, Job::Alarm).await
    }

    async fn request<Q, T, M>(
        &self,
        key: &Q,
        mut input: M,
        job: fn(M, oneshot::Sender<Response<T, M>>) -> Job<A>,
    ) -> Result<T, Error>
    where
        A::Key: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = A::Key> + ?Sized,
    {
        loop {
            let (reply, rx) = oneshot::channel();
            self.send(key, job(input, reply)).await?;

            match rx.await {
                Ok(Response::Done(result)) => return result,
                Ok(Response::Resend(back)) => input = back,
                Err(_) => return Err(Error::Stopped),
            }
        }
    }

    async fn send<Q>(&self, key: &Q, mut job: Job<A>) -> Result<(), Error>
    where
        A::Key: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = A::Key> + ?Sized,
    {
        let max_live = self.inner.options.max_live;

        loop {
            match self.try_send(key, job)? {
                Sent::Done => return Ok(()),
                Sent::Wait(back) => job = back,
            }

            let mut live = self.inner.live.subscribe();
            live.wait_for(|n| *n < max_live).await.ok();
        }
    }

    // Sends under the registry lock, so an instance that finds its mailbox
    // empty under the same lock can stop without losing a message.
    fn try_send<Q>(&self, key: &Q, job: Job<A>) -> Result<Sent<A>, Error>
    where
        A::Key: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = A::Key> + ?Sized,
    {
        let mut registry = self.inner.registry.lock().unwrap();

        if let Some(entry) = registry.entries.get(key) {
            return match entry.tx.try_send(job) {
                Ok(()) => Ok(Sent::Done),
                Err(TrySendError::Full(_)) => Err(Error::Overloaded),
                Err(TrySendError::Closed(_)) => {
                    unreachable!("a mailbox closes after its entry goes")
                }
            };
        }

        if registry.closed {
            return Err(Error::Closed);
        }

        let live = *self.inner.live.borrow();

        if live >= self.inner.options.max_live {
            // An instance with no entry stops soon and frees its slot.
            if live == registry.entries.len() {
                let idle = registry
                    .entries
                    .iter()
                    .find(|(_, entry)| entry.is_idle())
                    .map(|(key, _)| key.clone());

                let Some(idle) = idle else {
                    return Err(Error::Overloaded);
                };

                // With no entry, the instance gets no call and stops.
                registry.entries.remove::<A::Key>(&idle);
            }

            return Ok(Sent::Wait(job));
        }

        let id = registry.next_id;
        registry.next_id += 1;

        let control = Arc::new(Control {
            stop: AtomicBool::new(false),
            busy: AtomicBool::new(false),
            cancel: Notify::new(),
            #[cfg(test)]
            gate: self.inner.gate.lock().unwrap().clone(),
        });

        let (tx, rx) = mpsc::channel(self.inner.options.mailbox);
        tx.try_send(job).unwrap();
        let key = key.to_owned();
        let entry = Entry {
            id,
            tx,
            control: control.clone(),
        };
        registry.entries.insert(key.clone(), entry);
        registry.controls.insert(id, control.clone());
        self.inner.live.send_modify(|n| *n += 1);

        #[cfg(test)]
        self.inner.peak.fetch_max(*self.inner.live.borrow(), SeqCst);

        let mailbox = Mailbox {
            key,
            id,
            rx,
            control,
            inner: self.inner.clone(),
        };

        tokio::spawn(run(mailbox));

        Ok(Sent::Done)
    }
}

mod control;
mod instance;
mod schedule;
#[cfg(test)]
mod tests;
