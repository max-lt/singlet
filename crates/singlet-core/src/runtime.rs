use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::File;
use std::fs::TryLockError;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::Actor;
use crate::Ctx;
use crate::Error;
use crate::Key;

/// Routes calls to one instance per key.
///
/// One runtime owns its data directory; a second `open` on the same
/// directory fails with [`Error::Locked`], even from another process. The
/// lock stays until the last instance stops, which can be `idle` after the
/// runtime drops.
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
    idle: Duration,
    make: Make<A>,
    registry: Mutex<Registry<A>>,
    _lock: File,
}

struct Registry<A: Actor> {
    entries: HashMap<A::Key, Entry<A>>,
    next_id: u64,
}

struct Entry<A: Actor> {
    id: u64,
    tx: mpsc::UnboundedSender<Envelope<A>>,
}

struct Envelope<A: Actor> {
    msg: A::Msg,
    reply: oneshot::Sender<Response<A>>,
}

enum Response<A: Actor> {
    Done(Result<A::Reply, Error>),
    Retry(A::Msg),
}

impl<A: Actor> Runtime<A> {
    /// Stops an instance after `idle` without calls. `make` builds the
    /// instance of a key each time it starts.
    pub fn open(
        dir: impl Into<PathBuf>,
        idle: Duration,
        make: impl Fn(&A::Key) -> A + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let dir = dir.into();
        let objects = dir.join("objects");
        std::fs::create_dir_all(&objects)?;

        let lock = File::create(dir.join("LOCK"))?;

        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(Error::Locked),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }

        Ok(Self {
            inner: Arc::new(Inner {
                objects,
                idle,
                make: Box::new(make),
                registry: Mutex::new(Registry {
                    entries: HashMap::new(),
                    next_id: 0,
                }),
                _lock: lock,
            }),
        })
    }

    /// Sends `msg` to the instance of `key` and waits for its reply.
    pub async fn call<Q>(&self, key: &Q, mut msg: A::Msg) -> Result<A::Reply, Error>
    where
        A::Key: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = A::Key> + ?Sized,
    {
        loop {
            let (reply, rx) = oneshot::channel();
            self.send(key, Envelope { msg, reply });

            match rx.await {
                Ok(Response::Done(result)) => return result,
                Ok(Response::Retry(m)) => msg = m,
                Err(_) => return Err(Error::Stopped),
            }
        }
    }

    // Sends under the registry lock, so an instance that finds its mailbox
    // empty under the same lock can stop without losing a message.
    fn send<Q>(&self, key: &Q, env: Envelope<A>)
    where
        A::Key: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = A::Key> + ?Sized,
    {
        let mut registry = self.inner.registry.lock().unwrap();

        if let Some(entry) = registry.entries.get(key) {
            entry
                .tx
                .send(env)
                .unwrap_or_else(|_| unreachable!("a mailbox closes after its entry goes"));

            return;
        }

        let id = registry.next_id;
        registry.next_id += 1;

        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(env).unwrap();
        let key = key.to_owned();
        registry.entries.insert(key.clone(), Entry { id, tx });

        let mailbox = Mailbox {
            key,
            id,
            rx,
            inner: self.inner.clone(),
        };

        tokio::spawn(run(mailbox));
    }
}

/// Removes its entry and sends back the calls it did not serve when it
/// drops, also on a panic. A new instance of the key can start only after
/// that.
struct Mailbox<A: Actor> {
    key: A::Key,
    id: u64,
    rx: mpsc::UnboundedReceiver<Envelope<A>>,
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

        while let Ok(env) = self.rx.try_recv() {
            env.reply.send(Response::Retry(env.msg)).ok();
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

    let mut ctx = match Ctx::open(&inner.objects, &mailbox.key.encode()) {
        Ok(ctx) => ctx,
        Err(e) => {
            let env = mailbox.rx.try_recv().expect("send puts the first call in");
            env.reply.send(Response::Done(Err(e))).ok();

            return;
        }
    };

    let mut actor = (inner.make)(&mailbox.key);

    loop {
        let env = match tokio::time::timeout(inner.idle, mailbox.rx.recv()).await {
            Ok(env) => env.expect("the entry holds a sender"),
            Err(_) if mailbox.retire() => return,
            Err(_) => continue,
        };

        let result = match actor.handle(&mut ctx, env.msg).await {
            Ok(reply) => ctx.commit().map(|()| reply),
            Err(e) => Err(e),
        };

        let failed = result.is_err();
        env.reply.send(Response::Done(result)).ok();

        if failed {
            return;
        }
    }
}
