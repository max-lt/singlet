use std::collections::HashMap;
use std::fs::File;
use std::fs::TryLockError;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::Actor;
use crate::Ctx;
use crate::Error;

/// Routes calls to one instance per key.
///
/// One runtime owns its data directory; a second `open` on the same
/// directory fails with [`Error::Locked`], even from another process.
pub struct Runtime<A> {
    inner: Arc<Inner<A>>,
}

impl<A> Clone for Runtime<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<A> {
    objects: PathBuf,
    idle: Duration,
    make: Box<dyn Fn(&str) -> A + Send + Sync>,
    registry: Arc<Mutex<Registry>>,
    _lock: File,
}

#[derive(Default)]
struct Registry {
    entries: HashMap<String, Entry>,
    next_id: u64,
}

struct Entry {
    id: u64,
    tx: mpsc::UnboundedSender<Envelope>,
}

struct Envelope {
    msg: Vec<u8>,
    reply: oneshot::Sender<Reply>,
}

enum Reply {
    Done(Result<Vec<u8>, Error>),
    Retry(Vec<u8>),
}

impl<A: Actor> Runtime<A> {
    /// Stops an instance after `idle` without calls. `make` builds the
    /// instance of a key each time it starts.
    pub fn open(
        dir: impl Into<PathBuf>,
        idle: Duration,
        make: impl Fn(&str) -> A + Send + Sync + 'static,
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
                registry: Arc::default(),
                _lock: lock,
            }),
        })
    }

    /// Sends `msg` to the instance of `key` and waits for its reply.
    pub async fn call(&self, key: &str, mut msg: Vec<u8>) -> Result<Vec<u8>, Error> {
        loop {
            let (reply, rx) = oneshot::channel();
            self.send(key, Envelope { msg, reply });

            match rx.await {
                Ok(Reply::Done(result)) => return result,
                Ok(Reply::Retry(m)) => msg = m,
                Err(_) => return Err(Error::Stopped),
            }
        }
    }

    // Sends under the registry lock, so an instance that finds its mailbox
    // empty under the same lock can stop without losing a message.
    fn send(&self, key: &str, env: Envelope) {
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
        registry.entries.insert(key.to_owned(), Entry { id, tx });

        let mailbox = Mailbox {
            key: key.to_owned(),
            id,
            rx,
            registry: self.inner.registry.clone(),
        };

        tokio::spawn(run(self.inner.clone(), mailbox));
    }
}

/// Removes its entry and sends back the calls it did not serve when it
/// drops, also on a panic. A new instance of the key can start only after
/// that.
struct Mailbox {
    key: String,
    id: u64,
    rx: mpsc::UnboundedReceiver<Envelope>,
    registry: Arc<Mutex<Registry>>,
}

impl Mailbox {
    fn remove_entry(&self, registry: &mut Registry) {
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
        let mut registry = self.registry.lock().unwrap();

        if !self.rx.is_empty() {
            return false;
        }

        self.remove_entry(&mut registry);

        true
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap();
        self.remove_entry(&mut registry);
        drop(registry);

        self.rx.close();

        while let Ok(env) = self.rx.try_recv() {
            env.reply.send(Reply::Retry(env.msg)).ok();
        }
    }
}

// The mailbox drops after the locals of serve, so the storage of this
// instance is closed before a new instance of the key can open it.
async fn run<A: Actor>(inner: Arc<Inner<A>>, mut mailbox: Mailbox) {
    serve(&inner, &mut mailbox).await;
}

async fn serve<A: Actor>(inner: &Inner<A>, mailbox: &mut Mailbox) {
    let mut ctx = match Ctx::open(&inner.objects, mailbox.key.clone()) {
        Ok(ctx) => ctx,
        Err(e) => {
            let env = mailbox.rx.try_recv().unwrap();
            env.reply.send(Reply::Done(Err(e))).ok();

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
        env.reply.send(Reply::Done(result)).ok();

        if failed {
            return;
        }
    }
}
