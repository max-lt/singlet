use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;

use tokio::sync::mpsc;

use super::Inner;
use super::Job;
use super::OnFailure;
use super::Registry;
use super::Response;
use super::control::Control;
use super::control::TurnError;
use crate::Actor;
use crate::Ctx;
use crate::Error;
use crate::Key;
use crate::alarm;

/// Removes its entry and sends back the jobs it did not serve when it
/// drops, also on a panic.
pub(super) struct Mailbox<A: Actor> {
    pub(super) key: A::Key,
    pub(super) id: u64,
    pub(super) rx: mpsc::Receiver<Job<A>>,
    pub(super) control: Arc<Control>,
    pub(super) inner: Arc<Inner<A>>,
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
    fn retire(&self) -> bool {
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
            job.resend();
        }

        self.inner.live.send_modify(|n| *n -= 1);
    }
}

// The mailbox drops after serve returns: a failed turn has rolled back
// before a new instance of the key starts, and the file is closed before
// shutdown counts the instance as stopped.
pub(super) async fn run<A: Actor>(mut mailbox: Mailbox<A>) {
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
                Job::Alarm(_, done) => done.send(Response::Done(Err(e))).ok(),
            };

            return;
        }
    };

    let mut actor = (mailbox.inner.make)(&mailbox.key);
    serve_turns(mailbox, &mut actor, &mut ctx).await;

    tokio::task::spawn_blocking(move || drop(ctx)).await.ok();
}

async fn serve_turns<A: Actor>(mailbox: &mut Mailbox<A>, actor: &mut A, ctx: &mut Ctx) {
    let inner = &mailbox.inner;
    let control = &mailbox.control;

    loop {
        let job = match tokio::time::timeout(inner.options.idle, mailbox.rx.recv()).await {
            Ok(Some(job)) => job,
            // A shutdown removed the entry, and the mailbox is empty.
            Ok(None) => return,
            Err(_) if mailbox.retire() => return,
            Err(_) => continue,
        };

        // The sender gets Error::Closed when it sends the job again.
        if control.stopped() {
            job.resend();

            return;
        }

        control.busy.store(true, SeqCst);

        let failed = match job {
            Job::Call(msg, reply) => {
                let result = call_turn(actor, ctx, control, msg).await;
                let failed = result.is_err();

                if let Some(result) = inner.reply(result) {
                    reply.send(Response::Done(result)).ok();
                }

                failed
            }
            Job::Alarm(on_failure, done) => {
                let result = alarm_turn(actor, ctx, control, on_failure).await;
                let failed = result.is_err();

                if let Some(result) = inner.reply(result) {
                    done.send(Response::Done(result)).ok();
                }

                failed
            }
        };

        if failed {
            return;
        }

        control.busy.store(false, SeqCst);
    }
}

async fn call_turn<A: Actor>(
    actor: &mut A,
    ctx: &mut Ctx,
    control: &Control,
    msg: A::Msg,
) -> Result<A::Reply, TurnError> {
    let value = control.run(actor.handle(ctx, msg)).await?.map_err(began)?;
    control.commit(ctx).await?;

    Ok(value)
}

// The turn began and did not commit, so an error that says otherwise comes
// from a call of the actor to another runtime.
fn began(e: Error) -> Error {
    match e {
        Error::Closed | Error::Overloaded | Error::Commit(_) => Error::Actor(Box::new(e)),
        e => e,
    }
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
