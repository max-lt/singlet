#[cfg(test)]
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

use tokio::sync::Notify;

use crate::Ctx;
use crate::Error;

pub(super) enum TurnError {
    Aborted,
    Failed(Error),
}

impl From<Error> for TurnError {
    fn from(e: Error) -> Self {
        Self::Failed(e)
    }
}

/// Lets a shutdown abort the turn of an instance, but not its commit.
pub(super) struct Control {
    pub(super) stop: AtomicBool,
    // Holds its permit, so a turn that starts after the stop is aborted.
    pub(super) cancel: Notify,
    #[cfg(test)]
    pub(super) gate: Option<Arc<Gate>>,
}

impl Control {
    pub(super) fn stopped(&self) -> bool {
        self.stop.load(SeqCst)
    }

    pub(super) fn stop(&self) {
        self.stop.store(true, SeqCst);
        self.cancel.notify_one();
    }

    /// Runs the code of the actor until it completes or a shutdown aborts
    /// it. The abort drops the future at its next `await`.
    pub(super) async fn run<F: Future>(&self, code: F) -> Result<F::Output, TurnError> {
        tokio::select! {
            output = code => Ok(output),
            () = self.cancel.notified() => Err(TurnError::Aborted),
        }
    }

    pub(super) async fn commit(&self, ctx: &mut Ctx) -> Result<(), TurnError> {
        #[cfg(test)]
        self.pass(Point::BeforeCommit).await;

        // A stop after this read waits for the commit; the commit is not in
        // run, so the cancel cannot reach it.
        if self.stopped() {
            return Err(TurnError::Aborted);
        }

        #[cfg(test)]
        self.pass(Point::InCommit).await;

        Ok(ctx.commit().await?)
    }

    #[cfg(test)]
    async fn pass(&self, at: Point) {
        if let Some(gate) = self.gate.as_ref().filter(|gate| gate.at == at) {
            gate.reached.notify_one();
            gate.open.notified().await;
        }
    }
}

#[cfg(test)]
#[derive(PartialEq)]
pub(super) enum Point {
    BeforeCommit,
    InCommit,
}

/// Holds an instance at a point of its commit, for the tests.
#[cfg(test)]
pub(super) struct Gate {
    pub(super) at: Point,
    pub(super) reached: Notify,
    pub(super) open: Notify,
}
