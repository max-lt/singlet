//! One instance per key, with its own SQLite storage.
//!
//! A [`Runtime`] routes each call to the instance of its key. It starts the
//! instance on the first call and stops it after a time without calls. Calls
//! for one key run one at a time, in order.

mod alarm;
mod ctx;
mod error;
mod key;
mod options;
mod runtime;

pub use ctx::Ctx;
pub use error::Error;
pub use key::Key;
pub use options::Options;
pub use runtime::Runtime;

/// The code of one instance.
///
/// A turn is one call to `handle` or `alarm`. The writes of a turn commit
/// when it returns `Ok`, before the caller gets the reply. A turn that
/// returns `Err` discards its writes and the instance, so memory and storage
/// stay in step.
pub trait Actor: Send + 'static {
    type Key: Key;
    type Msg: Send + 'static;
    type Reply: Send + 'static;

    fn handle(
        &mut self,
        ctx: &mut Ctx,
        msg: Self::Msg,
    ) -> impl Future<Output = Result<Self::Reply, Error>> + Send;

    /// Runs when the alarm set with [`Ctx::set_alarm`] is due. The alarm is
    /// cleared before this turn, so the turn can set the next one. A turn
    /// that fails keeps the alarm, and the runtime tries again with backoff.
    fn alarm(&mut self, ctx: &mut Ctx) -> impl Future<Output = Result<(), Error>> + Send {
        let _ = ctx;

        async { Ok(()) }
    }
}
