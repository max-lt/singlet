//! One instance per key, with its own SQLite storage.
//!
//! A [`Runtime`] routes each call to the instance of its key. It starts the
//! instance on the first call and stops it after a time without calls. Calls
//! for one key run one at a time, in order.

mod ctx;
mod error;
mod runtime;

pub use ctx::Ctx;
pub use error::Error;
pub use runtime::Runtime;

/// The code of one instance.
///
/// A turn is one call to `handle`. The writes of a turn commit when it
/// returns `Ok`, before the caller gets the reply. A turn that returns `Err`
/// discards its writes and the instance, so memory and storage stay in step.
pub trait Actor: Send + 'static {
    fn handle(
        &mut self,
        ctx: &mut Ctx,
        msg: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send;
}
