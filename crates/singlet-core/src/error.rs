/// An error of the runtime or of an instance.
///
/// An error from [`Runtime::call`](crate::Runtime::call) means that the turn
/// did not commit.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A SQLite operation failed.
    #[error(transparent)]
    Storage(#[from] rusqlite::Error),

    /// A file operation on the data directory failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Another runtime holds the lock of the data directory.
    #[error("data directory is in use by another runtime")]
    Locked,

    /// A storage file has a format version that this crate does not read.
    #[error("storage format {0} is not supported")]
    Format(i64),

    /// The mailbox of the key is full. The call did not run.
    #[error("too many calls wait for this key")]
    Overloaded,

    /// The runtime is shut down. The call did not run.
    #[error("runtime is shut down")]
    Closed,

    /// The instance panicked during the turn. The turn did not commit.
    #[error("instance stopped before it replied")]
    Stopped,

    /// From [`Actor::handle`](crate::Actor::handle) or
    /// [`Actor::alarm`](crate::Actor::alarm). The turn did not commit. The
    /// caller gets the error of the actor back with `downcast_ref`.
    #[error(transparent)]
    Actor(Box<dyn std::error::Error + Send + Sync>),
}
