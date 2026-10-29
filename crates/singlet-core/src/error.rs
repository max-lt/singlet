/// An error of the runtime or of an instance.
///
/// An error from [`Runtime::call`](crate::Runtime::call) means that the turn
/// did not commit, except [`Error::Commit`].
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

    /// The commit of the turn failed, and the turn may have committed: after
    /// a failed fsync, its writes can be on disk. Send the call again only if
    /// it is safe to apply it two times.
    #[error("commit failed, and the turn may have committed")]
    Commit(#[source] rusqlite::Error),

    /// The mailbox of the key is full. The call did not run.
    #[error("too many calls wait for this key")]
    Overloaded,

    /// The call did not begin, because the runtime is shut down.
    #[error("runtime is shut down")]
    Closed,

    /// The call began and did not commit. The instance panicked, or a
    /// shutdown aborted the turn.
    #[error("instance stopped before it replied")]
    Stopped,

    /// From [`Runtime::shutdown`](crate::Runtime::shutdown) after its
    /// deadline.
    #[error("shutdown deadline passed: aborted turns {aborted}, running instances {running}")]
    Deadline {
        /// The turns that the shutdown aborted. They did not commit.
        aborted: usize,
        /// The instances that still run. While this is above 0, the runtime
        /// keeps the lock of its data directory.
        running: usize,
    },

    /// From [`Actor::handle`](crate::Actor::handle) or
    /// [`Actor::alarm`](crate::Actor::alarm). The turn did not commit. The
    /// caller gets the error of the actor back with `downcast_ref`.
    #[error(transparent)]
    Actor(Box<dyn std::error::Error + Send + Sync>),
}
