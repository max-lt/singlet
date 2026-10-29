#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] rusqlite::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("data directory is in use by another runtime")]
    Locked,

    /// The mailbox of the key is full. The call did not run.
    #[error("too many calls wait for this key")]
    Overloaded,

    /// The instance panicked during the call. The turn did not commit.
    #[error("instance stopped before it replied")]
    Stopped,

    /// From [`Actor::handle`](crate::Actor::handle). The turn did not commit.
    /// The caller gets the error of the actor back with `downcast_ref`.
    #[error("{0}")]
    Actor(Box<dyn std::error::Error + Send + Sync>),
}
