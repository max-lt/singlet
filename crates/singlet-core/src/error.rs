#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] rusqlite::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("data directory is in use by another runtime")]
    Locked,

    #[error("instance stopped before it replied")]
    Stopped,

    #[error("{0}")]
    Actor(String),
}
