use std::time::Duration;

/// The settings of a [`Runtime`](crate::Runtime).
#[derive(Clone, Debug)]
pub struct Options {
    /// An instance stops after this time without calls.
    pub idle: Duration,

    /// The calls that can wait for one key. Above that, a call fails at once
    /// with [`Error::Overloaded`](crate::Error::Overloaded). At least 1.
    pub mailbox: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(60),
            mailbox: 1024,
        }
    }
}
