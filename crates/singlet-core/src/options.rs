use std::time::Duration;

/// The settings of a [`Runtime`](crate::Runtime).
#[derive(Clone, Debug)]
pub struct Options {
    /// An instance stops after this time without calls.
    pub idle: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(60),
        }
    }
}
