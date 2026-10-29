use std::time::Duration;

/// The settings of a [`Runtime`](crate::Runtime).
#[derive(Clone, Debug)]
pub struct Options {
    /// An instance stops after this time without calls.
    pub idle: Duration,

    /// The calls that can wait for one key. Above that, a call fails at once
    /// with [`Error::Overloaded`](crate::Error::Overloaded). At least 1.
    pub mailbox: usize,

    /// The wait before a failed alarm turn runs again. It doubles after each
    /// failure, up to 60 s.
    pub alarm_backoff: Duration,

    /// The tries of an alarm turn after its first failure. The last try
    /// deletes the alarm before it runs, so its failure drops the alarm.
    pub alarm_retries: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(60),
            mailbox: 1024,
            alarm_backoff: Duration::from_secs(1),
            alarm_retries: 6,
        }
    }
}
