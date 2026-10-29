use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use super::Inner;
use super::OnFailure;
use super::Runtime;
use crate::Actor;
use crate::Error;
use crate::Key;
use crate::Options;
use crate::alarm;
use crate::alarm::Index;

const MAX_BACKOFF: Duration = Duration::from_secs(60);
pub(super) const INDEX_RETRY: Duration = Duration::from_secs(1);

// Holds the runtime only while it reads the index, so a dropped runtime can
// go away while the scheduler waits.
pub(super) async fn schedule<A: Actor>(inner: Weak<Inner<A>>, index: Arc<Index>) {
    let mut failing = false;

    loop {
        let next = {
            let Some(strong) = inner.upgrade() else {
                return;
            };

            if strong.registry.lock().unwrap().closed {
                return;
            }

            spawn_due(&strong, &index)
        };

        if let Err(e) = &next
            && !failing
        {
            tracing::warn!("cannot read the alarm index, alarms wait: {e}");
        }

        failing = next.is_err();

        let wait = match next {
            Ok(Some(at)) => Duration::from_millis((at - alarm::now_ms()).max(0).cast_unsigned()),
            Ok(None) => {
                index.wake.notified().await;
                continue;
            }
            // The index is a hint, so a failed read only delays the alarms.
            Err(_) => INDEX_RETRY,
        };

        tokio::time::timeout(wait, index.wake.notified()).await.ok();
    }
}

/// Starts a delivery for each due alarm, and returns the next alarm.
fn spawn_due<A: Actor>(inner: &Arc<Inner<A>>, index: &Arc<Index>) -> Result<Option<i64>, Error> {
    let now = alarm::now_ms();

    for key in index.take_due(now)? {
        tokio::spawn(deliver(Arc::downgrade(inner), index.clone(), key));
    }

    index.next_after(now)
}

async fn deliver<A: Actor>(inner: Weak<Inner<A>>, index: Arc<Index>, key: Vec<u8>) {
    let mut failures = 0;

    loop {
        let Some(inner) = inner.upgrade() else {
            break;
        };

        let Some(decoded) = A::Key::decode(&key) else {
            index.set(&key, None).ok();
            break;
        };

        let Options {
            alarm_retries,
            alarm_backoff,
            ..
        } = inner.options;

        let on_failure = if failures < alarm_retries {
            OnFailure::Retry
        } else {
            OnFailure::Drop
        };

        let rt = Runtime { inner };
        let result = rt.alarm(&decoded, on_failure).await;

        // A shutdown is not a failure of the alarm.
        if result.is_ok()
            || on_failure == OnFailure::Drop
            || rt.inner.registry.lock().unwrap().closed
        {
            break;
        }

        let backoff = alarm_backoff.saturating_mul(1 << failures.min(16));
        tokio::time::sleep(backoff.min(MAX_BACKOFF)).await;
        failures += 1;
    }

    index.delivered(&key);
}
