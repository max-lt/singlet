use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rusqlite::Connection;
use tokio::sync::Notify;

use crate::Error;
use crate::db;

/// The alarms of a data directory that are due or will be due.
///
/// The alarm of an object lives in its own file. An entry here is never
/// later than that alarm, so a crash between the two writes wakes the object
/// early, and the object checks its file. It never wakes it late.
pub(crate) struct Index {
    conn: Mutex<Connection>,
    delivering: Mutex<HashSet<Vec<u8>>>,
    pub(crate) wake: Notify,
}

impl Index {
    pub(crate) fn open(path: &Path) -> Result<Self, Error> {
        let conn = db::open(
            path,
            "CREATE TABLE alarms (key BLOB PRIMARY KEY, at INTEGER NOT NULL) WITHOUT ROWID;
             CREATE INDEX alarms_at ON alarms (at);",
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
            delivering: Mutex::default(),
            wake: Notify::new(),
        })
    }

    /// Moves the entry of `key` to `at` if `at` is earlier.
    pub(crate) fn lower(&self, key: &[u8], at: i64) -> Result<(), Error> {
        self.conn
            .lock()
            .unwrap()
            .prepare_cached(
                "INSERT INTO alarms (key, at) VALUES (?1, ?2)
                 ON CONFLICT (key) DO UPDATE SET at = MIN(at, excluded.at)",
            )?
            .execute((key, at))?;

        self.wake.notify_one();

        Ok(())
    }

    pub(crate) fn set(&self, key: &[u8], at: Option<i64>) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();

        let Some(at) = at else {
            conn.prepare_cached("DELETE FROM alarms WHERE key = ?1")?
                .execute([key])?;

            return Ok(());
        };

        conn.prepare_cached("INSERT OR REPLACE INTO alarms (key, at) VALUES (?1, ?2)")?
            .execute((key, at))?;
        drop(conn);

        self.wake.notify_one();

        Ok(())
    }

    /// Returns the keys due at `now` that no task delivers, and marks them
    /// as delivering.
    pub(crate) fn take_due(&self, now: i64) -> Result<Vec<Vec<u8>>, Error> {
        let due: Vec<Vec<u8>> = self
            .conn
            .lock()
            .unwrap()
            .prepare_cached("SELECT key FROM alarms WHERE at <= ?1")?
            .query_map([now], |row| row.get(0))?
            .collect::<Result<_, _>>()?;

        let mut delivering = self.delivering.lock().unwrap();

        Ok(due
            .into_iter()
            .filter(|key| delivering.insert(key.clone()))
            .collect())
    }

    pub(crate) fn next_after(&self, now: i64) -> Result<Option<i64>, Error> {
        let next = self
            .conn
            .lock()
            .unwrap()
            .prepare_cached("SELECT MIN(at) FROM alarms WHERE at > ?1")?
            .query_row([now], |row| row.get(0))?;

        Ok(next)
    }

    pub(crate) fn delivered(&self, key: &[u8]) {
        self.delivering.lock().unwrap().remove(key);
        self.wake.notify_one();
    }
}

pub(crate) fn to_ms(at: SystemTime) -> i64 {
    let ms = at
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    i64::try_from(ms).unwrap_or(i64::MAX)
}

pub(crate) fn from_ms(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms.max(0).cast_unsigned())
}

pub(crate) fn now_ms() -> i64 {
    to_ms(SystemTime::now())
}
