use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use rusqlite::Connection;
use rusqlite::OptionalExtension;
use sha2::Digest;
use sha2::Sha256;

use crate::Error;
use crate::alarm;
use crate::alarm::Index;
use crate::db;

/// The storage of one instance.
///
/// Reads see the writes of the current turn. The runtime commits or discards
/// these writes at the end of the turn.
pub struct Ctx {
    // None only while a commit runs on a blocking thread.
    conn: Option<Connection>,
    key: Vec<u8>,
    index: Arc<Index>,
    writing: bool,
    alarm: Option<Option<i64>>,
}

impl Ctx {
    pub(crate) fn open(dir: &Path, key: Vec<u8>, index: Arc<Index>) -> Result<Self, Error> {
        let name: String = Sha256::digest(&key)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();

        let conn = db::open(
            &dir.join(format!("{name}.sqlite")),
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS alarm (id INTEGER PRIMARY KEY CHECK (id = 0), at INTEGER NOT NULL);",
        )?;

        Ok(Self {
            conn: Some(conn),
            key,
            index,
            writing: false,
            alarm: None,
        })
    }

    fn conn(&self) -> &Connection {
        self.conn.as_ref().expect("commit puts the connection back")
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let value = self
            .conn()
            .prepare_cached("SELECT value FROM kv WHERE key = ?1")?
            .query_row([key], |row| row.get(0))
            .optional()?;

        Ok(value)
    }

    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.begin()?;
        self.conn()
            .prepare_cached("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")?
            .execute((key, value))?;

        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.begin()?;
        self.conn()
            .prepare_cached("DELETE FROM kv WHERE key = ?1")?
            .execute([key])?;

        Ok(())
    }

    pub fn alarm(&self) -> Result<Option<SystemTime>, Error> {
        Ok(self.alarm_ms()?.map(alarm::from_ms))
    }

    /// Replaces the alarm. The runtime calls [`Actor::alarm`] at `at`, also
    /// after a restart.
    ///
    /// [`Actor::alarm`]: crate::Actor::alarm
    pub fn set_alarm(&mut self, at: SystemTime) -> Result<(), Error> {
        let at = alarm::to_ms(at);

        self.begin()?;
        self.conn()
            .prepare_cached("INSERT OR REPLACE INTO alarm (id, at) VALUES (0, ?1)")?
            .execute([at])?;
        self.alarm = Some(Some(at));

        Ok(())
    }

    pub fn delete_alarm(&mut self) -> Result<(), Error> {
        self.begin()?;
        self.conn()
            .prepare_cached("DELETE FROM alarm")?
            .execute([])?;
        self.alarm = Some(None);

        Ok(())
    }

    pub(crate) fn alarm_ms(&self) -> Result<Option<i64>, Error> {
        let at = self
            .conn()
            .prepare_cached("SELECT at FROM alarm WHERE id = 0")?
            .query_row([], |row| row.get(0))
            .optional()?;

        Ok(at)
    }

    /// Makes the index entry of this object equal to its alarm.
    pub(crate) fn sync_index(&self) -> Result<(), Error> {
        self.index.set(&self.key, self.alarm_ms()?)
    }

    fn begin(&mut self) -> Result<(), Error> {
        if self.writing {
            return Ok(());
        }

        self.conn().execute_batch("BEGIN IMMEDIATE")?;
        self.writing = true;

        Ok(())
    }

    /// Runs on a blocking thread, as it waits for fsync.
    pub(crate) async fn commit(&mut self) -> Result<(), Error> {
        if !self.writing {
            return Ok(());
        }

        self.writing = false;

        let conn = self.conn.take().expect("commit puts the connection back");
        let index = self.index.clone();
        let key = self.key.clone();
        let alarm = self.alarm.take();

        let (conn, result) = tokio::task::spawn_blocking(move || {
            let result = commit(&conn, &index, &key, alarm);

            (conn, result)
        })
        .await
        .expect("commit does not panic");

        self.conn = Some(conn);

        result
    }
}

fn commit(
    conn: &Connection,
    index: &Index,
    key: &[u8],
    alarm: Option<Option<i64>>,
) -> Result<(), Error> {
    if let Some(Some(at)) = alarm {
        index.lower(key, at)?;
    }

    conn.execute_batch("COMMIT")?;

    // On failure the entry stays early or stale, never late, and the next
    // wake corrects it.
    if let Some(at) = alarm {
        index.set(key, at).ok();
    }

    Ok(())
}
