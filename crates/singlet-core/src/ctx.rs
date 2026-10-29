use std::path::Path;

use rusqlite::Connection;
use rusqlite::OptionalExtension;
use sha2::Digest;
use sha2::Sha256;

use crate::Error;

/// The storage of one instance.
///
/// Reads see the writes of the current turn. The runtime commits or discards
/// these writes at the end of the turn.
pub struct Ctx {
    conn: Connection,
    writing: bool,
}

impl Ctx {
    pub(crate) fn open(dir: &Path, key: &[u8]) -> Result<Self, Error> {
        let name: String = Sha256::digest(key)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();

        let conn = Connection::open(dir.join(format!("{name}.sqlite")))?;
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID",
        )?;

        Ok(Self {
            conn,
            writing: false,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let value = self
            .conn
            .prepare_cached("SELECT value FROM kv WHERE key = ?1")?
            .query_row([key], |row| row.get(0))
            .optional()?;

        Ok(value)
    }

    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.begin()?;
        self.conn
            .prepare_cached("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")?
            .execute((key, value))?;

        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.begin()?;
        self.conn
            .prepare_cached("DELETE FROM kv WHERE key = ?1")?
            .execute([key])?;

        Ok(())
    }

    fn begin(&mut self) -> Result<(), Error> {
        if self.writing {
            return Ok(());
        }

        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        self.writing = true;

        Ok(())
    }

    /// Waits for fsync on the calling thread.
    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        if !self.writing {
            return Ok(());
        }

        self.writing = false;
        self.conn.execute_batch("COMMIT")?;

        Ok(())
    }
}
