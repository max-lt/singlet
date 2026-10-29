use std::path::Path;

use rusqlite::Connection;

use crate::Error;

/// The layout of the tables. A file with another version does not open.
const FORMAT: i64 = 1;

/// Opens a SQLite file in WAL mode with `synchronous=FULL`, and creates its
/// tables with `schema` on the first open.
pub(crate) fn open(path: &Path, schema: &str) -> Result<Connection, Error> {
    let conn = Connection::open(path)?;
    conn.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;
    conn.pragma_update(None, "synchronous", "FULL")?;

    match conn.pragma_query_value(None, "user_version", |row| row.get(0))? {
        FORMAT => {}
        0 => conn.execute_batch(&format!(
            "BEGIN; {schema} PRAGMA user_version = {FORMAT}; COMMIT;"
        ))?,
        other => return Err(Error::Format(other)),
    }

    Ok(conn)
}
