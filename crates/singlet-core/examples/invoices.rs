//! Invoice numbers per company: strictly sequential, no gap, no lock.
//!
//! A failed turn discards its writes, so a number is used only when the
//! invoice is accepted.

use std::time::Duration;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Runtime;

struct Ledger;

impl Actor for Ledger {
    async fn handle(&mut self, ctx: &mut Ctx, msg: Vec<u8>) -> Result<Vec<u8>, Error> {
        let next = match ctx.get("next")? {
            Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
            None => 1,
        };

        let number = format!("{}-{next:06}", ctx.key());
        ctx.put("next", &(next + 1).to_le_bytes())?;
        ctx.put(&number, &msg)?;

        if msg.is_empty() {
            return Err(Error::Actor("empty invoice".into()));
        }

        Ok(number.into_bytes())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let dir = tempfile::tempdir()?;
    let rt = Runtime::open(dir.path(), Duration::from_secs(60), |_| Ledger)?;

    let invoices = [&b"120 EUR"[..], b"", b"80 EUR", b"45 EUR"];

    for body in invoices {
        match rt.call("ACME", body.to_vec()).await {
            Ok(number) => println!("issued {}", String::from_utf8(number).unwrap()),
            Err(e) => println!("rejected: {e}"),
        }
    }

    Ok(())
}
