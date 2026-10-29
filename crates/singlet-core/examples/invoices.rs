//! Invoice numbers per company: strictly sequential, no gap, no lock.
//!
//! A failed turn discards its writes, so a number is used only when the
//! invoice is accepted.

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Options;
use singlet_core::Runtime;

struct Ledger {
    company: String,
}

impl Actor for Ledger {
    type Key = String;
    type Msg = String;
    type Reply = String;

    async fn handle(&mut self, ctx: &mut Ctx, body: String) -> Result<String, Error> {
        let next = match ctx.get("next")? {
            Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
            None => 1,
        };

        let number = format!("{}-{next:06}", self.company);
        ctx.put("next", &(next + 1).to_le_bytes())?;
        ctx.put(&number, body.as_bytes())?;

        if body.is_empty() {
            return Err(Error::Actor("empty invoice".into()));
        }

        Ok(number)
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let dir = tempfile::tempdir()?;
    let rt = Runtime::open(dir.path(), Options::default(), |company: &String| Ledger {
        company: company.clone(),
    })?;

    let invoices = ["120 EUR", "", "80 EUR", "45 EUR"];

    for body in invoices {
        match rt.call("ACME", body.to_owned()).await {
            Ok(number) => println!("issued {number}"),
            Err(e) => println!("rejected: {e}"),
        }
    }

    Ok(())
}
