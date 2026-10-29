//! Many buyers, one stock per product: no overselling, no lock.

use std::time::Duration;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Runtime;

enum Command {
    Restock,
    Buy,
}

#[derive(PartialEq)]
enum Outcome {
    Restocked,
    Bought,
    SoldOut,
}

struct Stock;

impl Actor for Stock {
    type Key = u64;
    type Msg = Command;
    type Reply = Outcome;

    async fn handle(&mut self, ctx: &mut Ctx, command: Command) -> Result<Outcome, Error> {
        let left = match ctx.get("left")? {
            Some(v) => u32::from_le_bytes(v.try_into().unwrap()),
            None => 0,
        };

        let outcome = match command {
            Command::Restock => {
                ctx.put("left", &(left + 10).to_le_bytes())?;
                Outcome::Restocked
            }
            Command::Buy if left > 0 => {
                ctx.put("left", &(left - 1).to_le_bytes())?;
                Outcome::Bought
            }
            Command::Buy => Outcome::SoldOut,
        };

        Ok(outcome)
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let dir = tempfile::tempdir()?;
    let rt = Runtime::open(dir.path(), Duration::from_secs(60), |_| Stock)?;

    rt.call(&42, Command::Restock).await?;

    let buyers: Vec<_> = (0..50)
        .map(|_| {
            let rt = rt.clone();
            tokio::spawn(async move { rt.call(&42, Command::Buy).await })
        })
        .collect();

    let mut bought = 0;

    for buyer in buyers {
        if buyer.await.unwrap()? == Outcome::Bought {
            bought += 1;
        }
    }

    println!("50 buyers, 10 items: {bought} bought");

    Ok(())
}
