//! Many buyers, one stock per product: no overselling, no lock.

use std::time::Duration;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Runtime;

#[derive(Clone, Copy)]
#[repr(u8)]
enum Command {
    Restock,
    Buy,
}

impl Command {
    fn decode(msg: &[u8]) -> Option<Self> {
        match msg {
            [0] => Some(Self::Restock),
            [1] => Some(Self::Buy),
            _ => None,
        }
    }

    fn encode(self) -> Vec<u8> {
        vec![self as u8]
    }
}

#[derive(PartialEq)]
#[repr(u8)]
enum Outcome {
    Restocked,
    Bought,
    SoldOut,
}

impl Outcome {
    fn decode(reply: &[u8]) -> Self {
        match reply {
            [0] => Self::Restocked,
            [1] => Self::Bought,
            [2] => Self::SoldOut,
            _ => unreachable!("Stock replies with an Outcome"),
        }
    }
}

struct Stock;

impl Actor for Stock {
    async fn handle(&mut self, ctx: &mut Ctx, msg: Vec<u8>) -> Result<Vec<u8>, Error> {
        let Some(command) = Command::decode(&msg) else {
            return Err(Error::Actor("unknown command".into()));
        };

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

        Ok(vec![outcome as u8])
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let dir = tempfile::tempdir()?;
    let rt = Runtime::open(dir.path(), Duration::from_secs(60), |_| Stock)?;

    rt.call("sku-42", Command::Restock.encode()).await?;

    let buyers: Vec<_> = (0..50)
        .map(|_| {
            let rt = rt.clone();
            tokio::spawn(async move { rt.call("sku-42", Command::Buy.encode()).await })
        })
        .collect();

    let mut bought = 0;

    for buyer in buyers {
        if Outcome::decode(&buyer.await.unwrap()?) == Outcome::Bought {
            bought += 1;
        }
    }

    println!("50 buyers, 10 items: {bought} bought");

    Ok(())
}
