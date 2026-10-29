//! An order that is not paid in time gets a reminder, also if the instance
//! stopped in between.

use std::time::Duration;
use std::time::SystemTime;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Runtime;

const PAY_WITHIN: Duration = Duration::from_millis(300);

enum Command {
    Place,
    Pay,
}

struct Order {
    id: String,
}

impl Actor for Order {
    type Key = String;
    type Msg = Command;
    type Reply = ();

    async fn handle(&mut self, ctx: &mut Ctx, command: Command) -> Result<(), Error> {
        match command {
            Command::Place => ctx.set_alarm(SystemTime::now() + PAY_WITHIN),
            Command::Pay => ctx.delete_alarm(),
        }
    }

    async fn alarm(&mut self, _ctx: &mut Ctx) -> Result<(), Error> {
        println!("{}: reminder sent", self.id);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let dir = tempfile::tempdir()?;
    let rt = Runtime::open(dir.path(), Duration::from_millis(100), |id: &String| {
        Order { id: id.clone() }
    })?;

    rt.call("order-1", Command::Place).await?;
    rt.call("order-2", Command::Place).await?;
    rt.call("order-2", Command::Pay).await?;

    tokio::time::sleep(PAY_WITHIN * 2).await;

    Ok(())
}
