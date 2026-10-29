# singlet

One instance per key, with its own SQLite storage.

All calls for a key go to one instance. The instance runs one call at a time,
in order. It keeps its state in memory between calls and in a SQLite file on
disk. Code for one key reads like code for one server.

## Example

```rust
use std::time::Duration;

use singlet_core::{Actor, Ctx, Error, Runtime};

#[derive(Default)]
struct Counter;

impl Actor for Counter {
    async fn handle(&mut self, ctx: &mut Ctx, _msg: Vec<u8>) -> Result<Vec<u8>, Error> {
        let n = match ctx.get("n")? {
            Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
            None => 0,
        };

        ctx.put("n", &(n + 1).to_le_bytes())?;

        Ok((n + 1).to_le_bytes().to_vec())
    }
}

let rt = Runtime::open("data", Duration::from_secs(60), |_key| Counter)?;
let reply = rt.call("room-1", vec![]).await?;
```

## Examples

- `stock`: 50 buyers, 10 items, no overselling.
- `invoices`: invoice numbers with no gap; a rejected invoice uses no number.
- `rate_limit`: a token bucket per API key, in memory only.

```sh
cargo run --example stock
```

## Rules

- A runtime starts the instance of a key on the first call. It stops the
  instance after the idle time with no calls.
- One call to `handle` is a turn. Calls for one key do not interleave, also
  across an `await`. Calls for different keys run in parallel.
- The writes of a turn commit when `handle` returns `Ok`, before the caller
  gets the reply.
- A turn that returns `Err` discards its writes and the instance. The next
  call starts a new instance from storage.
- A panic stops the instance. Its caller gets `Error::Stopped`. Calls that
  wait in the mailbox go to the new instance.
- One runtime owns its data directory. A second runtime on the same directory
  fails with `Error::Locked`, also from another process.

## Storage

Each key has the file `<dir>/objects/<sha256 of key>.sqlite`, in WAL mode with
`synchronous=FULL`. A commit waits for fsync. A committed write survives a
crash of the process. It does not survive the loss of the disk.

## Limits

- One node. There is no replication.
- A commit runs on the async thread of the instance.
- An `await` in `handle` blocks all calls for its key.

## License

MIT
