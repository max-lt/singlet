# singlet

One instance per key, with its own SQLite storage.

All calls for a key go to one instance. The instance runs one call at a time,
in order. It keeps its state in memory between calls and in a SQLite file on
disk. Code for one key reads like code for one server.

## Example

```rust
use std::time::Duration;

use singlet_core::{Actor, Ctx, Error, Options, Runtime};

struct Counter;

impl Actor for Counter {
    type Key = String;
    type Msg = ();
    type Reply = u64;

    async fn handle(&mut self, ctx: &mut Ctx, _: ()) -> Result<u64, Error> {
        let n = match ctx.get("n")? {
            Some(v) => u64::from_le_bytes(v.try_into().unwrap()),
            None => 0,
        };

        ctx.put("n", &(n + 1).to_le_bytes())?;

        Ok(n + 1)
    }
}

let rt = Runtime::open("data", Options::default(), |_key| Counter)?;
let n = rt.call("room-1", ()).await?;
```

## Examples

- `stock`: 50 buyers, 10 items, no overselling.
- `invoices`: invoice numbers with no gap; a rejected invoice uses no number.
- `rate_limit`: a token bucket per API key, in memory only.
- `reminder`: an alarm per order that is not paid in time.

```sh
cargo run --example stock
```

## Rules

- A runtime starts the instance of a key on the first call. It stops the
  instance after the idle time with no calls.
- `Msg` and `Reply` move in memory; there is no serialization.
- `Key::encode` names the storage file of a key. It must give different
  bytes for different keys, and the same bytes in every version.
- One call to `handle` is a turn. Calls for one key do not interleave, also
  across an `await`. Calls for different keys run in parallel.
- The writes of a turn commit when `handle` returns `Ok`, before the caller
  gets the reply.
- A turn that returns `Err` discards its writes and the instance. The next
  call starts a new instance from storage.
- `ctx.set_alarm(at)` sets the one alarm of an instance. At `at`, the
  runtime runs `Actor::alarm` as a turn, also after a restart and also if
  the instance stopped. The alarm is cleared before that turn.
- A failed alarm turn keeps the alarm. The runtime tries again after 1 s,
  then doubles the wait up to 60 s. An alarm can run more than once.
- A panic stops the instance. Its caller gets `Error::Stopped`. Calls that
  wait in the mailbox go to the new instance.
- `Runtime::open` must run inside a Tokio runtime.
- One runtime owns its data directory. A second runtime on the same directory
  fails with `Error::Locked`, also from another process. The lock stays until
  the last instance stops.

## Storage

Each key has the file `<dir>/objects/<sha256 of key>.sqlite`, in WAL mode with
`synchronous=FULL`. A commit waits for fsync. A committed write survives a
crash of the process. It does not survive the loss of the disk.

The alarm of an object is in its own file. `<dir>/alarms.sqlite` is an index
of the alarms for the scheduler. An entry is never later than the alarm in
the file, so a crash between the two writes wakes an object early. The object
then checks its file and does nothing.

## Limits

- One node. There is no replication.
- An `await` in `handle` blocks all calls for its key.

## Fuzzing

The `runtime` target checks each reply against a model, with calls, bursts
and pauses near the idle time.

```sh
just fuzz       # 300 s
just fuzz 3600
```

## License

MIT
