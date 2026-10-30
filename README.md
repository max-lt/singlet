# singlet

One instance per key, with its own SQLite storage.

All calls for a key go to one instance. The instance runs one call at a time,
in order. It keeps its state in memory between calls and in a SQLite file on
disk. Code for one key reads like code for one server.

## Example

```rust,no_run
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let rt = Runtime::open("data", Options::default(), |_| Counter)?;
    let n = rt.call("room-1", ()).await?;
    println!("room-1: {n}");

    Ok(())
}
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

### Instances

- A runtime starts the instance of a key on the first call. It stops the
  instance after `Options::idle` (60 s) with no calls.
- `Key::encode` names the storage file of a key. It must give different
  bytes for different keys, and the same bytes in every version.
- `Msg` and `Reply` move in memory; there is no serialization.
- `Options::max_live` (1024) instances run at once. To start one more, a
  call stops an idle instance and waits for its slot. If every instance has
  work, the call fails at once with `Error::Overloaded`.
- `Runtime::open` must run inside a Tokio runtime.
- One runtime owns its data directory. A second runtime on the same directory
  fails with `Error::Locked`, also from another process.

### Turns

- One call to `handle` is a turn. Calls for one key do not interleave, also
  across an `await`. Calls for different keys run in parallel.
- The writes of a turn commit when `handle` or `alarm` returns `Ok`, before
  the caller gets the reply.
- A turn that returns `Err` discards its writes and the instance. The next
  call starts a new instance from storage.
- A panic stops the instance. Calls that wait in the mailbox go to the new
  instance.
- Above `Options::mailbox` (1024) waiting calls for one key, a call fails at
  once with `Error::Overloaded`.

### Errors

An error from `call` means that the turn did not commit, except
`Error::Commit`.

| Error                     | The turn             | Cause                                                      |
| ------------------------- | -------------------- | ---------------------------------------------------------- |
| `Actor`                   | began, not committed | `handle` returned `Err`                                    |
| `Storage`, `Io`, `Format` | not committed        | a SQLite or file operation failed                          |
| `Overloaded`              | did not begin        | the mailbox of the key is full, or every instance has work |
| `Closed`                  | did not begin        | the runtime is shut down                                   |
| `Stopped`                 | began, not committed | a panic, or an abort by `shutdown`                         |
| `Commit`                  | may have committed   | the COMMIT failed, for example on fsync                    |

Send the call again after an error, except `Commit`. After `Commit`, send it
again only if it is safe to apply it two times.

When `handle` returns `Closed`, `Overloaded` or `Commit`, for example from a
call to another runtime, the caller gets it in `Error::Actor`.

### Alarms

- `ctx.set_alarm(at)` sets the one alarm of an instance. At `at`, the
  runtime runs `Actor::alarm` as a turn, also after a restart and also if
  the instance stopped. The alarm is cleared before that turn.
- A failed alarm turn keeps the alarm. The runtime tries again after
  `Options::alarm_backoff` (1 s), then doubles the wait up to 60 s. It tries
  again `Options::alarm_retries` (6) times at most.
- The last try deletes the alarm before it runs, so its failure drops the
  alarm.
- An alarm can run more than once.

### Shutdown

- `Runtime::shutdown(deadline)` runs the calls that wait until `deadline`.
  New calls fail with `Error::Closed`.
- After `deadline`, it aborts each turn in `handle` or `alarm`. That turn
  does not commit, and its caller gets `Error::Stopped`. A turn in its
  commit is not aborted. A call that did not begin gets `Error::Closed`.
- An aborted alarm stays set, and the abort does not count as a try.
- The abort takes effect at the next `await` of a turn. A turn with no
  `await` cannot be stopped.
- After the abort, shutdown waits 1 s more. It returns `Error::Deadline` if
  it aborted a turn or if an instance still runs.
- Shutdown releases the lock only when no instance runs. If the runtime
  drops with no shutdown, the lock stays until the last instance stops.

## Storage

Each key has the file `<dir>/objects/<sha256 of key>.sqlite`, in WAL mode with
`synchronous=FULL`. A commit waits for fsync. A committed write survives a
crash of the process. It does not survive the loss of the disk.

Each file keeps its format version in `PRAGMA user_version`. A file with
another version, or with tables and no version, fails to open with
`Error::Format`.

The alarm of an object is in its own file. `<dir>/alarms.sqlite` is an index
of the alarms for the scheduler. An entry is never later than the alarm in
the file, so a crash between the two writes wakes an object early. The object
then checks its file and does nothing.

If a read of the index fails, the scheduler logs one WARN through `tracing`
and reads the index again after 1 s, until a read succeeds.

## Limits

- One node. There is no replication.
- An `await` in `handle` blocks all calls for its key.
- A call from `handle` to its own key waits for ever, as the instance runs
  one turn at a time. A cycle of calls between keys waits for ever too. The
  deadline of `Runtime::shutdown` aborts these turns.
- Each running instance holds 3 file descriptors: its file, the WAL and the
  shared memory. Set `ulimit -n` above 3 times `Options::max_live`, or a
  call that starts an instance fails with `Error::Storage`.

## Fuzzing

The `runtime` target checks each reply against a model, with calls, bursts,
alarms, restarts, aborts and pauses near the idle time. An abort hangs a call
or an alarm turn, and checks that a shutdown aborts it and discards its writes.
At the end, it reopens the directory and checks the stored counts and which
keys fired an alarm.

```sh
just fuzz       # 300 s
just fuzz 3600
```

## License

MIT
