//! A token bucket per API key, in memory only.
//!
//! The bucket is lost when the instance stops. That is correct for a rate
//! limit, and it costs no write per call.

use std::time::Duration;
use std::time::Instant;

use singlet_core::Actor;
use singlet_core::Ctx;
use singlet_core::Error;
use singlet_core::Runtime;

const CAPACITY: f64 = 5.0;
const REFILL_PER_SEC: f64 = 1.0;

struct Bucket {
    tokens: f64,
    at: Instant,
}

impl Actor for Bucket {
    async fn handle(&mut self, _ctx: &mut Ctx, _msg: Vec<u8>) -> Result<Vec<u8>, Error> {
        let now = Instant::now();
        let refill = now.duration_since(self.at).as_secs_f64() * REFILL_PER_SEC;
        self.tokens = (self.tokens + refill).min(CAPACITY);
        self.at = now;

        if self.tokens < 1.0 {
            return Ok(b"429".to_vec());
        }

        self.tokens -= 1.0;

        Ok(b"200".to_vec())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let dir = tempfile::tempdir()?;
    let rt = Runtime::open(dir.path(), Duration::from_secs(60), |_| Bucket {
        tokens: CAPACITY,
        at: Instant::now(),
    })?;

    for key in ["key-a", "key-b"] {
        let mut codes = Vec::new();

        for _ in 0..8 {
            codes.push(String::from_utf8(rt.call(key, vec![]).await?).unwrap());
        }

        println!("{key}: {}", codes.join(" "));
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    let code = String::from_utf8(rt.call("key-a", vec![]).await?).unwrap();
    println!("key-a after 2 s: {code}");

    Ok(())
}
