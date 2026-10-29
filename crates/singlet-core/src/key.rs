use std::hash::Hash;

/// The identity of an instance.
pub trait Key: Eq + Hash + Clone + Send + Sync + 'static {
    /// Names the storage file of the key. Two different keys must give
    /// different bytes, and a key must give the same bytes in every version.
    fn encode(&self) -> Vec<u8>;

    /// The inverse of `encode`. The runtime uses it to wake the key of an
    /// alarm after a restart.
    fn decode(bytes: &[u8]) -> Option<Self>;
}

impl Key for String {
    fn encode(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        String::from_utf8(bytes.to_vec()).ok()
    }
}

impl Key for u64 {
    fn encode(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        Some(u64::from_be_bytes(bytes.try_into().ok()?))
    }
}
