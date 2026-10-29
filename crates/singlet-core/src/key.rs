use std::hash::Hash;

/// The identity of an instance.
pub trait Key: Eq + Hash + Clone + Send + Sync + 'static {
    /// Names the storage file of the key. Two different keys must give
    /// different bytes, and a key must give the same bytes in every version.
    fn encode(&self) -> Vec<u8>;
}

impl Key for String {
    fn encode(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Key for u64 {
    fn encode(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}
