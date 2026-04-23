/// Bloom filter: probabilistic set membership.
/// False positives possible, false negatives impossible.
/// Uses double-hashing to simulate k independent hash functions:
///   h_i(key) = (h1(key) + i * h2(key)) % m
pub struct BloomFilter {
    bits: Vec<u8>, // bit array packed into bytes
    m: usize,      // total number of bits
    k: usize,      // number of hash functions
}

impl BloomFilter {
    pub fn new(m: usize, k: usize) -> Self {
        BloomFilter {
            bits: vec![0u8; m.div_ceil(8)],
            m,
            k,
        }
    }

    pub fn insert(&mut self, key: &str) {
        let positions: Vec<usize> = self.positions(key).collect();
        for pos in positions {
            self.bits[pos / 8] |= 1 << (pos % 8);
        }
    }

    /// Returns false  → key definitely not present (no false negatives).
    /// Returns true   → key probably present (false positives possible).
    pub fn contains(&self, key: &str) -> bool {
        self.positions(key).all(|pos| self.bits[pos / 8] & (1 << (pos % 8)) != 0)
    }

    pub fn to_bytes(&self) -> &[u8] {
        &self.bits
    }

    pub fn from_bytes(bytes: &[u8], m: usize, k: usize) -> Self {
        BloomFilter {
            bits: bytes.to_vec(),
            m,
            k,
        }
    }

    fn positions(&self, key: &str) -> impl Iterator<Item = usize> {
        let h1 = fnv1a(key);
        let h2 = djb2(key) | 1; // ensure h2 is odd so it stays coprime with m
        let m = self.m;
        let k = self.k;
        (0..k).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % m)
    }
}

fn fnv1a(key: &str) -> usize {
    let mut hash: u64 = 14695981039346656037;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash as usize
}

fn djb2(key: &str) -> usize {
    let mut hash: u64 = 5381;
    for byte in key.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
    }
    hash as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_key_always_found() {
        let mut f = BloomFilter::new(1024, 3);
        f.insert("hello");
        f.insert("world");
        assert!(f.contains("hello"));
        assert!(f.contains("world"));
    }

    #[test]
    fn missing_key_usually_absent() {
        let mut f = BloomFilter::new(1024, 3);
        f.insert("hello");
        // "definitely_not_here" with high probability returns false
        // (could theoretically false-positive, but not with these specific strings)
        assert!(!f.contains("definitely_not_here"));
    }

    #[test]
    fn roundtrip_serialization() {
        let mut f = BloomFilter::new(1024, 3);
        f.insert("key1");
        f.insert("key2");
        let bytes = f.to_bytes().to_vec();
        let f2 = BloomFilter::from_bytes(&bytes, 1024, 3);
        assert!(f2.contains("key1"));
        assert!(f2.contains("key2"));
        assert!(!f2.contains("key3"));
    }
}
