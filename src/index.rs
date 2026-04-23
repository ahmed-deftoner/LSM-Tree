use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

/// Sparse index: maps sampled keys to their byte offsets in an SSTable file.
/// On lookup, binary-search for the largest indexed key ≤ target, seek there,
/// then scan forward — early-exit is safe because SSTable keys are sorted.
pub struct SparseIndex {
    entries: Vec<(String, u64)>, // (key, byte_offset), sorted ascending by key
}

impl SparseIndex {
    pub fn new() -> Self {
        SparseIndex { entries: Vec::new() }
    }

    pub fn add(&mut self, key: String, offset: u64) {
        self.entries.push((key, offset));
    }

    /// Return the byte offset to seek to before scanning for `key`.
    /// Finds the largest indexed key ≤ target. Returns 0 if none qualify
    /// (key is before all indexed keys → scan from file start).
    pub fn find_offset(&self, key: &str) -> u64 {
        match self.entries.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
            Ok(i) => self.entries[i].1,       // exact match on an indexed key
            Err(0) => 0,                       // key < all indexed keys
            Err(i) => self.entries[i - 1].1,  // between entries[i-1] and entries[i]
        }
    }

    pub fn save(&self, path: &str) -> io::Result<()> {
        let mut f = OpenOptions::new().create(true).write(true).open(path)?;
        for (key, offset) in &self.entries {
            writeln!(f, "{},{}", key, offset)?;
        }
        Ok(())
    }

    pub fn load(path: &str) -> io::Result<Self> {
        if !Path::new(path).exists() {
            return Ok(SparseIndex::new());
        }
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() { continue; }
            // Format: key,offset — split_once on last comma since key has no commas
            if let Some((key, offset_str)) = line.split_once(',') {
                if let Ok(offset) = offset_str.parse::<u64>() {
                    entries.push((key.to_string(), offset));
                }
            }
        }
        Ok(SparseIndex { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp(name: &str) -> String {
        format!("/tmp/lsm_index_test_{}.index", name)
    }

    #[test]
    fn find_offset_exact_match() {
        let mut idx = SparseIndex::new();
        idx.add("a".into(), 0);
        idx.add("e".into(), 100);
        idx.add("j".into(), 200);
        assert_eq!(idx.find_offset("e"), 100);
    }

    #[test]
    fn find_offset_between_entries() {
        let mut idx = SparseIndex::new();
        idx.add("a".into(), 0);
        idx.add("e".into(), 100);
        idx.add("j".into(), 200);
        // "g" is between "e" and "j" → seek to "e"'s offset
        assert_eq!(idx.find_offset("g"), 100);
    }

    #[test]
    fn find_offset_before_all_entries() {
        let mut idx = SparseIndex::new();
        idx.add("e".into(), 100);
        // "a" < "e" → scan from file start
        assert_eq!(idx.find_offset("a"), 0);
    }

    #[test]
    fn find_offset_empty_index() {
        let idx = SparseIndex::new();
        assert_eq!(idx.find_offset("anything"), 0);
    }

    #[test]
    fn save_and_load_roundtrip() {
        let path = tmp("roundtrip");
        let _ = fs::remove_file(&path);

        let mut idx = SparseIndex::new();
        idx.add("apple".into(), 0);
        idx.add("mango".into(), 512);
        idx.add("zebra".into(), 1024);
        idx.save(&path).unwrap();

        let loaded = SparseIndex::load(&path).unwrap();
        assert_eq!(loaded.find_offset("mango"), 512);
        assert_eq!(loaded.find_offset("orange"), 512); // between mango and zebra
        assert_eq!(loaded.find_offset("zebra"), 1024);
    }
}
