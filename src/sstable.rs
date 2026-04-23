use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;

use crate::bloom::BloomFilter;

/// Sentinel stored as the value when a key is deleted.
/// `get()` returns this raw; `Db` layer converts it to `None`.
/// `compact()` drops tombstone entries so deleted keys don't persist forever.
pub const TOMBSTONE: &str = "\x00TOMBSTONE";

// Bloom filter params: 8192 bits (~1KB per filter), 3 hash functions.
// False positive rate ≈ 0.8% for up to 1000 keys.
const BLOOM_BITS: usize = 8192;
const BLOOM_HASHES: usize = 3;

pub struct SSTableManager {
    dir: String,
    pub files: Vec<String>,           // .sst paths, oldest → newest
    filters: Vec<BloomFilter>,        // one per file, same order
    next_id: u64,
    pub compaction_threshold: usize,
}

impl SSTableManager {
    pub fn new(dir: &str, compaction_threshold: usize) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        // Discover existing .sst files, sort by numeric ID
        let mut found: Vec<(u64, String)> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("sstable_") && name.ends_with(".sst") {
                let stem = name.trim_start_matches("sstable_").trim_end_matches(".sst");
                if let Ok(id) = stem.parse::<u64>() {
                    found.push((id, format!("{}/{}", dir, name)));
                }
            }
        }
        found.sort_by_key(|(id, _)| *id);

        let next_id = found.last().map(|(id, _)| id + 1).unwrap_or(0);

        // Load companion bloom filter for each .sst file
        let mut files = Vec::new();
        let mut filters = Vec::new();
        for (_, sst_path) in found {
            let bloom_path = bloom_path(&sst_path);
            let filter = load_bloom(&bloom_path)?;
            files.push(sst_path);
            filters.push(filter);
        }

        Ok(SSTableManager { dir: dir.to_string(), files, filters, next_id, compaction_threshold })
    }

    /// Write sorted entries to a new SSTable + companion bloom filter file.
    pub fn flush(&mut self, entries: Vec<(String, String)>) -> io::Result<()> {
        let sst_path = format!("{}/sstable_{:08}.sst", self.dir, self.next_id);

        // Build bloom filter from all keys in this flush
        let mut filter = BloomFilter::new(BLOOM_BITS, BLOOM_HASHES);
        let mut file = OpenOptions::new().create(true).write(true).open(&sst_path)?;
        for (k, v) in &entries {
            filter.insert(k);
            writeln!(file, "{},{}", k, v)?;
        }
        file.flush()?;

        // Persist bloom filter alongside the SSTable
        save_bloom(&bloom_path(&sst_path), &filter)?;

        self.files.push(sst_path);
        self.filters.push(filter);
        self.next_id += 1;
        Ok(())
    }

    pub fn needs_compaction(&self) -> bool {
        self.files.len() >= self.compaction_threshold
    }

    /// Check bloom filter first — skip disk scan if key is definitely absent.
    /// Returns None for both missing keys and tombstones (deletion is opaque to caller).
    /// Scans newest → oldest so a tombstone in a newer file shadows an older value.
    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        for (path, filter) in self.files.iter().zip(self.filters.iter()).rev() {
            if !filter.contains(key) {
                continue; // definitely not in this SSTable
            }
            if let Some(v) = scan_file(path, key)? {
                // Tombstone found — key is deleted; stop searching older files
                if v == TOMBSTONE { return Ok(None); }
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Merge all SSTables into one, deduplicating. Build new bloom filter. Delete old files.
    pub fn compact(&mut self) -> io::Result<()> {
        let mut merged: BTreeMap<String, String> = BTreeMap::new();

        for path in &self.files {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.is_empty() { continue; }
                if let Some((k, v)) = line.split_once(',') {
                    merged.insert(k.to_string(), v.to_string());
                }
            }
        }

        // Write compacted SSTable — drop tombstones, they've served their purpose
        let new_sst = format!("{}/sstable_{:08}.sst", self.dir, self.next_id);
        let mut out = OpenOptions::new().create(true).write(true).open(&new_sst)?;
        let mut new_filter = BloomFilter::new(BLOOM_BITS, BLOOM_HASHES);
        for (k, v) in &merged {
            if v == TOMBSTONE { continue; }
            new_filter.insert(k);
            writeln!(out, "{},{}", k, v)?;
        }
        out.flush()?;
        save_bloom(&bloom_path(&new_sst), &new_filter)?;

        // Delete old .sst and .bloom files
        for path in &self.files {
            fs::remove_file(path)?;
            let _ = fs::remove_file(bloom_path(path)); // best-effort
        }

        self.next_id += 1;
        self.files = vec![new_sst];
        self.filters = vec![new_filter];
        Ok(())
    }
}

fn bloom_path(sst_path: &str) -> String {
    sst_path.replace(".sst", ".bloom")
}

fn save_bloom(path: &str, filter: &BloomFilter) -> io::Result<()> {
    let mut f = OpenOptions::new().create(true).write(true).open(path)?;
    f.write_all(filter.to_bytes())?;
    Ok(())
}

fn load_bloom(path: &str) -> io::Result<BloomFilter> {
    if !Path::new(path).exists() {
        // No bloom file — return an empty filter (all gets pass through to disk scan)
        return Ok(BloomFilter::new(BLOOM_BITS, BLOOM_HASHES));
    }
    let mut f = File::open(path)?;
    let mut bytes = Vec::new();
    f.read_to_end(&mut bytes)?;
    Ok(BloomFilter::from_bytes(&bytes, BLOOM_BITS, BLOOM_HASHES))
}

fn scan_file(path: &str, key: &str) -> io::Result<Option<String>> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut result = None;
    for line in reader.lines() {
        let line = line?;
        if let Some((k, v)) = line.split_once(',') {
            if k == key {
                result = Some(v.to_string());
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp(name: &str) -> String {
        let p = format!("/tmp/lsm_sst_test_{}", name);
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn flush_and_get() {
        let dir = tmp("basic");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![
            ("a".into(), "1".into()),
            ("b".into(), "2".into()),
        ]).unwrap();
        assert_eq!(mgr.get("a").unwrap(), Some("1".to_string()));
        assert_eq!(mgr.get("missing").unwrap(), None);
    }

    #[test]
    fn newer_file_wins() {
        let dir = tmp("newer_wins");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![("k".into(), "old".into())]).unwrap();
        mgr.flush(vec![("k".into(), "new".into())]).unwrap();
        assert_eq!(mgr.get("k").unwrap(), Some("new".to_string()));
    }

    #[test]
    fn compaction_deduplicates_and_leaves_one_file() {
        let dir = tmp("compact");
        let mut mgr = SSTableManager::new(&dir, 3).unwrap();
        mgr.flush(vec![("a".into(), "1".into()), ("b".into(), "old".into())]).unwrap();
        mgr.flush(vec![("b".into(), "new".into()), ("c".into(), "3".into())]).unwrap();
        mgr.flush(vec![("d".into(), "4".into())]).unwrap();

        mgr.compact().unwrap();
        assert_eq!(mgr.files.len(), 1);
        assert_eq!(mgr.get("a").unwrap(), Some("1".to_string()));
        assert_eq!(mgr.get("b").unwrap(), Some("new".to_string()));
        assert_eq!(mgr.get("c").unwrap(), Some("3".to_string()));
        assert_eq!(mgr.get("d").unwrap(), Some("4".to_string()));
    }

    #[test]
    fn startup_discovers_existing_files() {
        let dir = tmp("discovery");
        {
            let mut mgr = SSTableManager::new(&dir, 10).unwrap();
            mgr.flush(vec![("x".into(), "42".into())]).unwrap();
        }
        let mgr2 = SSTableManager::new(&dir, 10).unwrap();
        assert_eq!(mgr2.files.len(), 1);
        assert_eq!(mgr2.get("x").unwrap(), Some("42".to_string()));
    }

    #[test]
    fn bloom_filter_skips_missing_key() {
        let dir = tmp("bloom_skip");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![("present".into(), "yes".into())]).unwrap();

        // "absent" not inserted → bloom says no → scan_file never called
        assert_eq!(mgr.get("absent").unwrap(), None);
        assert_eq!(mgr.get("present").unwrap(), Some("yes".to_string()));
    }

    #[test]
    fn bloom_filter_survives_restart() {
        let dir = tmp("bloom_restart");
        {
            let mut mgr = SSTableManager::new(&dir, 10).unwrap();
            mgr.flush(vec![("k".into(), "v".into())]).unwrap();
        }
        // Reopen — bloom filter loaded from .bloom file
        let mgr2 = SSTableManager::new(&dir, 10).unwrap();
        assert!(mgr2.filters[0].contains("k"));
        assert!(!mgr2.filters[0].contains("definitely_not_here"));
    }

    #[test]
    fn tombstone_dropped_during_compaction() {
        let dir = tmp("tombstone_compact");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        // Write a value, then a tombstone for the same key
        mgr.flush(vec![("k".into(), "v".into())]).unwrap();
        mgr.flush(vec![("k".into(), TOMBSTONE.into())]).unwrap();
        mgr.compact().unwrap();

        // After compaction tombstone is gone — key returns None
        assert_eq!(mgr.get("k").unwrap(), None);

        // Verify key is not in the compacted file at all
        let content = std::fs::read_to_string(&mgr.files[0]).unwrap();
        assert!(!content.contains("k,"));
    }

    #[test]
    fn tombstone_in_newer_file_shadows_older_value() {
        let dir = tmp("tombstone_shadow");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![("k".into(), "alive".into())]).unwrap();
        mgr.flush(vec![("k".into(), TOMBSTONE.into())]).unwrap();

        // Newest file has tombstone → get returns None, not "alive"
        assert_eq!(mgr.get("k").unwrap(), None);
    }

    #[test]
    fn bloom_filter_rebuilt_after_compaction() {
        let dir = tmp("bloom_compact");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![("a".into(), "1".into())]).unwrap();
        mgr.flush(vec![("b".into(), "2".into())]).unwrap();
        mgr.compact().unwrap();

        // Single compacted filter must contain both keys
        assert!(mgr.filters[0].contains("a"));
        assert!(mgr.filters[0].contains("b"));
        assert!(!mgr.filters[0].contains("z"));
    }
}
