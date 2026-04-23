use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::bloom::BloomFilter;
use crate::index::SparseIndex;

/// Sentinel stored as the value when a key is deleted.
/// `get()` returns this raw; `Db` layer converts it to `None`.
/// `compact()` drops tombstone entries so deleted keys don't persist forever.
pub const TOMBSTONE: &str = "\x00TOMBSTONE";

// Bloom filter params: 8192 bits (~1KB per filter), 3 hash functions.
// False positive rate ≈ 0.8% for up to 1000 keys.
const BLOOM_BITS: usize = 8192;
const BLOOM_HASHES: usize = 3;

// Sample one index entry every N keys.
// Smaller N = more index entries = faster seek, more memory.
const SPARSE_INDEX_INTERVAL: usize = 4;

pub struct SSTableManager {
    dir: String,
    pub files: Vec<String>,    // .sst paths, oldest → newest
    filters: Vec<BloomFilter>, // one per file, same order
    indexes: Vec<SparseIndex>, // one per file, same order
    next_id: u64,
    pub compaction_threshold: usize,
}

impl SSTableManager {
    pub fn new(dir: &str, compaction_threshold: usize) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

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

        let mut files = Vec::new();
        let mut filters = Vec::new();
        let mut indexes = Vec::new();
        for (_, sst_path) in found {
            let filter = load_bloom(&bloom_path(&sst_path))?;
            let index = SparseIndex::load(&index_path(&sst_path))?;
            files.push(sst_path);
            filters.push(filter);
            indexes.push(index);
        }

        Ok(SSTableManager { dir: dir.to_string(), files, filters, indexes, next_id, compaction_threshold })
    }

    /// Write sorted entries to a new SSTable + bloom filter + sparse index.
    pub fn flush(&mut self, entries: Vec<(String, String)>) -> io::Result<()> {
        let sst_path = format!("{}/sstable_{:08}.sst", self.dir, self.next_id);
        let mut file = OpenOptions::new().create(true).write(true).open(&sst_path)?;

        let mut filter = BloomFilter::new(BLOOM_BITS, BLOOM_HASHES);
        let mut index = SparseIndex::new();
        let mut byte_offset: u64 = 0;

        for (i, (k, v)) in entries.iter().enumerate() {
            filter.insert(k);
            if i % SPARSE_INDEX_INTERVAL == 0 {
                index.add(k.clone(), byte_offset);
            }
            let line = format!("{},{}\n", k, v);
            byte_offset += line.len() as u64;
            file.write_all(line.as_bytes())?;
        }
        file.flush()?;

        save_bloom(&bloom_path(&sst_path), &filter)?;
        index.save(&index_path(&sst_path))?;

        self.files.push(sst_path);
        self.filters.push(filter);
        self.indexes.push(index);
        self.next_id += 1;
        Ok(())
    }

    pub fn needs_compaction(&self) -> bool {
        self.files.len() >= self.compaction_threshold
    }

    /// Bloom filter → index seek → short sorted scan (early-exit).
    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        for ((path, filter), index) in self.files.iter()
            .zip(self.filters.iter())
            .zip(self.indexes.iter())
            .rev()
        {
            if !filter.contains(key) {
                continue; // definitely not here
            }
            let offset = index.find_offset(key);
            if let Some(v) = scan_from(path, key, offset)? {
                if v == TOMBSTONE { return Ok(None); }
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Merge all SSTables, drop tombstones, rebuild bloom + index. Delete old files.
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

        let new_sst = format!("{}/sstable_{:08}.sst", self.dir, self.next_id);
        let mut out = OpenOptions::new().create(true).write(true).open(&new_sst)?;
        let mut new_filter = BloomFilter::new(BLOOM_BITS, BLOOM_HASHES);
        let mut new_index = SparseIndex::new();
        let mut byte_offset: u64 = 0;

        for (i, (k, v)) in merged.iter().enumerate() {
            if v == TOMBSTONE { continue; }
            new_filter.insert(k);
            if i % SPARSE_INDEX_INTERVAL == 0 {
                new_index.add(k.clone(), byte_offset);
            }
            let line = format!("{},{}\n", k, v);
            byte_offset += line.len() as u64;
            out.write_all(line.as_bytes())?;
        }
        out.flush()?;

        save_bloom(&bloom_path(&new_sst), &new_filter)?;
        new_index.save(&index_path(&new_sst))?;

        for path in &self.files {
            fs::remove_file(path)?;
            let _ = fs::remove_file(bloom_path(path));
            let _ = fs::remove_file(index_path(path));
        }

        self.next_id += 1;
        self.files = vec![new_sst];
        self.filters = vec![new_filter];
        self.indexes = vec![new_index];
        Ok(())
    }
}

fn bloom_path(sst_path: &str) -> String { sst_path.replace(".sst", ".bloom") }
fn index_path(sst_path: &str) -> String { sst_path.replace(".sst", ".index") }

fn save_bloom(path: &str, filter: &BloomFilter) -> io::Result<()> {
    let mut f = OpenOptions::new().create(true).write(true).open(path)?;
    f.write_all(filter.to_bytes())?;
    Ok(())
}

fn load_bloom(path: &str) -> io::Result<BloomFilter> {
    if !Path::new(path).exists() {
        return Ok(BloomFilter::new(BLOOM_BITS, BLOOM_HASHES));
    }
    let mut f = File::open(path)?;
    let mut bytes = Vec::new();
    f.read_to_end(&mut bytes)?;
    Ok(BloomFilter::from_bytes(&bytes, BLOOM_BITS, BLOOM_HASHES))
}

/// Seek to `offset` in the SSTable, scan forward.
/// Early-exit when current key > target (file is sorted — target can't appear later).
fn scan_from(path: &str, key: &str, offset: u64) -> io::Result<Option<String>> {
    if !Path::new(path).exists() { return Ok(None); }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if let Some((k, v)) = line.split_once(',') {
            if k == key { return Ok(Some(v.to_string())); }
            if k > key  { return Ok(None); } // sorted: won't appear later
        }
    }
    Ok(None)
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
        let mgr2 = SSTableManager::new(&dir, 10).unwrap();
        assert!(mgr2.filters[0].contains("k"));
        assert!(!mgr2.filters[0].contains("definitely_not_here"));
    }

    #[test]
    fn bloom_filter_rebuilt_after_compaction() {
        let dir = tmp("bloom_compact");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![("a".into(), "1".into())]).unwrap();
        mgr.flush(vec![("b".into(), "2".into())]).unwrap();
        mgr.compact().unwrap();
        assert!(mgr.filters[0].contains("a"));
        assert!(mgr.filters[0].contains("b"));
        assert!(!mgr.filters[0].contains("z"));
    }

    #[test]
    fn tombstone_dropped_during_compaction() {
        let dir = tmp("tombstone_compact");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![("k".into(), "v".into())]).unwrap();
        mgr.flush(vec![("k".into(), TOMBSTONE.into())]).unwrap();
        mgr.compact().unwrap();
        assert_eq!(mgr.get("k").unwrap(), None);
        let content = std::fs::read_to_string(&mgr.files[0]).unwrap();
        assert!(!content.contains("k,"));
    }

    #[test]
    fn tombstone_in_newer_file_shadows_older_value() {
        let dir = tmp("tombstone_shadow");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![("k".into(), "alive".into())]).unwrap();
        mgr.flush(vec![("k".into(), TOMBSTONE.into())]).unwrap();
        assert_eq!(mgr.get("k").unwrap(), None);
    }

    #[test]
    fn sparse_index_built_and_used() {
        let dir = tmp("sparse_index");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        // 8 keys — index entries at positions 0 and 4 (interval=4)
        let entries: Vec<(String, String)> = (0..8)
            .map(|i| (format!("key{:02}", i), format!("val{}", i)))
            .collect();
        mgr.flush(entries).unwrap();

        // All keys reachable via index seek
        for i in 0..8 {
            assert_eq!(
                mgr.get(&format!("key{:02}", i)).unwrap(),
                Some(format!("val{}", i))
            );
        }
    }

    #[test]
    fn sparse_index_survives_restart() {
        let dir = tmp("index_restart");
        {
            let mut mgr = SSTableManager::new(&dir, 10).unwrap();
            let entries: Vec<(String, String)> = (0..8)
                .map(|i| (format!("k{:02}", i), format!("v{}", i)))
                .collect();
            mgr.flush(entries).unwrap();
        }
        let mgr2 = SSTableManager::new(&dir, 10).unwrap();
        assert_eq!(mgr2.get("k05").unwrap(), Some("v5".to_string()));
    }

    #[test]
    fn sparse_index_rebuilt_after_compaction() {
        let dir = tmp("index_compact");
        let mut mgr = SSTableManager::new(&dir, 10).unwrap();
        mgr.flush(vec![("a".into(), "1".into()), ("b".into(), "2".into()),
                       ("c".into(), "3".into()), ("d".into(), "4".into())]).unwrap();
        mgr.flush(vec![("e".into(), "5".into())]).unwrap();
        mgr.compact().unwrap();
        // Index rebuilt — all keys still reachable
        for (k, v) in [("a","1"),("b","2"),("c","3"),("d","4"),("e","5")] {
            assert_eq!(mgr.get(k).unwrap(), Some(v.to_string()));
        }
    }
}
