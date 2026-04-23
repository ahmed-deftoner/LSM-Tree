use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

pub struct SSTableManager {
    dir: String,
    pub files: Vec<String>,  // oldest → newest
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
                // sstable_00000000.sst → parse the 8-digit number
                let stem = name.trim_start_matches("sstable_").trim_end_matches(".sst");
                if let Ok(id) = stem.parse::<u64>() {
                    let path = format!("{}/{}", dir, name);
                    found.push((id, path));
                }
            }
        }
        found.sort_by_key(|(id, _)| *id);

        let next_id = found.last().map(|(id, _)| id + 1).unwrap_or(0);
        let files = found.into_iter().map(|(_, p)| p).collect();

        Ok(SSTableManager {
            dir: dir.to_string(),
            files,
            next_id,
            compaction_threshold,
        })
    }

    /// Write sorted entries to a new SSTable file. Triggers compaction if threshold reached.
    pub fn flush(&mut self, entries: Vec<(String, String)>) -> io::Result<()> {
        let path = format!("{}/sstable_{:08}.sst", self.dir, self.next_id);
        let mut file = OpenOptions::new().create(true).write(true).open(&path)?;
        for (k, v) in &entries {
            writeln!(file, "{},{}", k, v)?;
        }
        file.flush()?;
        self.files.push(path);
        self.next_id += 1;

        if self.files.len() >= self.compaction_threshold {
            self.compact()?;
        }
        Ok(())
    }

    /// Check MemTable first, then scan SSTables newest → oldest. Returns first match.
    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        for path in self.files.iter().rev() {
            if let Some(v) = scan_file(path, key)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Merge all SSTable files (oldest → newest) into one, deduplicating. Delete old files.
    fn compact(&mut self) -> io::Result<()> {
        let mut merged: BTreeMap<String, String> = BTreeMap::new();

        // Oldest → newest so newer writes overwrite older for same key
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

        // Write compacted file
        let new_path = format!("{}/sstable_{:08}.sst", self.dir, self.next_id);
        let mut out = OpenOptions::new().create(true).write(true).open(&new_path)?;
        for (k, v) in &merged {
            writeln!(out, "{},{}", k, v)?;
        }
        out.flush()?;

        // Delete old files
        for path in &self.files {
            fs::remove_file(path)?;
        }

        self.next_id += 1;
        self.files = vec![new_path];
        Ok(())
    }
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
        // 3 flushes triggers compaction
        mgr.flush(vec![("a".into(), "1".into()), ("b".into(), "old".into())]).unwrap();
        mgr.flush(vec![("b".into(), "new".into()), ("c".into(), "3".into())]).unwrap();
        mgr.flush(vec![("d".into(), "4".into())]).unwrap();

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
        // Recreate manager — should discover the file
        let mgr2 = SSTableManager::new(&dir, 10).unwrap();
        assert_eq!(mgr2.files.len(), 1);
        assert_eq!(mgr2.get("x").unwrap(), Some("42".to_string()));
    }
}
