use std::io;
use crate::memtable::MemTable;
use crate::sstable::{SSTableManager, TOMBSTONE};
use crate::wal::Wal;

const MEMTABLE_THRESHOLD: usize = 3;
const COMPACTION_THRESHOLD: usize = 3;

pub struct Db {
    memtable: MemTable,
    sstables: SSTableManager,
    wal: Wal,
}

impl Db {
    /// Open (or create) an LSM store at `dir`.
    /// Replays WAL on startup to recover any unflushed writes.
    pub fn open(dir: &str) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        let wal = Wal::new(&format!("{}/wal.log", dir))?;
        let mut sstables = SSTableManager::new(dir, COMPACTION_THRESHOLD)?;
        let mut memtable = MemTable::new(MEMTABLE_THRESHOLD);

        // Replay WAL into MemTable; flush to SSTable if threshold hit during replay.
        // WAL is truncated only after a successful flush — so partial replays are safe.
        for (k, v) in wal.recover()? {
            memtable.insert(k, v);
            if memtable.is_full() {
                sstables.flush(memtable.drain_sorted())?;
                // Don't truncate WAL here — we may still have more entries to replay.
                // WAL will be truncated after the next real set() flush.
            }
        }

        Ok(Db { memtable, sstables, wal })
    }

    pub fn set(&mut self, key: &str, value: &str) -> io::Result<()> {
        self.wal.append(key, value)?;  // write-ahead first
        self.memtable.insert(key.to_string(), value.to_string());
        if self.memtable.is_full() {
            self.sstables.flush(self.memtable.drain_sorted())?;
            self.wal.truncate()?;      // safe: all data now in SSTable on disk
        }
        Ok(())
    }

    pub fn sstable_count(&self) -> usize {
        self.sstables.files.len()
    }

    /// Returns true when enough SSTables have accumulated to warrant compaction.
    pub fn needs_compaction(&self) -> bool {
        self.sstables.needs_compaction()
    }

    /// Merge all SSTables into one. Call this on a schedule or when needs_compaction() is true.
    pub fn compact(&mut self) -> io::Result<()> {
        self.sstables.compact()
    }

    /// Write a tombstone for `key`. Subsequent `get()` returns `None`.
    /// Tombstone is removed from disk permanently during the next compaction.
    pub fn delete(&mut self, key: &str) -> io::Result<()> {
        self.set(key, TOMBSTONE)
    }

    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        // Check hot MemTable first, then cold SSTables newest→oldest.
        // A tombstone in either layer means the key is deleted — return None.
        if let Some(v) = self.memtable.get(key) {
            if v == TOMBSTONE { return Ok(None); }
            return Ok(Some(v.clone()));
        }
        self.sstables.get(key) // sstable::get already handles tombstones
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp(name: &str) -> String {
        let p = format!("/tmp/lsm_db_test_{}", name);
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn basic_set_get() {
        let dir = tmp("basic");
        let mut db = Db::open(&dir).unwrap();
        db.set("name", "ahmed").unwrap();
        assert_eq!(db.get("name").unwrap(), Some("ahmed".to_string()));
        assert_eq!(db.get("missing").unwrap(), None);
    }

    #[test]
    fn overwrite_latest_wins() {
        let dir = tmp("overwrite");
        let mut db = Db::open(&dir).unwrap();
        db.set("k", "old").unwrap();
        db.set("k", "new").unwrap();
        assert_eq!(db.get("k").unwrap(), Some("new".to_string()));
    }

    #[test]
    fn key_survives_flush() {
        let dir = tmp("flush");
        let mut db = Db::open(&dir).unwrap();
        // threshold=3: 3rd insert triggers flush, MemTable empties
        db.set("a", "1").unwrap();
        db.set("b", "2").unwrap();
        db.set("c", "3").unwrap();
        assert_eq!(db.get("a").unwrap(), Some("1".to_string()));
        assert_eq!(db.get("b").unwrap(), Some("2".to_string()));
        assert_eq!(db.get("c").unwrap(), Some("3".to_string()));
    }

    #[test]
    fn key_survives_compaction() {
        let dir = tmp("compact");
        let mut db = Db::open(&dir).unwrap();
        // 9 inserts → 3 flushes → 1 compaction
        for i in 0..9 {
            db.set(&format!("k{}", i), &format!("v{}", i)).unwrap();
        }
        db.compact().unwrap();
        for i in 0..9 {
            assert_eq!(
                db.get(&format!("k{}", i)).unwrap(),
                Some(format!("v{}", i))
            );
        }
        assert_eq!(db.sstables.files.len(), 1);
    }

    #[test]
    fn delete_in_memtable() {
        let dir = tmp("del_mem");
        let mut db = Db::open(&dir).unwrap();
        db.set("k", "v").unwrap();
        db.delete("k").unwrap();
        assert_eq!(db.get("k").unwrap(), None);
    }

    #[test]
    fn delete_after_flush_shadows_sstable() {
        let dir = tmp("del_shadow");
        let mut db = Db::open(&dir).unwrap();
        // Flush "k" to SSTable
        db.set("a", "1").unwrap();
        db.set("b", "2").unwrap();
        db.set("k", "alive").unwrap(); // triggers flush
        // Now delete "k" — tombstone lives in MemTable
        db.delete("k").unwrap();
        assert_eq!(db.get("k").unwrap(), None);
    }

    #[test]
    fn delete_cleared_after_compaction() {
        let dir = tmp("del_compact");
        let mut db = Db::open(&dir).unwrap();
        // Write and delete enough to trigger multiple flushes
        for i in 0..3 {
            db.set(&format!("k{}", i), "v").unwrap();
        }
        // k0 is now in SSTable; delete it (tombstone flushes with next batch)
        for i in 0..3 {
            db.delete(&format!("k{}", i)).unwrap();
        }
        db.compact().unwrap();
        // After compaction, tombstones are gone — keys return None
        for i in 0..3 {
            assert_eq!(db.get(&format!("k{}", i)).unwrap(), None);
        }
    }

    #[test]
    fn crash_recovery() {
        let dir = tmp("recovery");
        {
            let mut db = Db::open(&dir).unwrap();
            // 2 writes — below flush threshold, live only in WAL + MemTable
            db.set("x", "10").unwrap();
            db.set("y", "20").unwrap();
            // "crash": drop without flushing
        }
        // Reopen — WAL replays x and y back into MemTable
        let db2 = Db::open(&dir).unwrap();
        assert_eq!(db2.get("x").unwrap(), Some("10".to_string()));
        assert_eq!(db2.get("y").unwrap(), Some("20".to_string()));
    }
}
