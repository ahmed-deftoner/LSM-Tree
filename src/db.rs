use std::io;
use crate::memtable::MemTable;
use crate::sstable::SSTableManager;
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

    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        // Check hot MemTable first, then cold SSTables newest→oldest
        if let Some(v) = self.memtable.get(key) {
            return Ok(Some(v.clone()));
        }
        self.sstables.get(key)
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
        for i in 0..9 {
            assert_eq!(
                db.get(&format!("k{}", i)).unwrap(),
                Some(format!("v{}", i))
            );
        }
        assert_eq!(db.sstables.files.len(), 1);
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
