# LSM Tree — Learning Implementation

A Log-Structured Merge Tree key-value store built in Rust from scratch. No external dependencies.

## Architecture

```
Write path:
  set(k, v) → WAL append → MemTable insert → [flush if full] → SSTable → [compact if needed]

Read path:
  get(k) → MemTable → Bloom Filter → Sparse Index seek → SSTable scan (early-exit)
```

### Components

| Module | Role |
|--------|------|
| `memtable.rs` | In-memory `BTreeMap` buffer. Sorted by key. Flushed to disk when full. |
| `wal.rs` | Write-Ahead Log. Append-only file replayed on startup for crash recovery. |
| `sstable.rs` | Sorted String Tables on disk. Manages flush, lookup, and compaction. |
| `bloom.rs` | Per-SSTable bloom filter. Skips disk reads for keys that are definitely absent. |
| `index.rs` | Per-SSTable sparse index. Binary-searches to a byte offset, then scans forward. |
| `db.rs` | Public API. Orchestrates all components. |

### Data files (stored in `data/`)

```
wal.log                   — append-only recovery log
sstable_00000000.sst      — sorted key,value lines
sstable_00000000.bloom    — bloom filter bits (binary)
sstable_00000000.index    — sparse index: key,byte_offset per N keys
```

## Features

- **Append-only writes** — `set()` never overwrites existing data, always appends
- **MemTable** — writes buffer in a sorted `BTreeMap`; flushed to disk when threshold is reached
- **WAL crash recovery** — unflushed MemTable entries are replayed from the WAL on restart
- **SSTables** — immutable sorted files on disk; multiple files merged during compaction
- **Compaction** — merges all SSTables into one, deduplicating keys (latest value wins); decoupled from the write path so it can run on any strategy
- **Bloom filters** — probabilistic check before any disk read; false positives possible, false negatives impossible (~0.8% FP rate at 1000 keys)
- **Sparse index** — samples one `(key, byte_offset)` entry every N keys; binary-searches on lookup then scans forward with early-exit (file is sorted)
- **Tombstone deletes** — `delete(key)` writes a sentinel value; tombstones shadow older values and are purged during compaction

## Usage

```rust
let mut db = Db::open("data")?;

db.set("name", "ahmed")?;
db.set("city", "karachi")?;

db.get("name")?;   // Some("ahmed")
db.get("ghost")?;  // None

db.delete("name")?;
db.get("name")?;   // None

if db.needs_compaction() {
    db.compact()?;
}
```

## Tuning constants (`db.rs`, `sstable.rs`)

| Constant | Default | Effect |
|----------|---------|--------|
| `MEMTABLE_THRESHOLD` | 3 | Keys in memory before flush (set higher for real use) |
| `COMPACTION_THRESHOLD` | 3 | SSTable file count before `needs_compaction()` is true |
| `BLOOM_BITS` | 8192 | Bits per bloom filter (~1 KB); more bits = lower false positive rate |
| `BLOOM_HASHES` | 3 | Hash functions per bloom filter |
| `SPARSE_INDEX_INTERVAL` | 4 | Index one entry every N keys; smaller = faster seek, more memory |

## Potential improvements

### Correctness
- **`fsync` after writes** — currently only `flush()` (userspace buffer); a hard crash could still lose data without `sync_all()`
- **WAL truncation on partial replay** — if the process crashes mid-replay, the WAL could contain a partial line; add a checksum per entry (e.g. CRC32) to detect and skip corrupt records

### Read performance
- **Block cache (LRU)** — cache recently read SSTable blocks in memory; hot keys stop touching disk entirely
- **Binary SSTable format** — fixed-size blocks with a block index; enables efficient block-level caching and avoids line-by-line parsing
- **Multi-level compaction (LSM levels)** — L0 files can overlap; L1+ are sorted and non-overlapping within a level; each level is ~10× the size of the previous; reads become O(levels) instead of O(files)

### Write performance
- **Tiered compaction strategy** — instead of one flat level, group SSTables by size tier; reduces write amplification at the cost of more files per read
- **Background compaction thread** — run compaction off the write path entirely; requires `Arc<Mutex<Db>>` or a message-passing design

### Features
- **Range scans** — `scan(start, end)` is natural since SSTables are sorted; merge results across MemTable and all SSTables
- **Snapshots** — point-in-time read views; assign a sequence number to each write and filter by it on read
- **Column families** — separate MemTable/SSTable trees per logical namespace
- **Compression** — compress SSTable blocks (e.g. LZ4, Snappy) to reduce I/O at the cost of CPU

### Operational
- **Metrics** — expose bloom filter false positive rate, compaction frequency, MemTable size, SSTable count
- **Configurable via builder pattern** — instead of compile-time constants, accept config at `Db::open()`
