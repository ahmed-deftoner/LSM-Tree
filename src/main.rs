mod db;
mod memtable;
mod sstable;
mod wal;

use db::Db;

fn main() -> std::io::Result<()> {
    let mut store = Db::open("data")?;

    // 9 writes → 3 flushes → 3 SSTable files
    for i in 0..9 {
        store.set(&format!("k{}", i), &format!("v{}", i))?;
    }

    println!("SSTables before compact: {}", store.sstable_count());

    // Compaction is now explicit — call it on a schedule, or when needs_compaction() is true
    if store.needs_compaction() {
        store.compact()?;
    }

    println!("SSTables after compact:  {}", store.sstable_count());
    println!("{:?}", store.get("k0")?);
    println!("{:?}", store.get("k8")?);
    println!("{:?}", store.get("missing")?);

    Ok(())
}
