mod db;
mod memtable;
mod sstable;
mod wal;

use db::Db;

fn main() -> std::io::Result<()> {
    let mut store = Db::open("data")?;

    store.set("name", "ahmed")?;
    store.set("lang", "rust")?;
    store.set("name", "nadeem")?; // overwrite — latest wins
    store.set("city", "karachi")?; // 3rd unique key → triggers flush → SSTable written

    println!("{:?}", store.get("name")?);    // Some("nadeem")
    println!("{:?}", store.get("lang")?);    // Some("rust")
    println!("{:?}", store.get("missing")?); // None

    Ok(())
}
