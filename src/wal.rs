use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};

pub struct Wal {
    path: String,
}

impl Wal {
    pub fn new(path: &str) -> io::Result<Self> {
        // Create file if it doesn't exist
        OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal { path: path.to_string() })
    }

    pub fn append(&self, key: &str, value: &str) -> io::Result<()> {
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        writeln!(file, "{},{}", key, value)?;
        Ok(())
    }

    pub fn recover(&self) -> io::Result<Vec<(String, String)>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() { continue; }
            if let Some((k, v)) = line.split_once(',') {
                entries.push((k.to_string(), v.to_string()));
            }
        }
        Ok(entries)
    }

    pub fn truncate(&self) -> io::Result<()> {
        OpenOptions::new().write(true).truncate(true).open(&self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp(name: &str) -> String {
        let p = format!("/tmp/lsm_wal_test_{}.log", name);
        let _ = fs::remove_file(&p);
        p
    }

    #[test]
    fn append_and_recover() {
        let path = tmp("basic");
        let wal = Wal::new(&path).unwrap();
        wal.append("name", "ahmed").unwrap();
        wal.append("lang", "rust").unwrap();
        let entries = wal.recover().unwrap();
        assert_eq!(entries, vec![
            ("name".to_string(), "ahmed".to_string()),
            ("lang".to_string(), "rust".to_string()),
        ]);
    }

    #[test]
    fn truncate_clears() {
        let path = tmp("truncate");
        let wal = Wal::new(&path).unwrap();
        wal.append("k", "v").unwrap();
        wal.truncate().unwrap();
        let entries = wal.recover().unwrap();
        assert!(entries.is_empty());
    }
}
