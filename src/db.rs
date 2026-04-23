use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

pub struct Db {
    path: String,
}

impl Db {
    pub fn new(path: &str) -> Self {
        Db { path: path.to_string() }
    }

    pub fn set(&self, key: &str, value: &str) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{},{}", key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        if !Path::new(&self.path).exists() {
            return Ok(None);
        }
        let file = File::open(&self.path)?;
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
}
