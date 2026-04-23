use std::collections::BTreeMap;

pub struct MemTable {
    data: BTreeMap<String, String>,
    pub threshold: usize,
}

impl MemTable {
    pub fn new(threshold: usize) -> Self {
        MemTable {
            data: BTreeMap::new(),
            threshold,
        }
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn is_full(&self) -> bool {
        self.data.len() >= self.threshold
    }

    /// Drains all entries in sorted key order and empties self.
    pub fn drain_sorted(&mut self) -> Vec<(String, String)> {
        let entries: Vec<(String, String)> = self.data.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.data.clear();
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut m = MemTable::new(3);
        m.insert("b".into(), "2".into());
        m.insert("a".into(), "1".into());
        assert_eq!(m.get("a"), Some(&"1".to_string()));
        assert_eq!(m.get("missing"), None);
    }

    #[test]
    fn is_full() {
        let mut m = MemTable::new(2);
        assert!(!m.is_full());
        m.insert("x".into(), "1".into());
        assert!(!m.is_full());
        m.insert("y".into(), "2".into());
        assert!(m.is_full());
    }

    #[test]
    fn drain_sorted_order_and_empties() {
        let mut m = MemTable::new(5);
        m.insert("c".into(), "3".into());
        m.insert("a".into(), "1".into());
        m.insert("b".into(), "2".into());
        let drained = m.drain_sorted();
        assert_eq!(drained, vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
            ("c".to_string(), "3".to_string()),
        ]);
        assert_eq!(m.get("a"), None);
    }
}
