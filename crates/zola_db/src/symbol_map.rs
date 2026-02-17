use std::collections::HashMap;
use std::path::Path;

use crate::error::{ZolaError, Result};

pub struct SymbolMap {
    names: Vec<String>,
    ids: HashMap<String, u64>,
}

impl SymbolMap {
    pub fn new() -> Self {
        SymbolMap {
            names: Vec::new(),
            ids: HashMap::new(),
        }
    }

    pub fn get_or_insert(&mut self, name: &str) -> u64 {
        if let Some(&id) = self.ids.get(name) {
            return id;
        }
        let id = self.names.len() as u64;
        self.names.push(name.to_string());
        self.ids.insert(name.to_string(), id);
        id
    }

    pub fn get_id(&self, name: &str) -> Option<u64> {
        self.ids.get(name).copied()
    }

    pub fn get_name(&self, id: u64) -> Option<&str> {
        self.names.get(id as usize).map(|s| s.as_str())
    }

    pub fn load(path: &Path) -> Result<SymbolMap> {
        if !path.exists() {
            return Ok(SymbolMap::new());
        }
        let content = std::fs::read_to_string(path).map_err(|e| ZolaError::io(path, e))?;
        let mut names = Vec::new();
        let mut ids = HashMap::new();
        for (i, line) in content.lines().enumerate() {
            if !line.is_empty() {
                names.push(line.to_string());
                ids.insert(line.to_string(), i as u64);
            }
        }
        Ok(SymbolMap { names, ids })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let content: String = self.names.join("\n") + "\n";
        std::fs::write(path, content).map_err(|e| ZolaError::io(path, e))?;
        Ok(())
    }
}
