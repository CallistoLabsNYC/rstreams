use std::collections::HashMap;

use redb::{Database, Error, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

pub trait KVStore {
    fn get<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: DeserializeOwned;

    fn insert<T>(&mut self, key: &str, value: T) -> Result<(), Error>
    where
        T: Serialize;
}

pub struct Store<'a> {
    table: TableDefinition<'a, &'static str, &'static [u8]>,
    db: Database,
}

impl<'a> Store<'a> {
    pub fn new(name: &'a str) -> Result<Self, Error> {
        let table = TableDefinition::new(name);
        let db = Database::create(format!("{}.redb", name)).unwrap();

        Ok(Self { table, db })
    }
}

impl<'a> KVStore for Store<'a> {
    fn get<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: DeserializeOwned,
    {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(self.table).unwrap();
        let x = match table.get(key) {
            Err(err) => Err(err.into()),
            Ok(optional_value) => match optional_value {
                None => Ok(None),
                Some(v) => Ok(Some(serde_json::from_slice(v.value()).unwrap())),
            },
        };
        x
    }

    fn insert<T>(&mut self, key: &str, value: T) -> Result<(), Error>
    where
        T: Serialize,
    {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(self.table).unwrap();
            table
                .insert(key, serde_json::to_vec(&value).unwrap().as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();
        Ok(())
    }
}

impl<'a> KVStore for HashMap<String, Vec<u8>> {
    fn get<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: DeserializeOwned,
    {
        match self.get(key) {
            None => Ok(None),
            Some(v) => Ok(Some(serde_json::from_slice(v.as_ref()).unwrap())),
        }
    }

    fn insert<T>(&mut self, key: &str, value: T) -> Result<(), Error>
    where
        T: Serialize,
    {
        self.insert(key.to_owned(), serde_json::to_vec(&value).unwrap());
        Ok(())
    }
}

