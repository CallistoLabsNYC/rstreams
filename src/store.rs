//! Key-Value store

use std::collections::HashMap;

use crate::error::{Error, Result};
use redb::{Database, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

pub trait KVStore {
    fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned;

    fn insert<T>(&mut self, key: &str, value: T) -> Result<()>
    where
        T: Serialize;
}

pub struct Store<'a> {
    table: TableDefinition<'a, &'static str, &'static [u8]>,
    db: Database,
}

impl<'a> Store<'a> {
    pub fn new(name: &'a str) -> Result<Self> {
        let table = TableDefinition::new(name);
        let db = Database::create(format!("{}.redb", name)).map_err(|err| {
            tracing::error!("Store Error: {:?}", err);
            Error::KVStoreError
        })?;

        // need to write to initialize the DB I'm told
        // https://github.com/cberner/redb/issues/731
        let mut store = Self { table, db };
        store.insert("__init__", "__init__")?;

        Ok(store)
    }

    fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let read_txn = self.db.begin_read().map_err(|err| {
            tracing::error!("Store Error: {:?}", err);
            Error::KVStoreError
        })?;

        let table = read_txn.open_table(self.table).map_err(|err| {
            tracing::error!("Store Error: {:?}", err);
            Error::KVStoreError
        })?;

        let optional_value = table.get(key).map_err(|err| {
            tracing::error!("Store Error: {:?}", err);
            Error::KVStoreError
        })?;

        match optional_value {
            None => Ok(None),
            Some(v) => Ok(Some(serde_json::from_slice(v.value()).map_err(|err| {
                tracing::error!("Deserialize Error: {:?}", err);
                Error::KVDataCorrupted
            })?)),
        }
    }

    fn insert<T>(&mut self, key: &str, value: T) -> Result<()>
    where
        T: Serialize,
    {
        let write_txn = self.db.begin_write().map_err(|err| {
            tracing::error!("Store Error: {:?}", err);
            Error::KVStoreError
        })?;
        {
            let mut table = write_txn.open_table(self.table).map_err(|err| {
                tracing::error!("Store Error: {:?}", err);
                Error::KVStoreError
            })?;

            table
                .insert(
                    key,
                    serde_json::to_vec(&value)
                        .map_err(|err| {
                            tracing::error!("Deserialize Error: {:?}", err);
                            Error::KVDataCorrupted
                        })?
                        .as_slice(),
                )
                .map_err(|err| {
                    tracing::error!("Store Error: {:?}", err);
                    Error::KVStoreError
                })?;
        }
        write_txn.commit().map_err(|err| {
            tracing::error!("Store Error: {:?}", err);
            Error::KVStoreError
        })?;
        Ok(())
    }
}

impl<'a> KVStore for Store<'a> {
    fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.get(key)
    }

    fn insert<T>(&mut self, key: &str, value: T) -> Result<()>
    where
        T: Serialize,
    {
        self.insert(key, value)
    }
}

impl KVStore for HashMap<String, Vec<u8>> {
    fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        match self.get(key) {
            None => Ok(None),
            Some(v) => Ok(Some(serde_json::from_slice(v.as_ref()).map_err(|err| {
                tracing::error!("Deserialize Error: {:?}", err);
                Error::KVDataCorrupted
            })?)),
        }
    }

    fn insert<T>(&mut self, key: &str, value: T) -> Result<()>
    where
        T: Serialize,
    {
        self.insert(
            key.to_owned(),
            serde_json::to_vec(&value).map_err(|err| {
                tracing::error!("Deserialize Error: {:?}", err);
                Error::KVDataCorrupted
            })?,
        );
        Ok(())
    }
}
