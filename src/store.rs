use bytes::Bytes;
use nom::AsBytes;
use redb::{Database, Error, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

use crate::{from_bytes, to_bytes};

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

    pub fn get<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: DeserializeOwned,
    {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(self.table).unwrap();
        let x = match table.get(key) {
            Err(err) => Err(err.into()),
            Ok(optional_value) => match optional_value {
                None => Ok(None),
                Some(v) => Ok(Some(from_bytes(Bytes::copy_from_slice(v.value())).unwrap())),
            },
        };
        x
    }

    pub fn insert<T>(&mut self, key: &str, value: T) -> Result<(), Error>
    where
        T: Serialize,
    {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(self.table).unwrap();
            table
                .insert(key, to_bytes(value).unwrap().as_bytes())
                .unwrap();
        }
        write_txn.commit().unwrap();
        Ok(())
    }
}
