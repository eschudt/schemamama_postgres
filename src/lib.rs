extern crate schemamama;
extern crate postgres;

use postgres::error::Error as PostgresError;
use postgres::{Client, Transaction};
use schemamama::{Adapter, Migration, Version};
use std::collections::BTreeSet;

/// A migration to be used within a PostgreSQL client.
pub trait PostgresMigration : Migration {
    /// Called when this migration is to be executed. This function has an empty body by default,
    /// so its implementation is optional.
    #[allow(unused_variables)]
    fn up(&self, transaction: &mut Transaction) -> Result<(), PostgresError> {
        Ok(())
    }

    /// Called when this migration is to be reversed. This function has an empty body by default,
    /// so its implementation is optional.
    #[allow(unused_variables)]
    fn down(&self, transaction: &mut Transaction) -> Result<(), PostgresError> {
        Ok(())
    }
}

/// An adapter that allows its migrations to act upon PostgreSQL client transactions.
pub struct PostgresAdapter<'a> {
    client: &'a mut Client,
    metadata_table: &'static str,
}

impl<'a> PostgresAdapter<'a> {
    /// Create a new migrator tied to a PostgreSQL client.
    pub fn new(client: &'a mut Client) -> PostgresAdapter<'a> {
        Self::with_metadata_table(client, "schemamama")
    }

    /// Create a new migrator tied to a PostgreSQL client with custom metadata table name
    pub fn with_metadata_table(
        client: &'a mut Client,
        metadata_table: &'static str
    ) -> PostgresAdapter<'a> {
        PostgresAdapter { client, metadata_table }
    }

    /// Create the tables Schemamama requires to keep track of schema state. If the tables already
    /// exist, this function has no operation.
    pub fn setup_schema(&mut self) -> Result<(), PostgresError> {
        let query = format!("CREATE TABLE IF NOT EXISTS {} (version BIGINT PRIMARY KEY);", self.metadata_table);
        let statement = self.client.prepare(&query)?;
        self.client.execute(&statement, &[]).map(|_| ())
    }
}

fn record_version(transaction: &mut Transaction, version: Version, metadata_table: &str) -> Result<(), PostgresError> {
    let query = format!("INSERT INTO {} (version) VALUES ($1);", metadata_table);
    let statement = transaction.prepare(&query)?;
    transaction.execute(&statement, &[&version]).map(|_| ())
}

fn erase_version(transaction: &mut Transaction, version: Version, metadata_table: &str) -> Result<(), PostgresError> {
    let query = format!("DELETE FROM {} WHERE version = $1;", metadata_table);
    let statement = transaction.prepare(&query)?;
    transaction.execute(&statement, &[&version]).map(|_| ())
}

impl<'a> Adapter for PostgresAdapter<'a> {
    type MigrationType = dyn PostgresMigration;
    type Error = PostgresError;

    fn current_version(&mut self) -> Result<Option<Version>, PostgresError> {
        let query = format!("SELECT version FROM {} ORDER BY version DESC LIMIT 1;", self.metadata_table);
        let statement = self.client.prepare(&query)?;
        let row = self.client.query(&statement, &[])?;
        Ok(row.iter().next().map(|r| r.get(0)))
    }

    fn migrated_versions(&mut self) -> Result<BTreeSet<Version>, PostgresError> {
        let query = format!("SELECT version FROM {};", self.metadata_table);
        let statement = self.client.prepare(&query)?;
        let row = self.client.query(&statement, &[])?;
        Ok(row.iter().map(|r| r.get(0)).collect())
    }

    fn apply_migration(&mut self, migration: &dyn PostgresMigration) -> Result<(), PostgresError> {
        let mut transaction = self.client.transaction()?;
        migration.up(&mut transaction)?;
        record_version(&mut transaction, migration.version(), self.metadata_table)?;
        transaction.commit()?;
        Ok(())
    }

    fn revert_migration(&mut self, migration: &dyn PostgresMigration) -> Result<(), PostgresError> {
        let mut transaction = self.client.transaction()?;
        migration.down(&mut transaction)?;
        erase_version(&mut transaction, migration.version(), self.metadata_table)?;
        transaction.commit()?;
        Ok(())
    }
}
