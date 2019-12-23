extern crate schemamama;
extern crate postgres;

use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemamama::{Adapter, Migration, Version};
use std::collections::BTreeSet;

/// A migration to be used within a PostgreSQL connection.
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

/// An adapter that allows its migrations to act upon PostgreSQL connection transactions.
pub struct PostgresAdapter<'a> {
    connection: &'a dyn postgres::GenericConnection,
    metadata_table: &'static str,
}

impl<'a> PostgresAdapter<'a> {
    /// Create a new migrator tied to a PostgreSQL connection.
    pub fn new(connection: &'a dyn postgres::GenericConnection) -> PostgresAdapter<'a> {
        Self::with_metadata_table(connection, "schemamama")
    }

    /// Create a new migrator tied to a PostgreSQL connection with custom metadata table name
    pub fn with_metadata_table(
        connection: &'a dyn postgres::GenericConnection,
        metadata_table: &'static str
    ) -> PostgresAdapter<'a> {
        PostgresAdapter { connection, metadata_table }
    }

    /// Create the tables Schemamama requires to keep track of schema state. If the tables already
    /// exist, this function has no operation.
    pub fn setup_schema(&self) -> Result<(), PostgresError> {
        let query = format!("CREATE TABLE IF NOT EXISTS {} (version BIGINT PRIMARY KEY);", self.metadata_table);
        self.connection.execute(&query, &[]).map(|_| ())
    }

    fn record_version(&self, version: Version) -> Result<(), PostgresError> {
        let query = format!("INSERT INTO {} (version) VALUES ($1);", self.metadata_table);
        self.connection.execute(&query, &[&version]).map(|_| ())
    }

    fn erase_version(&self, version: Version) -> Result<(), PostgresError> {
        let query = format!("DELETE FROM {} WHERE version = $1;", self.metadata_table);
        self.connection.execute(&query, &[&version]).map(|_| ())
    }
}

impl<'a> Adapter for PostgresAdapter<'a> {
    type MigrationType = dyn PostgresMigration;
    type Error = PostgresError;

    fn current_version(&self) -> Result<Option<Version>, PostgresError> {
        let query = format!("SELECT version FROM {} ORDER BY version DESC LIMIT 1;", self.metadata_table);
        let statement = self.connection.prepare(&query)?;
        let row = statement.query(&[])?;
        Ok(row.iter().next().map(|r| r.get(0)))
    }

    fn migrated_versions(&self) -> Result<BTreeSet<Version>, PostgresError> {
        let query = format!("SELECT version FROM {};", self.metadata_table);
        let statement = self.connection.prepare(&query)?;
        let row = statement.query(&[])?;
        Ok(row.iter().map(|r| r.get(0)).collect())
    }

    fn apply_migration(&self, migration: &dyn PostgresMigration) -> Result<(), PostgresError> {
        let mut transaction = self.connection.transaction()?;
        migration.up(&mut transaction)?;
        self.record_version(migration.version())?;
        transaction.commit()?;
        Ok(())
    }

    fn revert_migration(&self, migration: &dyn PostgresMigration) -> Result<(), PostgresError> {
        let mut transaction = self.connection.transaction()?;
        migration.down(&mut transaction)?;
        self.erase_version(migration.version())?;
        transaction.commit()?;
        Ok(())
    }
}
