#[macro_use]
extern crate schemamama;
extern crate schemamama_postgres;
extern crate postgres;

use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};
use postgres::{Client, Transaction, NoTls};
use postgres::error::Error as PostgresError;

fn make_database_connection() -> Client {
    let mut client = Client::connect("postgres://postgres@localhost", NoTls).unwrap();
    let statement = client.prepare("SET search_path TO pg_temp;").unwrap();
    client.execute(&statement, &[]).unwrap();
    client
}

fn current_schema_name(client: &mut Client) -> String {
    let statement = client.prepare("SELECT CURRENT_SCHEMA();").unwrap();
    let result = client.query(&statement, &[]).unwrap();
    result.iter().next().map(|r| r.get(0)).unwrap()
}

struct FirstMigration;
migration!(FirstMigration, 10, "first migration");

impl PostgresMigration for FirstMigration {
    fn up(&self, transaction: &mut Transaction) -> Result<(), PostgresError> {
        transaction.execute("CREATE TABLE first (id BIGINT PRIMARY KEY);", &[]).map(|_| ())
    }

    fn down(&self, transaction: &mut Transaction) -> Result<(), PostgresError> {
        transaction.execute("DROP TABLE first;", &[]).map(|_| ())
    }
}

struct SecondMigration;
migration!(SecondMigration, 20, "second migration");

impl PostgresMigration for SecondMigration {
}

#[test]
fn test_setup() {
    let mut client = make_database_connection();
    let schema_name = current_schema_name(&mut client);
    let mut adapter = PostgresAdapter::new(&mut client);
    let mut client = make_database_connection();
    let query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = $1 AND \
                 tablename = 'schemamama';";

    for _ in 0..2 {
        adapter.setup_schema().unwrap();
        let statement = client.prepare(query).unwrap();
        assert_eq!(client.execute(&statement, &[&schema_name]).unwrap(), 1);
    }
}

#[test]
fn test_setup_with_custom_metadata_table() {
    let mut client = make_database_connection();
    let schema_name = current_schema_name(&mut client);
    let mut adapter = PostgresAdapter::with_metadata_table(&mut client, "__custom__");
    let mut client = make_database_connection();
    let query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = $1 AND \
                 tablename = '__custom__';";

    for _ in 0..2 {
        adapter.setup_schema().unwrap();
        let statement = client.prepare(query).unwrap();
        assert_eq!(client.execute(&statement, &[&schema_name]).unwrap(), 1);
    }
}

#[test]
fn test_migration_count() {
    let mut client = make_database_connection();
    let mut adapter = PostgresAdapter::new(&mut client);
    adapter.setup_schema().unwrap();
    let mut migrator = Migrator::new(adapter);
    migrator.register(Box::new(FirstMigration));
    migrator.register(Box::new(SecondMigration));

    migrator.up(Some(1337)).unwrap();
    assert_eq!(migrator.current_version().unwrap(), Some(20));
    migrator.down(None).unwrap();
    assert_eq!(migrator.current_version().unwrap(), None);
}

#[test]
fn test_migration_up_and_down() {
    let mut client = make_database_connection();
    let schema_name = current_schema_name(&mut client);
    let mut adapter = PostgresAdapter::new(&mut client);
    let mut client = make_database_connection();
    adapter.setup_schema().unwrap();
    let mut migrator = Migrator::new(adapter);
    migrator.register(Box::new(FirstMigration));

    migrator.up(Some(10)).unwrap();
    let query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = $1 AND \
                 tablename = 'first';";
    let statement = client.prepare(query).unwrap();
    assert_eq!(client.execute(&statement, &[&schema_name]).unwrap(), 1);

    migrator.down(None).unwrap();
    assert_eq!(client.execute(&statement, &[&schema_name]).unwrap(), 0);
}
