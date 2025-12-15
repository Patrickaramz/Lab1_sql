import pandas as pd
import duckdb
import sqlite3
import os

SQLITE_DB_PATH = 'data/sqlite-sakila.db'
DUCKDB_DB_PATH = 'sakila.duckdb'

def get_sqlite_table_names(conn):
    """Hämtar namnen på alla tabeller från Sakila SQLite-schemat."""
    cursor = conn.cursor()
    # Query för att exkludera interna systemtabeller
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in cursor.fetchall()]
    return tables

def migrate_sqlite_to_duckdb(sqlite_path, duckdb_path):
    """
    Funktion för att migrera data. Läser in SQLite tabell för tabell till Pandas
    och skriver sedan till DuckDB.
    """
    print("Startar migrering av Sakila SQLite till DuckDB...")

    # 1. Kontrollera källans existens
    if not os.path.exists(sqlite_path):
        print(f"\nFEL: Källfilen hittades inte: {sqlite_path}")
        print("Kontrollera filnamn och sökväg.")
        return

    # 2. Anslut till SQLite
    try:
        sqlite_conn = sqlite3.connect(sqlite_path)
    except Exception as e:
        print(f"\nFEL: Kunde inte upprätta SQLite-anslutning.")
        print(e)
        return

    # 3. Anslut till DuckDB (skapar måldatabasen)
    duckdb_conn = duckdb.connect(database=duckdb_path)
    
    table_names = get_sqlite_table_names(sqlite_conn)
    print(f"\nHittade {len(table_names)} tabeller att processa.")

    # 4. Loopa genom tabellerna
    for table_name in table_names:
        print(f"Processar tabell: {table_name}")
        
        # A. Läs in data via Pandas
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, sqlite_conn)
        
        # B. Skriv DataFrame till DuckDB
        # Använder CREATE OR REPLACE för att hantera om databasen redan existerar
        duckdb_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df;")
        
        print(f"  -> Skapade {table_name} med {len(df)} rader.")

    # 5. Stäng anslutningarna
    sqlite_conn.close()
    duckdb_conn.close()
    
    print("\nMigrering slutförd! DuckDB-filen är redo för analys.")


if __name__ == "__main__":
    # Kör migreringen när skriptet startas
    migrate_sqlite_to_duckdb(SQLITE_DB_PATH, DUCKDB_DB_PATH)