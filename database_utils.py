from sqlalchemy import create_engine, text
from config import DB_CONFIG

def get_engine():
    # Construct the connection string once here
    conn_str = f"mssql+pyodbc://{DB_CONFIG['server']}/{DB_CONFIG['database']}?driver={DB_CONFIG['driver']}&trusted_connection=yes"
    return create_engine(conn_str, fast_executemany=True,  isolation_level="AUTOCOMMIT")

def create_schemas(engine):
    """
    Checks if medallion schemas exist in SQL Server and creates them if missing.
    """
    layers = ["bronze", "silver", "gold"]
    
    with engine.connect() as conn:
        for layer in layers:
            # 1. Query the system catalog to see if the schema name exists
            check_query = text("SELECT 1 FROM sys.schemas WHERE name = :schema_name")
            result = conn.execute(check_query, {"schema_name": layer}).fetchone()
            
            if not result:
                print(f"Creating schema: {layer}")
                # 2. Execute the create command if it's missing
                conn.execute(text(f"CREATE SCHEMA {layer}"))
            else:
                print(f"Schema '{layer}' already exists. Skipping...")

def truncate_table(engine, schema, table):
    """
    Safely clears all data from a specific table within a schema.
    Used to ensure an idempotent Full Load.
    """
    with engine.connect() as conn:
        # SQL Server requires the schema.tablename format
        sql = text(f"TRUNCATE TABLE {schema}.{table}")
        try:
            conn.execute(sql)
            conn.commit()
            print(f"Table '{schema}.{table}' truncated successfully.")
        except Exception as e:
            print(f"Error truncating table '{schema}.{table}': {e}")