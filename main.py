from database_utils import get_engine, create_schemas
from scripts.bronze.load_bronze import load_bronze

def main():
    print("Initializing Data Warehouse Pipeline...")

    try:
        # Step 1: Create Database Engine
        engine = get_engine()
        print("Database engine created successfully.")

        # Step 2: Create Schemas if they don't exist
        create_schemas(engine)

        # Step 3: Load Bronze Data
        load_bronze(engine)
        
        # Future Steps:
        # - Ingest data from source folders into Bronze layer
        # - Transform and load data into Silver layer
        # - Further transform and load data into Gold layer
        # - Implement logging, error handling, and notifications
        print("Data Warehouse Pipeline setup completed successfully.")
    except Exception as e:
        print(f"An error occurred during pipeline initialization: {e}")

if __name__ == "__main__":
    main()
    