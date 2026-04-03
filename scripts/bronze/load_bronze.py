import pandas as pd
import os
import time

def load_bronze(engine, base_data_path='./datasets'):
    """
    Load the bronze data from the CSV file and return it as a DataFrame.
    """
    folders = ['CORE','ORG','PERF','PSA','PSN']
    total_start_time = time.time()

    for folder in folders:
        folder_path = os.path.join(base_data_path, folder)
        source_prefix = folder.lower()


        if not os.path.exists(folder_path):
            print(f"Folder {folder} does not exist. Skipping.")
            continue
        
        print(f"Loading data from folder: {folder}")

        for file_name in os.listdir(folder_path):
            if file_name.endswith('.csv'):
                clean_file_name = file_name[:-4].lower()
                table_name = f"{source_prefix}_{clean_file_name}"
                
                file_path = os.path.join(folder_path, file_name)

                file_start_time = time.time()

                try:
                    # Load the CSV file into a DataFrame
                    df = pd.read_csv(file_path, low_memory=False)

                    # Write the DataFrame to the 'bronze' schema in the database
                    df.to_sql(
                        name=table_name,
                        con=engine,
                        schema='bronze',
                        if_exists='replace',
                        index=False,
                        chunksize=10000,
                    )
                    
                    # End timer for this table
                    file_end_time = time.time()
                    duration = file_end_time - file_start_time
                    print(f"✅ {table_name:.<25} Load Time: {duration:.2f} seconds")
                    
                except Exception as e:
                    print(f"❌ Error loading {table_name}: {e}")
                
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    print(f"✅ Total Load Time: {total_duration:.2f} seconds")
