import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import time

# Record the start time
start_time = time.time()

def insert_csv_to_postgresql(filename=None):
    # Path to the folder containing the CSV files
    path = r'C:\Users\loren\Desktop\MOOC\MOOC_G4_Maud_Lorenzo\Olist_Adile_Evelyne_Lorenzo\archive'
    
    if filename:
        filenames = [filename]
    else:
        filenames = [f.name for f in os.scandir(path) if f.name.endswith('.csv')]
    
    for filename in filenames:
        # Load the CSV file into a pandas dataframe
        dtype = None
        if 'customers' in filename:
            dtype = {'customer_zip_code_prefix': str}
        elif 'geolocation' in filename:
            dtype = {'geolocation_zip_code_prefix': str}
        elif 'sellers' in filename:
            dtype = {'seller_zip_code_prefix': str}

        df = pd.read_csv(os.path.join(path, filename), dtype=dtype)
        # Remove leading and trailing spaces from column names
        df.columns = df.columns.str.strip()
        # Remove the word "dataset" at the end of the table name
        table_name = filename[:-4].replace("_dataset", "")
        # Insert the data into the PostgreSQL database
        df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Execute SQL queries to make necessary changes to the database
    with open('bdd.sql', 'r') as file:
        queries = file.read()

        query_list = queries.split(';')

        with conn.cursor() as cursor:
            for query in query_list:
                if query.strip():  # Ignore empty strings or strings containing only spaces
                    cursor.execute(query)
            conn.commit()

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="t",
    user="postgres",
    password="greta2023"
)

# Create a SQLAlchemy engine for connecting to the PostgreSQL database
engine = create_engine('postgresql+psycopg2://', creator=lambda: conn, echo=False)

# Call the function
insert_csv_to_postgresql()

# Record the end time
end_time = time.time()

# Calculate the difference between the two
duration = end_time - start_time

# Close the connection
conn.close()

# Display the execution time in seconds
print(f"Execution time: {duration} seconds")
