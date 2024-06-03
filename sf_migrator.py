import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import warnings
warnings.filterwarnings("ignore")

databases = ["DATABASE_1", "DATABASE_2", "DATABASE_3"]


# Source
source_account = 'source_account_identifier'
source_username = 'source_username'
source_password = 'source_password'
source_role = 'source_role'
source_schema = 'source_schema'
source_warehouse = 'source_warehouse'

# Target
target_account = 'target_account_identifier'
target_username = 'target_username'
target_password = 'target_password'
target_role = 'target_role'
target_schema = 'target_schema'
target_warehouse = 'target_warehouse'


def read_data_from_source(conn, query):
    """Read data from source Snowflake into a pandas DataFrame"""
    return pd.read_sql(query, conn)

def write_data_to_target(conn, df, table_name):
    """Write data from a pandas DataFrame to target Snowflake using write_pandas"""
    success, num_chunks, num_rows, output = write_pandas(conn, df, table_name)
    if success:
        print(f"Successfully wrote {num_rows} rows to {table_name}")
    else:
        print(f"Failed to write data to {table_name}")

for database in databases:
    print(f"Database: ",database)
    # Snowflake connection details for source and target
    source_conn_params = {
        'user': source_username,
        'password': source_password,
        'account': source_account,
        'warehouse': source_warehouse,
        'database': database,
        'schema': source_schema,
        'role':source_role
    }
    
    target_conn_params = {
        'user': target_username,
        'password': target_password,
        'account': target_account,
        'warehouse': target_warehouse,
        'database': database,
        'schema': target_schema,
        'role':target_role 
    }
    
    # Connect to Snowflake
    source_conn = snowflake.connector.connect(**source_conn_params)
    target_conn = snowflake.connector.connect(**target_conn_params)

    # Open cursor to create database in TARGET
    # cursor = target_conn.cursor()

    # cursor.execute(f"CREATE DATABASE {database};")

    schema = source_schema
    
    # Read all tables from SOURCE    
    query = f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}'"
    df = pd.read_sql(query, source_conn)
    all_tables = df['TABLE_NAME'].values
    print("\tAll tables: ",all_tables)

    
    for table_name in all_tables:
        print('\t\t\t',table_name)
        # Define the table to migrate and the query to extract data
        source_table_name = table_name
        target_table_name = table_name
        
    
        # Step 1: Read data from source Snowflake into a pandas DataFrame
        query = f"SELECT * FROM {database}.PUBLIC.\"{table_name}\""
        table_data = pd.read_sql(query, source_conn)


        query = f"select GET_DDL ('TABLE', '\"{table_name}\"');"
        table_ddl = pd.read_sql(query, source_conn)
    
    
        table_ddl = table_ddl.values[0][0]

        # Open cursor to create table in TARGET
        # cursor = target_conn.cursor()
    
        # cursor.execute(table_ddl)
    
        # Step 2: Write data from pandas DataFrame to target Snowflake
        write_data_to_target(target_conn, table_data, table_name)
        print('\t\t\tDone')

# Close connections
source_conn.close()
target_conn.close()

    
    
