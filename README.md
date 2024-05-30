# Snowflake Data Migration Script

This script migrates data from a source Snowflake account to a target Snowflake account. 
It reads data from all tables in specified databases on the source account and writes it to corresponding tables on the target account.

## Prerequisites

- Python 3.6+
- Snowflake Python Connector
- pandas

## Installation

1. Install the required Python packages:
    ```
    pip install snowflake-connector-python pandas
    ```

2. Configure your Snowflake connection details in the script.

## Configuration

Edit the following variables in the script to match your Snowflake account details:

### Source Snowflake Configuration

```
source_account = 'source_account_identifier'
source_username = 'source_username'
source_password = 'source_password'
source_role = 'source_role'
source_schema = 'source_schema'
source_warehouse = 'source_warehouse'
databases = ["DATABASE_1", "DATABASE_2", "DATABASE_3"]
```

### Target Snowflake Configuration

```
target_account = 'target_account_identifier'
target_username = 'target_username'
target_password = 'target_password'
target_role = 'target_role'
target_schema = 'target_schema'
target_warehouse = 'target_warehouse'
databases = ["DATABASE_1", "DATABASE_2", "DATABASE_3"]
```

### Script Details
The script performs the following steps:

- Connects to the source and target Snowflake accounts.
- Iterates over the specified databases.
- For each database:
  - Fetches the list of tables.
  - For each table:
    - Reads the data from the source Snowflake into a pandas DataFrame.
    - Fetches the DDL statement for the table.
    - Creates the table in the target Snowflake using the fetched DDL statement.
    - Writes the data from the pandas DataFrame to the target Snowflake.

