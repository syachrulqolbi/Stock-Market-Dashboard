import pandas as pd
import json
from mysql.connector import MySQLConnection, Error

class MySQLDataConnector:
    """
    A class to manage MySQL database connections, table creation, data insertion,
    and enforcing row limits.
    """
    def __init__(self, credentials_file: str, table_name: str, primary_keys=None, max_row_key=None, sort_col=None):
        """
        Initializes the MySQLDataConnector.
        
        :param credentials_file: Path to the JSON file containing MySQL credentials.
        :param table_name: Name of the database table.
        :param primary_keys: List of primary key column(s) for the table.
        :param max_row_key: Maximum number of rows to keep in the table (optional).
        :param sort_col: Column used for sorting when enforcing max_row_key (optional).
        """
        self.table_name = table_name
        self.primary_keys = primary_keys if isinstance(primary_keys, list) else [primary_keys]
        self.max_row_key = max_row_key
        self.sort_col = sort_col
        self.connection = self._connect_to_database(credentials_file)

    def _connect_to_database(self, credentials_file: str) -> MySQLConnection:
        """Establishes a connection to the MySQL database using credentials from a JSON file."""
        try:
            with open(credentials_file, 'r') as file:
                credentials = json.load(file)
            return MySQLConnection(**credentials)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"❌ Error loading MySQL credentials: {e}")
        except Error as e:
            print(f"❌ Error connecting to MySQL: {e}")
        return None

    def create_table_if_not_exists(self, df: pd.DataFrame):
        """Creates a MySQL table if it does not already exist, based on the DataFrame's structure."""
        if self.connection is None:
            print("❌ No database connection available.")
            return

        column_definitions = [f'`{col}` VARCHAR(255)' for col in df.columns]
        
        if self.primary_keys:
            primary_key_clause = f'PRIMARY KEY ({", ".join(f"`{pk}`" for pk in self.primary_keys)})'
            column_definitions.append(primary_key_clause)
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            {', '.join(column_definitions)}
        ) ENGINE=InnoDB;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
            self.connection.commit()
        except Error as e:
            print(f"❌ Error creating table: {e}")

    def insert_or_update(self, df: pd.DataFrame):
        """Inserts data into the table, updating existing records if primary keys match."""
        if self.connection is None:
            print("❌ No database connection available.")
            return
        
        if df.empty:
            print("❌ No data to insert.")
            return
        
        self.create_table_if_not_exists(df)
        
        columns = ', '.join(f'`{col}`' for col in df.columns)
        values = ', '.join(['%s'] * len(df.columns))
        
        if self.primary_keys:
            update_clause = ', '.join(f'`{col}`=VALUES(`{col}`)' for col in df.columns if col not in self.primary_keys)
            insert_query = f"""
            INSERT INTO `{self.table_name}` ({columns})
            VALUES ({values})
            ON DUPLICATE KEY UPDATE {update_clause};
            """
        else:
            insert_query = f"""
            INSERT INTO `{self.table_name}` ({columns})
            VALUES ({values});
            """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(insert_query, df.itertuples(index=False, name=None))
            self.connection.commit()
        except Error as e:
            print(f"❌ Error inserting data: {e}")
        
        # Enforce row limit if applicable
        if self.max_row_key and self.sort_col:
            self._enforce_max_rows()

    def _enforce_max_rows(self):
        """Deletes older records to maintain max_row_key count per symbol, considering all primary keys."""
        if self.connection is None:
            print("❌ No database connection available.")
            return
        
        if not self.primary_keys or not self.sort_col:
            print("❌ Primary keys and sort_col are required to enforce max rows.")
            return

        primary_keys_str = ", ".join(f"`{pk}`" for pk in self.primary_keys)
        primary_key_conditions = " AND ".join(
            f"t.`{pk}` = sub.`{pk}`" for pk in self.primary_keys
        )

        delete_query = f"""
        DELETE t FROM `{self.table_name}` AS t
        JOIN (
            SELECT {primary_keys_str}
            FROM (
                SELECT {primary_keys_str},
                    ROW_NUMBER() OVER (PARTITION BY `Symbol` ORDER BY `{self.sort_col}` DESC) AS row_num
                FROM `{self.table_name}`
            ) AS ranked
            WHERE ranked.row_num > {self.max_row_key}
        ) AS sub
        ON {primary_key_conditions};
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(delete_query)
            self.connection.commit()
        except Error as e:
            print(f"❌ Error enforcing row limit: {e}")

    def read_table(self) -> pd.DataFrame:
        """
        Reads the entire table from the MySQL database and returns it as a Pandas DataFrame.
        
        :return: DataFrame containing the table's data.
        """
        if self.connection is None:
            print("❌ No database connection available.")
            return pd.DataFrame()
        
        query = f"SELECT * FROM `{self.table_name}`"
        
        try:
            df = pd.read_sql(query, self.connection)
            return df
        except Error as e:
            print(f"❌ Error reading table '{self.table_name}': {e}")
            return pd.DataFrame()

    def close_connection(self):
        """Closes the MySQL database connection."""
        if self.connection:
            self.connection.close()
            print("✅ Database connection closed.")
