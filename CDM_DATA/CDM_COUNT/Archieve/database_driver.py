import snowflake.connector
# from snowflake.connector.pandas_tools import write_pandas
from db_credential import SnowflakeCredential

class SnowflakeDatabaseDriver:
    
    def __init__(self, credential:SnowflakeCredential) -> None:
        self.credential = credential
        self.connection = None
        self.cursor = None
        
    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                account=self.credential.account,
                user=self.credential.user,
                password=self.credential.password,
                role=self.credential.role,
                warehouse=self.credential.warehouse,
                database=self.credential.database,
                schema=self.credential.schema
            )   
            self.cursor = self.connection.cursor()
        except snowflake.connector.Error as e:
            print(e)
        
    def execute(self, sql_query, params=None):
        try:
            if params:
                self.cursor.execute(sql_query, params)
            else:
                self.cursor.execute(sql_query)
            self.connection.commit()
        except snowflake.connector.Error as e:
            print(e)
    
        
    def fetchall(self, sql):
        try:    
            self.cursor.execute(sql)
            df =  self.cursor.fetch_pandas_all()
        except snowflake.connector.Error as e:
            print(e)
            
        return df
    
    def fetchone(self, sql_query):
        self.cursor.execute(sql_query)
        return self.cursor.fetchone()


    def close(self):
        self.cursor.close()
        self.connection.close()