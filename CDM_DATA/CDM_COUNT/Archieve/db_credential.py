import os

class SnowflakeCredential:
    def __init__(self, account: str, user=str, password=str, role=str, warehouse=str, database=str, schema=str) -> None:
        self.account = account
        self.user = user
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        
        
            
class DatabaseCredentialManager:
    
    @staticmethod
    def read_credential_from_env():
     
        return SnowflakeCredential(
            account=os.getenv('ACCOUNT'), 
            user=os.getenv('USERNAME'),
            password=os.getenv('PASSWORD'),
            role=os.getenv('ROLE'),
            warehouse=os.getenv('WAREHOUSE'),
            database=os.getenv('DATABASE'),
            schema='I2B2METADATA'    
        )