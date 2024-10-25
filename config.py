# config.py
from typing import Optional
import os
from dotenv import load_dotenv

class DatabaseConfig:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        
        self.host: str = os.getenv('DB_HOST', 'localhost')
        self.port: int = int(os.getenv('DB_PORT', '5432'))
        self.dbname: str = os.getenv('DB_NAME', 'TPC-H')
        self.user: str = os.getenv('DB_USER', 'postgres')
        self.password: str = os.getenv('DB_PASSWORD', '')

    def get_connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

    @staticmethod
    def validate_config() -> tuple[bool, Optional[str]]:
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            return False, f"Missing environment variables: {', '.join(missing_vars)}"
        return True, None