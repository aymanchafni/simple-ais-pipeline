import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Base de données
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "tanger_med")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    
    # Sources de données
    AIS_DATA_URL = "https://hub.marinecadastre.gov/datasets/..."
    
    # Configuration API
    API_HOST = "0.0.0.0"
    API_PORT = 8000
    
    @property
    def database_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"