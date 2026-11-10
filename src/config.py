import os
from datetime import date

from dotenv import load_dotenv

load_dotenv()

NOAA_BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"


def noaa_daily_url(day: date) -> str:
    """URL of the MarineCadastre/NOAA AIS file for one day (US coastal waters)."""
    return f"{NOAA_BASE_URL}/{day.year}/AIS_{day:%Y_%m_%d}.zip"


class Config:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "tanger_med")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

    DATA_DIR = os.getenv("AIS_DATA_DIR", "data")

    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))

    @property
    def database_url(self) -> str:
        return os.getenv(
            "DATABASE_URL",
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}",
        )
