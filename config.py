import os
from pathlib import Path
from dotenv import load_dotenv

# 1. Load environment variables from .env file
load_dotenv()

# 2. Define Base Directories (using Path for Windows/Linux compatibility)
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "datasets"

# 3. Map your 5 Source Folders
SOURCE_FOLDERS = {
    "CORE": DATA_DIR / "CORE",
    "PRF":  DATA_DIR / "PRF",
    "ORG":  DATA_DIR / "ORG",
    "PSA":  DATA_DIR / "PSA",
    "PRN":  DATA_DIR / "PRN"
}

# 4. Database Configuration (Pulled from .env)
DB_CONFIG = {
    "driver": os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
    "server": os.getenv("DB_SERVER"),
    "database": os.getenv("DB_NAME"),
}

# 5. Medallion Layer Names
LAYERS = {
    "BRONZE": "bronze",
    "SILVER": "silver",
    "GOLD":   "gold"
}