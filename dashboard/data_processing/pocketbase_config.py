# dashboard/data_processing/pocketbase_config.py

import os
from pocketbase import PocketBase
from pocketbase.client import ClientResponseError
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# PocketBase configuration
POCKETBASE_URL = os.getenv("POCKETBASE_URL")
POCKETBASE_EMAIL = os.getenv("POCKETBASE_EMAIL")
POCKETBASE_PASSWORD = os.getenv("POCKETBASE_PASSWORD")

# Initialize PocketBase client
pb = PocketBase(POCKETBASE_URL)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def authenticate_pocketbase():
    """Authenticate with PocketBase using admin credentials."""
    try:
        # Authenticate PocketBase admin
        pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        logging.info("Successfully authenticated with PocketBase")
        return pb
    except ClientResponseError as e:
        logging.error(f"Failed to authenticate with PocketBase: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during PocketBase authentication: {e}")
        raise


if __name__ == "__main__":
    # Test the PocketBase authentication
    pb_client = authenticate_pocketbase()
    print("PocketBase authentication successful.")
