import os
from dotenv import load_dotenv
from pocketbase import PocketBase
import pandas as pd
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PocketBase configuration
POCKETBASE_URL = os.getenv("POCKETBASE_URL")
POCKETBASE_EMAIL = os.getenv("POCKETBASE_EMAIL")
POCKETBASE_PASSWORD = os.getenv("POCKETBASE_PASSWORD")

# Initialize PocketBase client
pb = PocketBase(POCKETBASE_URL)


def authenticate_pocketbase():
    """Authenticate with PocketBase using admin credentials."""
    try:
        pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        logging.info("Successfully authenticated with PocketBase")
    except Exception as e:
        logging.error(f"Failed to authenticate with PocketBase: {e}")
        raise


def fetch_all_records(collection_name):
    """Fetch all records from a PocketBase collection."""
    try:
        records = pb.collection(collection_name).get_full_list(query_params={"$autoCancel": False})
        return records
    except Exception as e:
        logging.error(f"Error fetching records from {collection_name}: {e}")
        return []


def identify_duplicates(records, key_fields):
    """Identify duplicate records based on specified key fields."""
    df = pd.DataFrame([{k: v for k, v in record.__dict__.items() if not k.startswith('_')} for record in records])
    duplicates = df[df.duplicated(subset=key_fields, keep=False)].sort_values(by=key_fields)
    return duplicates


def delete_duplicate_records(collection_name, duplicates, key_fields):
    """Delete duplicate records, keeping the first occurrence."""
    try:
        for _, group in duplicates.groupby(key_fields):
            # Sort by created date and keep the oldest record
            group_sorted = group.sort_values('created')
            records_to_delete = group_sorted.iloc[1:]

            for _, record in records_to_delete.iterrows():
                pb.collection(collection_name).delete(record['id'])
                logging.info(f"Deleted duplicate record with ID {record['id']} from {collection_name}")
    except Exception as e:
        logging.error(f"Error deleting duplicate records: {e}")


def deduplicate_collection(collection_name, key_fields):
    """Deduplicate a PocketBase collection based on specified key fields."""
    authenticate_pocketbase()
    records = fetch_all_records(collection_name)
    if not records:
        logging.warning(f"No records found in {collection_name}")
        return

    duplicates = identify_duplicates(records, key_fields)
    if duplicates.empty:
        logging.info(f"No duplicates found in {collection_name}")
        return

    logging.info(f"Found {len(duplicates)} duplicate records in {collection_name}")
    delete_duplicate_records(collection_name, duplicates, key_fields)
    logging.info(f"Deduplication completed for {collection_name}")


if __name__ == "__main__":
    # Deduplicate repositories
    deduplicate_collection("repositories", ["full_name"])

    # Deduplicate issues
    deduplicate_collection("issues", ["repository", "number"])

    # Deduplicate pull requests
    deduplicate_collection("pull_requests", ["repository", "number"])