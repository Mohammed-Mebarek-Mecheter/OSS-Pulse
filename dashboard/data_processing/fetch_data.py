# dashboard/data_processing/fetch_data.py

import pandas as pd
import os
from pocketbase import PocketBase
from pocketbase.client import ClientResponseError
from dotenv import load_dotenv
import logging
from cleaner import clean_all_data
from transformer import transform_all_data
from pocketbase_config import authenticate_pocketbase

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data_from_pocketbase(collection_name, page_size=500):
    """Fetch data from a PocketBase collection and return as a DataFrame."""
    try:
        pb = authenticate_pocketbase()
        all_records = []
        page = 1
        while True:
            records = pb.collection(collection_name).get_list(page=page, per_page=page_size, query_params={"$autoCancel": False})
            if not records.items:
                break
            all_records.extend(records.items)
            if len(records.items) < page_size:
                break
            page += 1
            logging.info(f"Fetched page {page} from {collection_name}")

        if not all_records:
            logging.warning(f"No records found in {collection_name}")
            return pd.DataFrame()

        df = pd.DataFrame([{k: v for k, v in record.__dict__.items() if not k.startswith('_')} for record in all_records])
        logging.info(f"Successfully fetched {len(df)} records from {collection_name}")
        return df
    except ClientResponseError as e:
        logging.error(f"Error fetching data from {collection_name}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Unexpected error fetching data from {collection_name}: {e}")
        return pd.DataFrame()

def fetch_and_prepare_data():
    try:
        # Fetch raw data
        logging.info("Fetching raw data from PocketBase...")
        repo_df = fetch_data_from_pocketbase('repositories')
        issues_df = fetch_data_from_pocketbase('issues')
        pr_df = fetch_data_from_pocketbase('pull_requests')

        # Log the shape of each DataFrame
        logging.info(f"Fetched data shapes: repositories: {repo_df.shape}, issues: {issues_df.shape}, pull_requests: {pr_df.shape}")

        # Check if any DataFrame is empty
        if repo_df.empty or issues_df.empty or pr_df.empty:
            logging.warning("One or more DataFrames are empty. Skipping cleaning and transformation.")
            return

        # Clean data
        repo_df, issues_df, pr_df = clean_all_data(repo_df, issues_df, pr_df)

        # Transform data for analysis
        logging.info("Transforming data for dashboard visualization...")
        repo_transformed, issues_transformed, pr_transformed = transform_all_data(repo_df, issues_df, pr_df)

        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)

        # Save transformed data to csv files
        if not repo_transformed.empty:
            try:
                repo_transformed.to_csv('data/repo_data.csv')
            except Exception as e:
                logging.error(f"Error saving repository data to csv: {e}")
                # Fallback to CSV if csv fails
                repo_transformed.to_csv('data/repo_data.csv', index=False)
                logging.info("Saved repository data to CSV as a fallback")

        if not issues_transformed.empty:
            try:
                issues_transformed.to_csv('data/issues_data.csv')
            except Exception as e:
                logging.error(f"Error saving issues data to csv: {e}")
                issues_transformed.to_csv('data/issues_data.csv', index=False)
                logging.info("Saved issues data to CSV as a fallback")

        if not pr_transformed.empty:
            try:
                pr_transformed.to_csv('data/pr_data.csv')
            except Exception as e:
                logging.error(f"Error saving pull requests data to csv: {e}")
                pr_transformed.to_csv('data/pr_data.csv', index=False)
                logging.info("Saved pull requests data to CSV as a fallback")

        logging.info("Data processing complete. Files saved in data/")

    except Exception as e:
        logging.error(f"Error fetching and preparing data: {e}")
        raise

if __name__ == "__main__":
    fetch_and_prepare_data()