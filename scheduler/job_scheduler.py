# job_scheduler.py
import os
import logging
from pocketbase.client import ClientResponseError
from pocketbase import PocketBase
from scheduler.apscheduler_config import create_scheduler
from data_collection.data_inserter import insert_data
from data_processing.cleaner import clean_all_data
from data_processing.transformer import transform_all_data
from datetime import datetime
from dotenv import load_dotenv
import requests

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
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

# Check if environment variables are loaded
required_vars = ['GITHUB_TOKEN', 'POCKETBASE_URL', 'POCKETBASE_EMAIL', 'POCKETBASE_PASSWORD']
for var in required_vars:
    if not os.getenv(var):
        raise EnvironmentError(f"{var} is not set in the environment or .env file")

def authenticate_pocketbase():
    try:
        # Authenticate as an admin user
        admin_user = pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        logging.info("Successfully authenticated with PocketBase")
    except ClientResponseError as e:
        logging.error(f"Failed to authenticate with PocketBase: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during PocketBase authentication: {e}")
        raise

def scheduled_data_collection_and_processing(owner, repo):
    """
    The job function that will be scheduled to run at regular intervals.
    This function will collect data from GitHub, insert it into PocketBase,
    and then clean and transform the data for further use.
    """
    try:
        logging.info(f"Starting data collection for repository: {owner}/{repo}")
        authenticate_pocketbase()
        logging.info(f"Calling insert_data for {owner}/{repo}")
        insert_data(owner, repo)
        logging.info(f"Data collection completed for repository: {owner}/{repo}")

        # After data insertion, clean and transform the data
        logging.info(f"Starting data processing for repository: {owner}/{repo}")
        repo_clean, issues_clean, pr_clean = clean_all_data()
        repo_transformed, issues_transformed, pr_transformed = transform_all_data(repo_clean, issues_clean, pr_clean)
        logging.info(f"Data processing completed for repository: {owner}/{repo}")

    except Exception as e:
        logging.error(f"Error during data collection and processing for {owner}/{repo}: {e}")
        logging.exception("Traceback:")

def start_scheduler():
    """
    Starts the APScheduler with the defined jobs.
    """
    scheduler = create_scheduler()

    # Define the repositories you want to schedule data collection for
    repositories = [
        {'owner': 'numpy', 'repo': 'numpy'},
        {'owner': 'keras-team', 'repo': 'keras'},
        {'owner': 'pallets', 'repo': 'flask'},
        {'owner': 'django', 'repo': 'django'},
        {'owner': 'ansible', 'repo': 'ansible'},
        {'owner': 'hashicorp', 'repo': 'terraform'},
        {'owner': 'kubernetes', 'repo': 'kubernetes'},
        {'owner': 'grafana', 'repo': 'grafana'},
        {'owner': 'prometheus', 'repo': 'prometheus'},
        {'owner': 'helm', 'repo': 'helm'},
        {'owner': 'apache', 'repo': 'kafka'},
        {'owner': 'apache', 'repo': 'airflow'},
        {'owner': 'openai', 'repo': 'gym'},
        {'owner': 'huggingface', 'repo': 'transformers'},
        {'owner': 'tiangolo', 'repo': 'fastapi'},
        {'owner': 'pytorch', 'repo': 'pytorch'},
        {'owner': 'vercel', 'repo': 'next.js'},
        {'owner': 'gatsbyjs', 'repo': 'gatsby'},
        {'owner': 'vuejs', 'repo': 'vue'},
        {'owner': 'angular', 'repo': 'angular'},
        {'owner': 'sveltejs', 'repo': 'svelte'},
        {'owner': 'electron', 'repo': 'electron'},
        {'owner': 'denoland', 'repo': 'deno'},
        {'owner': 'rust-lang', 'repo': 'rust'},
        {'owner': 'golang', 'repo': 'go'},
        {'owner': 'dotnet', 'repo': 'runtime'},
        {'owner': 'flutter', 'repo': 'flutter'},
        {'owner': 'ionic-team', 'repo': 'ionic-framework'},
        {'owner': 'expo', 'repo': 'expo'},
        {'owner': 'facebook', 'repo': 'react-native'},
        {'owner': 'apache', 'repo': 'hadoop'},
        {'owner': 'apache', 'repo': 'cassandra'},
        {'owner': 'apache', 'repo': 'hbase'},
        {'owner': 'apache', 'repo': 'zookeeper'},
        {'owner': 'apache', 'repo': 'nifi'},
        {'owner': 'apache', 'repo': 'flink'},
        {'owner': 'apache', 'repo': 'beam'},
        {'owner': 'apache', 'repo': 'druid'},
        {'owner': 'apache', 'repo': 'ignite'},
        {'owner': 'apache', 'repo': 'storm'},
        {'owner': 'apache', 'repo': 'kylin'},
        {'owner': 'apache', 'repo': 'phoenix'},
        {'owner': 'apache', 'repo': 'arrow'},
        {'owner': 'apache', 'repo': 'parquet'},
        {'owner': 'apache', 'repo': 'avro'},
        {'owner': 'apache', 'repo': 'orc'},
        {'owner': 'apache', 'repo': 'iceberg'},
        {'owner': 'apache', 'repo': 'carbondata'}
    ]

    # Schedule the job for each repository
    for repo in repositories:
        owner = repo['owner']
        repository = repo['repo']
        scheduler.add_job(
            scheduled_data_collection_and_processing,
            'interval',  # Schedule to run at intervals
            minutes=10,  # Adjust this interval as needed
            args=[owner, repository],
            id=f"{owner}_{repository}_job",  # Unique job ID
            next_run_time=datetime.now()  # Start immediately
        )

    # Start the scheduler
    scheduler.start()
    logging.info("Scheduler started. Jobs have been scheduled.")

    # Keep the script running
    try:
        while True:
            pass  # Replace with more sophisticated logic if needed
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Scheduler shut down.")


if __name__ == "__main__":
    start_scheduler()