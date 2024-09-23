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
        admin_admin = pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
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
        {'owner': 'google', 'repo': 'protobuf'},
        {'owner': 'docker', 'repo': 'docker-ce'},
        {'owner': 'nodejs', 'repo': 'node'},
        {'owner': 'mozilla', 'repo': 'firefox'},
        {'owner': 'torvalds', 'repo': 'linux'},
        {'owner': 'apple', 'repo': 'swift'},
        {'owner': 'microsoft', 'repo': 'vscode'},
        {'owner': 'JetBrains', 'repo': 'kotlin'},
        {'owner': 'redis', 'repo': 'redis'},
        {'owner': 'mongodb', 'repo': 'mongo'},
        {'owner': 'postgres', 'repo': 'postgres'},
        {'owner': 'npm', 'repo': 'cli'},
        {'owner': 'yarnpkg', 'repo': 'yarn'},
        {'owner': 'webpack', 'repo': 'webpack'},
        {'owner': 'babel', 'repo': 'babel'},
        {'owner': 'eslint', 'repo': 'eslint'},
        {'owner': 'prettier', 'repo': 'prettier'},
        {'owner': 'jest-community', 'repo': 'jest'},
        {'owner': 'mocha-community', 'repo': 'mocha'},
        {'owner': 'chartjs', 'repo': 'Chart.js'},
        {'owner': 'mrdoob', 'repo': 'three.js'},
        {'owner': 'moment', 'repo': 'moment'},
        {'owner': 'lodash', 'repo': 'lodash'},
        {'owner': 'axios', 'repo': 'axios'},
        {'owner': 'expressjs', 'repo': 'express'},
        {'owner': 'sequelize', 'repo': 'sequelize'},
        {'owner': 'typeorm', 'repo': 'typeorm'},
        {'owner': 'prisma', 'repo': 'prisma'},
        {'owner': 'strapi', 'repo': 'strapi'},
        {'owner': 'nestjs', 'repo': 'nest'},
        {'owner': 'spring-projects', 'repo': 'spring-boot'},
        {'owner': 'JetBrains', 'repo': 'intellij-community'},
        {'owner': 'eclipse', 'repo': 'eclipse'},
        {'owner': 'chef', 'repo': 'chef'},
        {'owner': 'puppetlabs', 'repo': 'puppet'},
        {'owner': 'saltstack', 'repo': 'salt'},
        {'owner': 'docker', 'repo': 'compose'},
        {'owner': 'istio', 'repo': 'istio'},
        {'owner': 'etcd-io', 'repo': 'etcd'},
        {'owner': 'consul', 'repo': 'consul'},
        {'owner': 'rabbitmq', 'repo': 'rabbitmq-server'},
        {'owner': 'celery', 'repo': 'celery'},
        {'owner': 'rq', 'repo': 'rq'},
        {'owner': 'scrapy', 'repo': 'scrapy'},
        {'owner': 'requests', 'repo': 'requests'},
        {'owner': 'psf', 'repo': 'requests-html'},
        {'owner': 'certbot', 'repo': 'certbot'},
        {'owner': 'aws', 'repo': 'aws-cli'},
        {'owner': 'google', 'repo': 'gson'},
        {'owner': 'google', 'repo': 'guava'}
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