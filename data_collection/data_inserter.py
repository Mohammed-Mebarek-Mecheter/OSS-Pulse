# data_inserter.py
import os
import logging
from dotenv import load_dotenv
from pocketbase import PocketBase
from pocketbase.client import ClientResponseError
from data_collection.github_api import fetch_and_process_data

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


def insert_repository_data(repo_data):
    try:
        filter_query = f"full_name = '{repo_data['full_name']}'"
        existing_record = pb.collection("repositories").get_list(1, 1, {"filter": filter_query})

        # Remove 'id' from repo_data
        if 'id' in repo_data:
            del repo_data['id']

        if existing_record.items:
            result = pb.collection("repositories").update(existing_record.items[0].id, repo_data)
            logging.info(f"Updated repository data for {repo_data['full_name']}")
        else:
            result = pb.collection("repositories").create(repo_data)
            logging.info(f"Inserted repository data for {repo_data['full_name']}")

        return result.id
    except ClientResponseError as e:
        logging.error(f"PocketBase ClientResponseError in insert_repository_data: {e}")
        logging.error(f"Error details: {e.data}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in insert_repository_data: {e}")
        raise

def insert_issues_data(issues_data, repo_id, batch_size=100):
    for i in range(0, len(issues_data), batch_size):
        batch = issues_data[i:i + batch_size]
        for issue in batch:
            issue_data = {
                "number": issue["number"],
                "title": issue["title"],
                "state": issue["state"],
                "created_at": issue["created_at"],
                "updated_at": issue["updated_at"],
                "closed_at": issue.get("closed_at"),
                "repository": repo_id
            }
            try:
                pb.collection("issues").create(issue_data)
            except ClientResponseError as e:
                logging.error(f"Error inserting issue {issue['number']} for repository ID {repo_id}: {e}")
        logging.info(f"Inserted {len(batch)} issues for repository ID {repo_id}")

def insert_pull_requests_data(prs_data, repo_id, batch_size=100):
    for i in range(0, len(prs_data), batch_size):
        batch = prs_data[i:i + batch_size]
        for pr in batch:
            pr_data = {
                "number": pr["number"],
                "title": pr["title"],
                "state": pr["state"],
                "created_at": pr["created_at"],
                "updated_at": pr["updated_at"],
                "closed_at": pr.get("closed_at"),
                "merged_at": pr.get("merged_at"),
                "repository": repo_id
            }
            try:
                pb.collection("pull_requests").create(pr_data)
            except ClientResponseError as e:
                logging.error(f"Error inserting pull request {pr['number']} for repository ID {repo_id}: {e}")
        logging.info(f"Inserted {len(batch)} pull requests for repository ID {repo_id}")

def insert_data(owner, repo):
    authenticate_pocketbase()
    data = fetch_and_process_data(owner, repo)

    repo_id = insert_repository_data(data["repository"])
    insert_issues_data(data["issues"], repo_id)
    insert_pull_requests_data(data["pull_requests"], repo_id)

    logging.info(f"Authenticating with PocketBase for {owner}/{repo}")
    authenticate_pocketbase()
    logging.info(f"Fetching data from GitHub API for {owner}/{repo}")
    data = fetch_and_process_data(owner, repo)
    logging.info(f"Inserting repository data for {owner}/{repo}")
    repo_id = insert_repository_data(data["repository"])
    logging.info(f"Inserting issues data for {owner}/{repo}")
    insert_issues_data(data["issues"], repo_id)
    logging.info(f"Inserting pull requests data for {owner}/{repo}")
    insert_pull_requests_data(data["pull_requests"], repo_id)
    logging.info(f"Data insertion complete for {owner}/{repo}")

# The owner and repo parameters are no longer hardcoded
# This script will be triggered with the necessary parameters from job_scheduler.py
