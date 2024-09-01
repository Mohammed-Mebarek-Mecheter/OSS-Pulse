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

def authenticate_pocketbase():
    try:
        pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        logging.info("Successfully authenticated with PocketBase")
    except ClientResponseError as e:
        logging.error(f"Failed to authenticate with PocketBase: {e}")
        raise

def insert_repository_data(repo_data):
    try:
        existing_record = pb.collection("repositories").get_first_list_item(f"full_name = '{repo_data['full_name']}'")
        if existing_record:
            # Include the 'id' field when updating an existing record
            update_data = repo_data.copy()
            update_data['id'] = str(repo_data['id'])  # Convert to string if necessary
            result = pb.collection("repositories").update(existing_record.id, update_data)
            logging.info(f"Updated repository data for {repo_data['full_name']}")
        else:
            # Include the 'id' field when creating a new record
            create_data = repo_data.copy()
            create_data['id'] = str(repo_data['id'])  # Convert to string if necessary
            result = pb.collection("repositories").create(create_data)
            logging.info(f"Inserted repository data for {repo_data['full_name']}")

        return result.id
    except ClientResponseError as e:
        logging.error(f"Error inserting/updating repository data: {e}")
        raise

def insert_issues_data(issues_data, repo_id):
    batch_data = []
    for issue in issues_data:
        issue_data = {
            "number": issue["number"],
            "title": issue["title"],
            "state": issue["state"],
            "created_at": issue["created_at"],
            "updated_at": issue["updated_at"],
            "closed_at": issue.get("closed_at"),
            "repository": repo_id
        }
        batch_data.append(issue_data)

    try:
        pb.collection("issues").create_batch(batch_data)
        logging.info(f"Inserted {len(batch_data)} issues for repository ID {repo_id}")
    except ClientResponseError as e:
        logging.error(f"Error inserting issues data: {e}")
        raise

def insert_pull_requests_data(prs_data, repo_id):
    batch_data = []
    for pr in prs_data:
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
        batch_data.append(pr_data)

    try:
        pb.collection("pull_requests").create_batch(batch_data)
        logging.info(f"Inserted {len(batch_data)} pull requests for repository ID {repo_id}")
    except ClientResponseError as e:
        logging.error(f"Error inserting pull requests data: {e}")
        raise

def insert_data(owner, repo):
    authenticate_pocketbase()
    data = fetch_and_process_data(owner, repo)

    repo_id = insert_repository_data(data["repository"])
    insert_issues_data(data["issues"], repo_id)
    insert_pull_requests_data(data["pull_requests"], repo_id)

    logging.info(f"Data insertion complete for {owner}/{repo}")

if __name__ == "__main__":
    owner = "tensorflow"
    repo = "tensorflow"
    insert_data(owner, repo)