import os
from dotenv import load_dotenv
from pocketbase import PocketBase
from pocketbase.client import ClientResponseError
import pandas as pd
from data_collection.github_api import fetch_and_process_data

# Load environment variables
load_dotenv()

# PocketBase configuration
POCKETBASE_URL = os.getenv("POCKETBASE_URL")
POCKETBASE_EMAIL = os.getenv("POCKETBASE_EMAIL")
POCKETBASE_PASSWORD = os.getenv("POCKETBASE_PASSWORD")

# Initialize PocketBase client
pb = PocketBase(POCKETBASE_URL)

def authenticate_pocketbase():
    try:
        pb.admins.auth_with_password(POCKETBASE_EMAIL, POCKETBASE_PASSWORD)
        print("Successfully authenticated with PocketBase")
    except ClientResponseError as e:
        print(f"Failed to authenticate with PocketBase: {e}")
        raise

def insert_repository_data(repo_data):
    try:
        # Match the PocketBase field names
        repo_data = {
            "name": repo_data["name"],
            "full_name": repo_data["full_name"],
            "description": repo_data.get("description"),
            "stars": repo_data["stars"],
            "forks": repo_data["forks"],
            "open_issues": repo_data["open_issues"],
            "created_at": repo_data["created_at"],
            "updated_at": repo_data["updated_at"]
        }

        # Attempt to create the repository entry
        result = pb.collection("repositories").create(repo_data)
        print(f"Inserted repository data for {repo_data['full_name']}")
        return result.id
    except ClientResponseError as e:
        if e.status == 400 and "failed validation" in str(e):
            # Record already exists, update instead
            existing_record = pb.collection("repositories").get_first_list_item(f"full_name = '{repo_data['full_name']}'")
            result = pb.collection("repositories").update(existing_record.id, repo_data)
            print(f"Updated repository data for {repo_data['full_name']}")
            return result.id
        else:
            print(f"Error inserting repository data: {e}")
            raise

def insert_issues_data(issues_data, repo_id):
    for issue in issues_data:
        issue_data = {
            "number": issue["number"],
            "title": issue["title"],
            "state": issue["state"],
            "created_at": issue["created_at"],
            "updated_at": issue["updated_at"],
            "closed_at": issue.get("closed_at"),
            "repository": repo_id  # Relation to repositories collection
        }
        try:
            pb.collection("issues").create(issue_data)
            print(f"Inserted issue data for issue #{issue['number']}")
        except ClientResponseError as e:
            if e.status == 400 and "failed validation" in str(e):
                # Record already exists, update instead
                existing_record = pb.collection("issues").get_first_list_item(f"number = {issue['number']} && repository = '{repo_id}'")
                pb.collection("issues").update(existing_record.id, issue_data)
                print(f"Updated issue data for issue #{issue['number']}")
            else:
                print(f"Error inserting issue data: {e}")

def insert_pull_requests_data(prs_data, repo_id):
    for pr in prs_data:
        pr_data = {
            "number": pr["number"],
            "title": pr["title"],
            "state": pr["state"],
            "created_at": pr["created_at"],
            "updated_at": pr["updated_at"],
            "closed_at": pr.get("closed_at"),
            "merged_at": pr.get("merged_at"),
            "repository": repo_id  # Relation to repositories collection
        }
        try:
            pb.collection("pull_requests").create(pr_data)
            print(f"Inserted pull request data for PR #{pr['number']}")
        except ClientResponseError as e:
            if e.status == 400 and "failed validation" in str(e):
                # Record already exists, update instead
                existing_record = pb.collection("pull_requests").get_first_list_item(f"number = {pr['number']} && repository = '{repo_id}'")
                pb.collection("pull_requests").update(existing_record.id, pr_data)
                print(f"Updated pull request data for PR #{pr['number']}")
            else:
                print(f"Error inserting pull request data: {e}")

def insert_data(owner, repo):
    authenticate_pocketbase()
    data = fetch_and_process_data(owner, repo)

    repo_id = insert_repository_data(data["repository"])
    insert_issues_data(data["issues"], repo_id)
    insert_pull_requests_data(data["pull_requests"], repo_id)

    print(f"Data insertion complete for {owner}/{repo}")

if __name__ == "__main__":
    owner = "tensorflow"
    repo = "tensorflow"
    insert_data(owner, repo)
