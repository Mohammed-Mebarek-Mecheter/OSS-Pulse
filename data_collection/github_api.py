import os
import time
from datetime import datetime, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry

# Load environment variables
load_dotenv()

# GitHub API configuration
GITHUB_API_URL = "https://api.github.com"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Rate limiting: 5000 requests per hour
@sleep_and_retry
@limits(calls=5000, period=3600)
def call_github_api(url):
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()

def get_repository_data(owner, repo):
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}"
    return call_github_api(url)

def get_issues_data(owner, repo, state="all", since=None):
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/issues"
    params = {"state": state, "since": since, "per_page": 100}
    issues = []

    while url:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        issues.extend(response.json())
        url = response.links.get("next", {}).get("url")
        params = {}  # Clear params for subsequent requests

    return issues

def get_pull_requests_data(owner, repo, state="all"):
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/pulls"
    params = {"state": state, "per_page": 100}
    pull_requests = []

    while url:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        pull_requests.extend(response.json())
        url = response.links.get("next", {}).get("url")
        params = {}  # Clear params for subsequent requests

    return pull_requests

def process_repository_data(repo_data):
    return {
        "id": repo_data["id"],
        "name": repo_data["name"],
        "full_name": repo_data["full_name"],
        "description": repo_data.get("description"),  # Optional field
        "stars": repo_data["stargazers_count"],
        "forks": repo_data["forks_count"],
        "open_issues": repo_data["open_issues_count"],
        "created_at": repo_data["created_at"],
        "updated_at": repo_data["updated_at"]
    }

def process_issues_data(issues_data):
    return [
        {
            "id": issue["id"],
            "number": issue["number"],
            "title": issue["title"],
            "state": issue["state"],  # Make sure it’s 'open' or 'closed'
            "created_at": issue["created_at"],
            "updated_at": issue["updated_at"],
            "closed_at": issue.get("closed_at")  # Optional field
        }
        for issue in issues_data if "pull_request" not in issue  # Filter out PRs
    ]

def process_pull_requests_data(prs_data):
    return [
        {
            "id": pr["id"],
            "number": pr["number"],
            "title": pr["title"],
            "state": "merged" if pr.get("merged_at") else pr["state"],  # State should consider 'merged'
            "created_at": pr["created_at"],
            "updated_at": pr["updated_at"],
            "closed_at": pr.get("closed_at"),  # Optional field
            "merged_at": pr.get("merged_at")  # Optional field
        }
        for pr in prs_data
    ]

def fetch_and_process_data(owner, repo):
    repo_data = get_repository_data(owner, repo)
    processed_repo_data = process_repository_data(repo_data)

    # Fetch issues and PRs from the last 30 days
    since = (datetime.now() - timedelta(days=30)).isoformat()
    issues_data = get_issues_data(owner, repo, since=since)
    prs_data = get_pull_requests_data(owner, repo)

    processed_issues_data = process_issues_data(issues_data)
    processed_prs_data = process_pull_requests_data(prs_data)

    return {
        "repository": processed_repo_data,
        "issues": processed_issues_data,
        "pull_requests": processed_prs_data
    }

if __name__ == "__main__":
    # Example usage
    owner = "tensorflow"
    repo = "tensorflow"
    data = fetch_and_process_data(owner, repo)
    print(f"Fetched data for {owner}/{repo}")
    print(f"Repository data: {data['repository']}")
    print(f"Number of issues fetched: {len(data['issues'])}")
    print(f"Number of pull requests fetched: {len(data['pull_requests'])}")