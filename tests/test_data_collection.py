import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from data_collection.github_api import (
    call_github_api,
    get_repository_data,
    get_issues_data,
    get_pull_requests_data,
    process_repository_data,
    process_issues_data,
    process_pull_requests_data,
    fetch_and_process_data
)

# Mock data consistent with GitHub API responses
MOCK_REPO_DATA = {
    "id": 1296269,
    "name": "Hello-World",
    "full_name": "octocat/Hello-World",
    "description": "This your first repo!",
    "stargazers_count": 1,
    "forks_count": 1,
    "open_issues_count": 0,
    "created_at": "2011-01-26T19:01:12Z",
    "updated_at": "2011-01-26T19:14:43Z"
}

MOCK_ISSUE_DATA = [
    {
        "id": 1,
        "number": 1347,
        "title": "Found a bug",
        "state": "open",
        "created_at": "2011-04-22T13:33:48Z",
        "updated_at": "2011-04-22T13:33:48Z",
        "closed_at": None
    }
]

MOCK_PR_DATA = [
    {
        "id": 1,
        "number": 1347,
        "title": "Amazing new feature",
        "state": "open",
        "created_at": "2011-04-22T13:33:48Z",
        "updated_at": "2011-04-22T13:33:48Z",
        "closed_at": None,
        "merged_at": None
    }
]

@patch('data_collection.github_api.requests.get')
def test_call_github_api(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"key": "value"}
    mock_get.return_value = mock_response

    result = call_github_api("https://api.github.com/test")
    assert result == {"key": "value"}
    mock_get.assert_called_once()

@patch('data_collection.github_api.call_github_api')
def test_get_repository_data(mock_call_api):
    mock_call_api.return_value = MOCK_REPO_DATA
    result = get_repository_data("octocat", "Hello-World")
    assert result == MOCK_REPO_DATA
    mock_call_api.assert_called_once_with("https://api.github.com/repos/octocat/Hello-World")

@patch('data_collection.github_api.requests.get')
def test_get_issues_data(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_ISSUE_DATA
    mock_response.links = {}
    mock_get.return_value = mock_response

    result = get_issues_data("octocat", "Hello-World")
    assert result == MOCK_ISSUE_DATA
    mock_get.assert_called_once()

@patch('data_collection.github_api.requests.get')
def test_get_pull_requests_data(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = MOCK_PR_DATA
    mock_response.links = {}
    mock_get.return_value = mock_response

    result = get_pull_requests_data("octocat", "Hello-World")
    assert result == MOCK_PR_DATA
    mock_get.assert_called_once()

def test_process_repository_data():
    processed_data = process_repository_data(MOCK_REPO_DATA)
    assert processed_data["name"] == "Hello-World"
    assert processed_data["stars"] == 1
    assert processed_data["forks"] == 1

def test_process_issues_data():
    processed_data = process_issues_data(MOCK_ISSUE_DATA)
    assert len(processed_data) == 1
    assert processed_data[0]["number"] == 1347
    assert processed_data[0]["state"] == "open"

def test_process_pull_requests_data():
    processed_data = process_pull_requests_data(MOCK_PR_DATA)
    assert len(processed_data) == 1
    assert processed_data[0]["number"] == 1347
    assert processed_data[0]["state"] == "open"

@patch('data_collection.github_api.get_repository_data')
@patch('data_collection.github_api.get_issues_data')
@patch('data_collection.github_api.get_pull_requests_data')
def test_fetch_and_process_data(mock_get_prs, mock_get_issues, mock_get_repo):
    mock_get_repo.return_value = MOCK_REPO_DATA
    mock_get_issues.return_value = MOCK_ISSUE_DATA
    mock_get_prs.return_value = MOCK_PR_DATA

    result = fetch_and_process_data("octocat", "Hello-World")
    assert "repository" in result
    assert "issues" in result
    assert "pull_requests" in result
