# test_data_collection.py
import os
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
from data_collection.data_inserter import (
    authenticate_pocketbase,
    insert_repository_data,
    insert_issues_data,
    insert_pull_requests_data,
    insert_data,
    pb
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

@pytest.fixture
def mock_pb():
    with patch('data_collection.data_inserter.pb') as mock:
        yield mock

def test_authenticate_pocketbase(mock_pb):
    authenticate_pocketbase()
    mock_pb.admins.auth_with_password.assert_called_once()

def test_insert_repository_data_create(mock_pb):
    mock_repo_data = process_repository_data(MOCK_REPO_DATA)
    mock_pb.collection.return_value.get_first_list_item.return_value = None
    mock_pb.collection.return_value.create.return_value.id = "mock_id"

    repo_id = insert_repository_data(mock_repo_data)

    mock_pb.collection.return_value.get_first_list_item.assert_called_once()
    mock_pb.collection.return_value.create.assert_called_once()
    create_args = mock_pb.collection.return_value.create.call_args[0][0]
    assert create_args['id'] == str(MOCK_REPO_DATA['id'])
    assert repo_id == "mock_id"

def test_insert_repository_data_update(mock_pb):
    mock_repo_data = process_repository_data(MOCK_REPO_DATA)
    mock_pb.collection.return_value.get_first_list_item.return_value = MagicMock(id="existing_id")
    mock_pb.collection.return_value.update.return_value.id = "existing_id"

    repo_id = insert_repository_data(mock_repo_data)

    mock_pb.collection.return_value.get_first_list_item.assert_called_once()
    mock_pb.collection.return_value.update.assert_called_once()
    update_args = mock_pb.collection.return_value.update.call_args[0]
    assert update_args[0] == "existing_id"
    assert update_args[1]['id'] == str(MOCK_REPO_DATA['id'])
    assert repo_id == "existing_id"

@patch('data_collection.data_inserter.pb')
def test_insert_issues_data(mock_pb):
    repo_id = "mock_repo_id"
    mock_pb.collection.return_value.create_batch.return_value = True

    insert_issues_data(MOCK_ISSUE_DATA, repo_id)

    mock_pb.collection.return_value.create_batch.assert_called_once()

@patch('data_collection.data_inserter.pb')
def test_insert_pull_requests_data(mock_pb):
    repo_id = "mock_repo_id"
    mock_pb.collection.return_value.create_batch.return_value = True

    insert_pull_requests_data(MOCK_PR_DATA, repo_id)

    mock_pb.collection.return_value.create_batch.assert_called_once()

@patch('data_collection.data_inserter.authenticate_pocketbase')
@patch('data_collection.data_inserter.fetch_and_process_data')
@patch('data_collection.data_inserter.insert_repository_data')
@patch('data_collection.data_inserter.insert_issues_data')
@patch('data_collection.data_inserter.insert_pull_requests_data')
def test_insert_data(mock_insert_pr, mock_insert_issues, mock_insert_repo, mock_fetch_data, mock_authenticate):
    mock_fetch_data.return_value = {
        "repository": MOCK_REPO_DATA,
        "issues": MOCK_ISSUE_DATA,
        "pull_requests": MOCK_PR_DATA
    }
    mock_insert_repo.return_value = "mock_repo_id"

    insert_data("octocat", "Hello-World")

    mock_authenticate.assert_called_once()
    mock_fetch_data.assert_called_once_with("octocat", "Hello-World")
    mock_insert_repo.assert_called_once_with(MOCK_REPO_DATA)
    mock_insert_issues.assert_called_once_with(MOCK_ISSUE_DATA, "mock_repo_id")
    mock_insert_pr.assert_called_once_with(MOCK_PR_DATA, "mock_repo_id")

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