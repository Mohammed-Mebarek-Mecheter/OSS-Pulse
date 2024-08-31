Here’s a detailed documentation that reflects the changes made, including specific setup instructions for the PocketBase collections.

---

# OSS-Pulse Project Documentation

## Overview

The OSS-Pulse project is designed to collect data from GitHub repositories, including repository details, issues, and pull requests, and store this data in a PocketBase database for further processing and visualization.

### Key Components:
- **data_collection**: Module for collecting data from GitHub.
- **data_processing**: Module for cleaning and transforming the data.
- **dashboard**: A web-based dashboard for visualizing the collected data.
- **scheduler**: Module for scheduling data collection jobs.
- **tests**: Unit tests for the project modules.

---

## Changes and Setup Instructions

### 1. PocketBase Collection Structure

The following collections are set up in PocketBase to store GitHub data:

#### a. **repositories**
- **id** (text, auto-generated)
- **name** (text)
- **full_name** (text, unique)
- **description** (text, optional)
- **stars** (number)
- **forks** (number)
- **open_issues** (number)
- **created_at** (date)
- **updated_at** (date)

#### b. **issues**
- **id** (text, auto-generated)
- **number** (number)
- **title** (text)
- **state** (select: open, closed)
- **created_at** (date)
- **updated_at** (date)
- **closed_at** (date, optional)
- **repository** (relation to repositories collection)

#### c. **pull_requests**
- **id** (text, auto-generated)
- **number** (number)
- **title** (text)
- **state** (select: open, closed, merged)
- **created_at** (date)
- **updated_at** (date)
- **closed_at** (date, optional)
- **merged_at** (date, optional)
- **repository** (relation to repositories collection)

### 2. Setup Instructions for PocketBase

#### Step 1: Create Collections in PocketBase
1. **Login** to your PocketBase Admin UI.
2. **Create a new collection** for each of the following:
   - `repositories`
   - `issues`
   - `pull_requests`
   
3. **Add Fields** to each collection as specified above.
   - Set `full_name` in `repositories` as **unique**.
   - Set up `repository` in `issues` and `pull_requests` as a **relation** to the `repositories` collection.

#### Step 2: Set Up Environment Variables
Create a `.env` file at the root of your project with the following contents:
```bash
POCKETBASE_URL="http://localhost:8090"  # or your actual PocketBase URL
POCKETBASE_EMAIL="your-admin-email@example.com"
POCKETBASE_PASSWORD="your-admin-password"
GITHUB_TOKEN="your-github-token"
```

### 3. Code Adjustments

#### **data_inserter.py**:
- **Field Names**: Updated to match the PocketBase collection structure.
- **Data Types**: Optional fields such as `description`, `closed_at`, and `merged_at` are handled appropriately.
- **Relation Handling**: The `repository` field in `issues` and `pull_requests` collections is set correctly as a relation to the `repositories` collection.

#### **github_api.py**:
- **Data Processing**: The processing functions (`process_repository_data`, `process_issues_data`, `process_pull_requests_data`) ensure that data conforms to the PocketBase field requirements.
- **State Handling**: Adjusted `state` in `pull_requests` to correctly reflect "merged" status.

#### **test_data_collection.py**:
- **Tests Updated**: Tests were adjusted to mock PocketBase interactions properly and ensure the expected behavior of the data insertion logic.
- **Field Validations**: Ensured that all fields returned by the GitHub API are correctly processed and inserted into PocketBase.

### 4. Running the Project

#### Step 1: Install Dependencies
Ensure all required Python packages are installed by running:
```bash
pip install -r requirements.txt
```

#### Step 2: Run Tests
Run the test suite to verify that everything is set up correctly:
```bash
pytest tests/test_data_collection.py
```

#### Step 3: Insert Data Example
You can insert data from a specific GitHub repository by running:
```bash
python data_collection/data_inserter.py
```
This will authenticate with PocketBase, fetch data from GitHub, and insert the repository, issues, and pull requests data into PocketBase.

### 5. Troubleshooting

- **Authentication Issues**: Ensure your PocketBase credentials and GitHub token in the `.env` file are correct.
- **Data Insertion Failures**: Check the field names and types in PocketBase. The code assumes specific structures, so any mismatch can cause errors.

---

### Conclusion

The OSS-Pulse project is now fully integrated with PocketBase, allowing for efficient data collection from GitHub and storage in a structured database. The project has been tested and validated to work with the specified PocketBase collections. 

For further development or deployment, ensure the environment variables are properly set and that PocketBase collections match the described structure. 

Feel free to reach out for any further assistance or questions!