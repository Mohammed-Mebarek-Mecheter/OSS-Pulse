# OSS-Pulse Project Documentation

## Overview

**OSS-Pulse** is a data pipeline and analytics dashboard designed to monitor and analyze key metrics from GitHub repositories. The project automates the collection of repository details, issues, and pull requests, storing this data in PocketBase for further processing. The processed data is then visualized through an interactive Streamlit dashboard, providing insights into repository activity, growth, and health.

### Key Components:
- **data_collection**: Handles the extraction of data from the GitHub API, focusing on repositories, issues, and pull requests.
- **data_processing**: Comprises two main modules: cleaning (`cleaner.py`) and transforming (`transformer.py`) the raw data into structured, insightful metrics.
- **dashboard**: A Streamlit-based web dashboard designed for visualizing the processed data, offering interactive analytics and reports.
- **scheduler**: Automates periodic data collection to keep the dataset up to date.
- **tests**: Comprehensive unit tests ensuring the reliability and accuracy of all components in the pipeline.

---

## Changes and Setup Instructions

### 1. PocketBase Collection Structure

The following collections are defined in PocketBase to store data fetched from GitHub:

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
This ensures that your PocketBase credentials and GitHub token are securely managed and easily configurable.

### 3. Code Adjustments

#### **data_inserter.py**:
- **Field Names**: Updated to match the PocketBase collection structure.
- **Data Types**: Proper handling of optional fields such as `description`, `closed_at`, and `merged_at`.
- **Relation Handling**: The `repository` field in `issues` and `pull_requests` is managed correctly as a relation to the `repositories` collection.

#### **github_api.py**:
- **Data Processing**: Processing functions (`process_repository_data`, `process_issues_data`, `process_pull_requests_data`) ensure data conforms to the PocketBase schema.
- **State Handling**: Properly maps the `state` of pull requests to include "merged" status.

#### **cleaner.py**:
- **Outlier Handling**: Robust outlier detection and handling to manage diverse repository sizes, ensuring clean and consistent data.
- **Validation**: Ensures integrity by checking the logical order of dates and correcting or flagging inconsistencies.

#### **transformer.py**:
- **Metric Calculation**: Computes key metrics such as issue resolution time and PR merge time, including normalization across repositories.
- **Categorization**: Categorizes repositories into size classes (e.g., micro, small, medium, large) based on star counts, improving analytical comparisons.
- **Normalization**: Enhances comparability by normalizing metrics like stars per fork and contributors per star.

#### **test_data_processing.py**:
- **Comprehensive Testing**: Extensive test coverage for all transformation functions, including edge cases and large datasets.
- **Mocking**: Effectively mocks PocketBase interactions to isolate tests and validate processing logic.

### 4. Running the Project

#### Step 1: Install Dependencies
Ensure all required Python packages are installed:
```bash
pip install -r requirements.txt
```

#### Step 2: Run Tests
Run the test suite to verify the setup:
```bash
pytest tests/test_data_collection.py
pytest tests/test_data_processing.py
```

#### Step 3: Data Collection and Insertion
To collect and insert data from a specific GitHub repository:
```bash
python data_collection/data_inserter.py
```
This process authenticates with PocketBase, fetches data from GitHub, and inserts the repository, issues, and pull requests data into PocketBase.

### 5. Developing the Streamlit Dashboard

#### Step 1: Set Up the Streamlit App
- Create the initial layout and UI components in `dashboard/app.py`.
- Integrate with the cleaned and transformed data from PocketBase.
- Ensure dynamic filtering and interactive visualizations are responsive and intuitive.

#### Step 2: Visualize Key Metrics
- Implement visualizations for metrics such as stars over time, issue resolution times, and PR merge times.
- Add interactivity for users to explore data by repository size, date ranges, and contributor activity.

#### Step 3: Testing and Optimization
- Test the dashboard with various data sizes and filters.
- Optimize performance, focusing on data loading, filtering, and rendering times.

### 6. Deployment Preparation

#### Step 1: Finalize Environment Configuration
- Ensure all environment variables are correctly set for deployment, especially for Streamlit and PocketBase connections.

#### Step 2: Deploy the Streamlit App
- Deploy the app to a cloud platform (e.g., Streamlit Cloud, Heroku, AWS).
- Monitor the app post-deployment to ensure stability and performance.

### 7. Troubleshooting

- **Authentication Issues**: Verify PocketBase credentials and GitHub token in the `.env` file.
- **Data Insertion Errors**: Check for schema mismatches or incorrect field types in PocketBase collections.
- **Performance Issues**: Profile the app to identify and resolve bottlenecks in data processing or visualization rendering.

---

### Conclusion

OSS-Pulse is now a robust platform for collecting, processing, and visualizing GitHub repository metrics. With the structured data pipeline integrated into PocketBase and the interactive dashboard built on Streamlit, users can gain deep insights into the health and activity of open-source projects. Regular updates and continued development will further enhance the platform’s capabilities, making it a valuable tool for developers and project managers alike.
