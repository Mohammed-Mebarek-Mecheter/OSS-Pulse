# **OSS-Pulse Project Documentation**

## **Overview**

The OSS-Pulse project is designed to collect, process, and visualize data from GitHub repositories, focusing on repository details, issues, and pull requests. The collected data is stored in a PocketBase database, cleaned, transformed, and presented via a Streamlit dashboard for insightful visualization and analysis.

You can check out the live dashboard here: [OSS-Pulse Dashboard](https://oss-pulse.streamlit.app)


### **Key Components:**
- **data_collection**: Module responsible for fetching repository data, issues, and pull requests from GitHub.
- **data_processing**: Module that cleans and transforms the raw data for further analysis.
- **dashboard**: Streamlit-based web dashboard for visualizing the collected and processed data.
- **scheduler**: Automates the regular collection of data at defined intervals.
- **tests**: Unit tests for validating the different components of the project.

---

## **Project Structure**

Here is the updated project structure:

```
OSS-Pulse/
├── .env                         # Environment variables (GitHub token, PocketBase config)
├── .gitignore                   # Files to ignore (e.g., environment files)
├── app.py                       # Main Streamlit dashboard file
├── config.py                    # Project-wide configuration settings
├── dashboard/                   # Streamlit dashboard directory
│   ├── components/              # Dashboard UI components
│   │   ├── metrics_display.py    # Component to display metrics
│   │   ├── sidebar.py            # Sidebar filters and selections
│   │   ├── filters.py            # Advance filters and selections
│   ├── data_loader.py            # Loads data for use in the dashboard
│   ├── data_processing/          # Data cleaning and transformation
│   │   ├── data/                 # Data files
│   │   ├── cleaner.py            # Cleans the fetched raw data
│   │   ├── fetch_data.py         # Fetches data from PocketBase
│   │   ├── pocketbase_config.py  # PocketBase connection/authentication
│   │   ├── transformer.py        # Transforms cleaned data into metrics
│   ├── visualizations.py         # Contains visualization logic for charts/graphs
├── data_collection/              # Fetches data from GitHub and inserts it into PocketBase
│   ├── data_inserter.py          # Inserts data into PocketBase
│   ├── github_api.py             # Fetches data from the GitHub API
├── data_processing/              # Duplicate of cleaning and transformation (for testing)
│   ├── cleaner.py
│   ├── transformer.py
├── deduplicate_pocketbase.py     # Removes duplicates from PocketBase
├── poetry.lock                   # Poetry dependency lock file
├── pyproject.toml                # Project metadata and dependencies
├── README.md                     # Project documentation
├── requirements.txt              # Python package dependencies
├── scheduler/                    # Scheduler logic for data fetching automation
│   ├── apscheduler_config.py     # APScheduler configuration
│   ├── job_scheduler.py          # Triggers data collection jobs
```

---

## **PocketBase Setup**

### **PocketBase Collection Structure**

In PocketBase, create the following collections to store the GitHub data:

- **repositories**:
  - `id` (auto-generated)
  - `name` (text)
  - `full_name` (text, unique)
  - `description` (text, optional)
  - `stars` (number)
  - `forks` (number)
  - `open_issues` (number)
  - `created_at` (date)
  - `updated_at` (date)

- **issues**:
  - `id` (auto-generated)
  - `number` (number)
  - `title` (text)
  - `state` (select: open, closed)
  - `created_at` (date)
  - `updated_at` (date)
  - `closed_at` (date, optional)
  - `repository` (relation to repositories collection)

- **pull_requests**:
  - `id` (auto-generated)
  - `number` (number)
  - `title` (text)
  - `state` (select: open, closed, merged)
  - `created_at` (date)
  - `updated_at` (date)
  - `closed_at` (date, optional)
  - `merged_at` (date, optional)
  - `repository` (relation to repositories collection)

### **Environment Variables**

Create a `.env` file at the root of the project with the following variables:
```bash
POCKETBASE_URL="http://localhost:8090"
POCKETBASE_EMAIL="your-email@example.com"
POCKETBASE_PASSWORD="your-password"
GITHUB_TOKEN="your-github-token"
```

---

## **Data Collection Process**

### **Fetching Data**

1. **GitHub API**:
   - The `github_api.py` script fetches data from GitHub repositories, issues, and pull requests using the GitHub API.
   - Data is fetched and processed by `process_repository_data`, `process_issues_data`, and `process_pull_requests_data`.

2. **Data Insertion**:
   - The `data_inserter.py` script inserts the fetched data into PocketBase.
   - Fields like `full_name` in the repositories collection are used to check for duplicates, and data is either inserted or updated accordingly.

### **Scheduler**

- The scheduler in the `scheduler/` directory automates regular data fetching from GitHub.
- **APSscheduler** is configured in `apscheduler_config.py`, and jobs are triggered using `job_scheduler.py`.

### **Deduplication**
- The `deduplicate_pocketbase.py` script ensures there are no duplicate entries in the PocketBase collections.

---

## **Data Processing for Dashboard**

### **Cleaning Data**:
- **`cleaner.py`**:
  - Cleans data retrieved from PocketBase, removing unnecessary columns and transforming date and numeric fields.
  - Issues and PR data is cleaned separately to ensure data integrity.

### **Transforming Data**:
- **`transformer.py`**:
  - Transforms cleaned data into actionable metrics, like `issue resolution time`, `pull request merge time`, and categorizes repositories based on stars (e.g., micro, small, large).
  - Flags repositories that haven't been updated in the last six months.

### **Fetching and Saving Data**:
- **`fetch_data.py`**:
  - Fetches the cleaned and transformed data from PocketBase and saves it as CSV files (`repo_data.csv`, `issues_data.csv`, `pr_data.csv`).
  - This data is then loaded for the dashboard.

---

## **Streamlit Dashboard**

### **Overview**
- The dashboard (`app.py`) displays the collected and processed data visually, offering metrics and insights into GitHub repositories.

### **Dashboard Directory Overview**

The `dashboard` directory is the core of the OSS-Pulse project. It includes the components necessary to display, filter, and visualize GitHub repository data, as well as the logic for data processing and loading. Here's a breakdown of the key files and their responsibilities:

#### **Components Directory (`dashboard/components/`)**

- **`filters.py`**: Contains logic for advanced filtering options, allowing users to filter repositories by stars, forks, open issues, and contributors. It also supports time-period filtering, issue resolution time, and pull request merge time.
  
- **`metrics_display.py`**: Displays key metrics such as total repositories, stars, forks, open issues, and contributors. It also shows advanced metrics such as average issue resolution time, stars per fork, and contributors per repository.

- **`sidebar.py`**: Handles the sidebar user interface for filtering the dataset. It includes options for filtering by repository name, size categories, stars, forks, open issues, date range, and more. The sidebar provides a dynamic way for users to interact with the dataset and customize their view.

#### **Data Loader (`dashboard/data_loader.py`)**

- **`data_loader.py`**: This module is responsible for loading the datasets into the Streamlit dashboard. It handles caching the data for performance optimization, ensuring that data is not reloaded on every interaction, thus improving the overall performance of the app.

#### **Data Processing Directory (`dashboard/data_processing/`)**

- **`cleaner.py`**: Contains functions to clean and preprocess raw GitHub repository data. It handles missing values, data type conversion, and other transformations to prepare the dataset for analysis and visualization.
  
- **`transformer.py`**: Transforms the raw data by calculating additional metrics like `stars_per_fork`, `resolution_time_days`, and `merge_time_days`. These calculated fields enrich the dataset and provide deeper insights into repository performance.

- **`fetch_data.py`**: Includes logic for fetching and updating the GitHub repository data from external sources or databases. It ensures that the data is always up-to-date.

- **`pocketbase_config.py`**: Contains configuration details for connecting to a PocketBase instance for data storage. PocketBase can be used to store, query, and manage the repository data over time.

#### **Visualizations (`dashboard/visualizations.py`)**

- **`visualizations.py`**: This file is responsible for generating the visualizations displayed in the dashboard. It includes various plotting functions such as:
  - `plot_repository_growth`: Visualizes the growth of repositories based on stars, forks, and issues over time.
  - `plot_issue_resolution_time`: Plots issue resolution times, allowing users to compare across different repositories.
  - `plot_pull_request_merge_time`: Plots the time taken to merge pull requests, providing insights into the efficiency of repository maintenance.
  - there are other visuals

#### **Other Files**

- **`__init__.py`**: Initialization files that allow Python to recognize these directories as modules.

---

### **Data Files**

The data used in this project includes information about GitHub repositories, issues, and pull requests. It has been processed into CSV and Parquet formats for easy analysis and storage. The key datasets include:

- **`repo_data.csv` / `repo_data.parquet`**: Contains information about repositories, including stars, forks, open issues, and contributors.
- **`issues_data.csv` / `issues_data.parquet`**: Contains details about issues raised in repositories, including resolution time.
- **`pr_data.csv` / `pr_data.parquet`**: Contains metadata about pull requests, including merge time and status.

### **Data Source**

The data used in this project has been published on **Kaggle**:

[Kaggle Dataset: Open Source GitHub Repos (Stars, Issues, and PRs)](https://www.kaggle.com/datasets/mohammedmecheter/open-source-github-repos-stars-issues-and-prs)

This dataset provides insights into open-source GitHub repositories, including metrics like stars, forks, issues, pull requests, and more. It is ideal for analyzing trends, performance, and activity across different repositories.

---

## **Running the Project**

### **Step 1: Install Dependencies**
Install the necessary Python packages:
```bash
pip install -r requirements.txt
```

### **Step 2: Start PocketBase**
Run PocketBase on your local machine:
```bash
./pocketbase serve
```

### **Step 3: Insert Data**
To insert data into PocketBase:
```bash
python data_collection/data_inserter.py
```

### **Step 4: Fetch and Transform Data**
To clean and transform the fetched data:
```bash
python dashboard/data_processing/fetch_data.py
```

### **Step 5: Run the Streamlit Dashboard**
Start the Streamlit dashboard to visualize the data:
```bash
streamlit run app.py
```

---

## **Conclusion**

The OSS-Pulse project provides a robust solution for collecting, processing, and visualizing GitHub data, integrated with PocketBase as the backend. The data is regularly updated via an automated scheduler, cleaned and transformed for actionable insights, and displayed in a dynamic and interactive dashboard.

For future improvements, additional metrics, visualizations, and comparative analysis features can be added to further enhance the dashboard. Ensure that PocketBase collections match the specified structure for seamless operation.
