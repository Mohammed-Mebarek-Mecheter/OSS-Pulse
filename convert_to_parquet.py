import pandas as pd
from pathlib import Path

def convert_csv_to_parquet(csv_file):
    """Convert a CSV file to Parquet format."""
    df = pd.read_csv(csv_file)
    parquet_file = csv_file.with_suffix('.parquet')
    df.to_parquet(parquet_file, index=False)
    print(f"Converted {csv_file} to {parquet_file}")

def main():
    data_dir = Path('dashboard/data_processing/data')
    csv_files = ['repo_data.csv', 'issues_data.csv', 'pr_data.csv']

    for csv_file in csv_files:
        convert_csv_to_parquet(data_dir / csv_file)

if __name__ == "__main__":
    main()
