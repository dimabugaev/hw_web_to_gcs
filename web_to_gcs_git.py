from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataFrame"""
    return pd.read_csv(dataset_url)


@task(log_prints=True)
def clean(df: pd.DataFrame, col_prefix: str='tpep') -> pd.DataFrame:
    """Fix dtype issues"""

    df[f"{col_prefix}_pickup_datetime"] = pd.to_datetime(df[f"{col_prefix}_pickup_datetime"])
    df[f"{col_prefix}_dropoff_datetime"] = pd.to_datetime(df[f"{col_prefix}_dropoff_datetime"])

    print(df.head(2))
    print(f"collumns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write taxi data locally as parquet file"""
    Path(f'data/{color}').mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_to_gcs(path: Path) -> None:
    """Upload data file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcsbucket")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=f"{path}", to_path=path.name)


@flow()
def web_to_gcs() -> None:
    """The main ETL function"""
    color = 'green'
    year = 2019
    month = 4
    date_column_ptefix = 'lpep'

    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    print(dataset_file)
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    print(dataset_url)

    df = fetch(dataset_url)
    df_cleaned = clean(df, date_column_ptefix)
    path = write_local(df_cleaned, color, dataset_file)
    write_to_gcs(path)

if __name__ == '__main__':
    web_to_gcs()
