"""
Main ETL Pipeline for Transaction Processing - Phase 1 (MinIO Integrated)

This script runs continuously, generating fake transactions every minute,
uploading them to a MinIO Data Lake, and processing them through a pipeline.

TODO: Complete the following functions:
1. clean_data() - Clean and validate the raw transaction data
2. detect_suspicious_transactions() - Identify potentially fraudulent transactions
"""

import time
import pandas as pd
import boto3
from datetime import datetime
from pathlib import Path
from botocore.exceptions import ClientError
from scripts.generate_transactions import generate_transactions


# Configuration
TRANSACTIONS_FOLDER = Path("./transactions")
PROCESSED_FOLDER = Path("./processed")
SUSPICIOUS_FOLDER = Path("./suspicious")
INTERVAL_SECONDS = 60  # Generate transactions every 1 minute
TRANSACTIONS_PER_BATCH = 100  # Number of transactions to generate each time

# MinIO / S3 Configuration
BUCKET_NAME = "datalake"
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
)


def setup_folders():
    """Create necessary folders if they don't exist"""
    TRANSACTIONS_FOLDER.mkdir(exist_ok=True)
    PROCESSED_FOLDER.mkdir(exist_ok=True)
    SUSPICIOUS_FOLDER.mkdir(exist_ok=True)
    print(f"Folders initialized:")
    print(f"  - Data Lake (Staging): {TRANSACTIONS_FOLDER}")
    print(f"  - Processed: {PROCESSED_FOLDER}")
    print(f"  - Suspicious: {SUSPICIOUS_FOLDER}")


def setup_minio():
    """Validate MinIO connection and ensure the bucket exists"""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"S3 Storage: Bucket '{BUCKET_NAME}' verified.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            print(f"S3 Storage: Bucket '{BUCKET_NAME}' created successfully.")
        else:
            print(f"S3 Connection Error: {e}")
            raise


def generate_batch():
    """Generate a batch of fake transactions, save to data lake (MinIO) and cleanup staging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = TRANSACTIONS_FOLDER / f"transactions_{timestamp}.csv"

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Generating {TRANSACTIONS_PER_BATCH} transactions...")
    df = generate_transactions(TRANSACTIONS_PER_BATCH)

    # Save locally first (Staging)
    df.to_csv(filename, index=False)
    print(f"Temporary staging file created: {filename}")

    # Sync with MinIO Data Lake
    try:
        object_name = f"raw/transactions/{filename.name}"
        s3_client.upload_file(str(filename.resolve()), BUCKET_NAME, object_name)
        print(f"Data Lake: Uploaded s3://{BUCKET_NAME}/{object_name}")

        # Cleanup: Remove local redundancy after successful upload
        if filename.exists():
            filename.unlink()
            print(f"Staging cleaned: Local file removed.")

    except Exception as e:
        print(f"Data Lake Error: Sync failed, local file preserved: {e}")

    return df


def clean_data(df):
    """
    TODO: Implement data cleaning logic

    Clean and validate the transaction data. Consider:
    - Handling missing values
    - Removing duplicates
    - Data type validation
    - Standardizing formats
    - Handling outliers

    Args:
        df (pd.DataFrame): Raw transaction data

    Returns:
        pd.DataFrame: Cleaned transaction data
    """
    # For now, return the original DF to maintain flow
    # df_clean = df.copy()
    return df


def detect_suspicious_transactions(df):
    """
    TODO: Implement fraud detection logic

    Identify suspicious transactions based on various criteria. Consider:
    - Unusually high amounts
    - Multiple failed attempts
    - High-risk countries or merchants

    Args:
        df (pd.DataFrame): Cleaned transaction data

    Returns:
        tuple: (normal_df, suspicious_df) - DataFrames split by suspicion status
    """
    # For now, return empty suspicious DF to maintain flow
    return df, pd.DataFrame()


def process_batch(df_raw):
    """
    Process a batch of transactions through the ETL pipeline

    Args:
        df_raw (pd.DataFrame): Raw transaction data in memory
    """
    try:
        print(f"Loaded {len(df_raw)} transactions")

        # Step 1: Clean the data
        print("Cleaning data...")
        df_clean = clean_data(df_raw)
        print(f"Cleaned {len(df_clean)} transactions")

        # Step 2: Detect suspicious transactions
        print("Detecting suspicious transactions...")
        df_normal, df_suspicious = detect_suspicious_transactions(df_clean)
        print(f"Found {len(df_suspicious)} suspicious transactions")
        print(f"Found {len(df_normal)} normal transactions")

        # Save processed results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if len(df_normal) > 0:
            normal_file = PROCESSED_FOLDER / f"processed_{timestamp}.csv"
            df_normal.to_csv(normal_file, index=False)
            print(f"Saved normal transactions to: {normal_file}")

        if len(df_suspicious) > 0:
            suspicious_file = SUSPICIOUS_FOLDER / f"suspicious_{timestamp}.csv"
            df_suspicious.to_csv(suspicious_file, index=False)
            print(f"WARNING: Saved suspicious transactions to: {suspicious_file}")

        print(f"Batch processing completed successfully")

    except NotImplementedError as e:
        print(f"WARNING: Skipping processing: {e}")
    except Exception as e:
        print(f"ERROR: Error processing batch: {e}")


def main():
    """Main loop - generates and processes transactions every minute"""
    print("="*60)
    print("Transaction Processing Pipeline (MinIO Storage)")
    print("="*60)

    setup_folders()

    try:
        setup_minio()
    except Exception:
        print("Failed to initialize MinIO. Exiting...")
        return

    print(f"\nStarting continuous processing (every {INTERVAL_SECONDS} seconds)")
    print("Press Ctrl+C to stop\n")

    batch_count = 0

    try:
        while True:
            batch_count += 1
            print(f"\n{'='*60}")
            print(f"BATCH #{batch_count}")
            print(f"{'='*60}")

            # Generate new transactions and sync with S3
            df_to_process = generate_batch()

            # Process the batch directly in memory
            process_batch(df_to_process)

            # Wait for next interval
            print(f"\nWaiting {INTERVAL_SECONDS} seconds until next batch...")
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n\nPipeline stopped by user")
        print(f"Total batches processed: {batch_count}")


if __name__ == "__main__":
    main()