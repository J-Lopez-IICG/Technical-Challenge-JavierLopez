"""
Main ETL Pipeline for Transaction Processing - Phase 2 (ETL & Fraud Detection)

This script runs continuously, generating fake transactions every minute,
uploading them to a MinIO Data Lake, and processing them through a pipeline.
"""

import time
import pandas as pd
import boto3
import requests
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
    print("Folders initialized:")
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
            print("Staging cleaned: Local file removed.")

    except Exception as e:
        print(f"Data Lake Error: Sync failed, local file preserved: {e}")

    return df


def clean_data(df):
    """
    Limpieza técnica basada en el análisis del generador.
    """
    df_clean = df.copy()

    # 1. Manejo de Nulos (Imputación lógica)
    currency_by_country = {"MX": "MXN", "BR": "BRL", "CO": "COP", "AR": "ARS", "CL": "CLP", "PE": "PEN"}
    df_clean['currency'] = df_clean['currency'].fillna(df_clean['country'].map(currency_by_country))
    df_clean['currency'] = df_clean['currency'].fillna('USD') # Fallback final

    # IP Address nula -> placeholder
    df_clean['ip_address'] = df_clean['ip_address'].fillna('0.0.0.0')

    # 2. Validación de Tipos
    df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
    df_clean['settlement_date'] = pd.to_datetime(df_clean['settlement_date'])
    df_clean['amount'] = pd.to_numeric(df_clean['amount'], errors='coerce')

    # 3. Estandarización
    df_clean['country'] = df_clean['country'].astype(str).str.upper().str.strip()

    # 4. Manejo de Outliers Técnicos
    df_clean = df_clean[(df_clean['amount'] > 0) & (df_clean['amount'] < 1000000)]

    return df_clean


def convert_to_usd(df):
    """
    Normalización de montos usando API de tipos de cambio con Timeout agresivo.
    """
    print("  -> Fetching exchange rates...")
    try:
        response = requests.get("https://open.er-api.com/v6/latest/USD", timeout=2)
        rates = response.json().get('rates', {})

        df['rate_to_usd'] = df['currency'].map(lambda x: rates.get(x, 1.0))
        df['amount_usd'] = round(df['amount'] / df['rate_to_usd'], 2)
        print("  -> API conversion successful.")

    except Exception as e:
        print("  -> API notice: Using static rates (fallback).")
        static_rates = {"MXN": 17.0, "BRL": 5.0, "COP": 3900.0, "ARS": 800.0, "CLP": 950.0, "PEN": 3.7, "USD": 1.0}
        df['amount_usd'] = df.apply(lambda x: round(x['amount'] / static_rates.get(x['currency'], 1.0), 2), axis=1)

    return df


def detect_suspicious_transactions(df):
    """
    Detección de fraude basada en reglas de negocio.
    """
    df_analysis = df.copy()

    # CRITERIO 1: Montos altos en USD
    mask_high_amount = df_analysis['amount_usd'] > 10000

    # CRITERIO 2: Múltiples intentos fallidos
    mask_reentry = (df_analysis['status'] == 'declined') & (df_analysis['attempt_number'] >= 3)

    # CRITERIO 3: Violaciones de Seguridad
    mask_security = df_analysis['response_message'].str.contains('Security violation', case=False, na=False)

    # CRITERIO 4: Transacciones internacionales de alto monto (> $500 USD)
    mask_intl_risk = (df_analysis['is_international'] == True) & (df_analysis['amount_usd'] > 500)

    # Combinar reglas
    is_suspicious = mask_high_amount | mask_reentry | mask_security | mask_intl_risk

    df_suspicious = df_analysis[is_suspicious].copy()
    df_normal = df_analysis[~is_suspicious].copy()

    return df_normal, df_suspicious


def process_batch(df_raw):
    """
    Process a batch of transactions through the ETL pipeline including
    currency normalization via API and cloud persistence.
    """
    try:
        print(f"Loaded {len(df_raw)} transactions from Data Lake")

        # Step 1: Clean the data
        print("Cleaning and standardizing data...")
        df_clean = clean_data(df_raw)

        # Step 2: Currency Conversion (Bonus Point)
        print("Normalizing currencies via ExchangeRate API...")
        df_normalized = convert_to_usd(df_clean)
        print(f"Standardized {len(df_normalized)} transactions to USD")

        # Step 3: Detect suspicious transactions
        print("Detecting suspicious transactions...")
        df_normal, df_suspicious = detect_suspicious_transactions(df_normalized)

        print("Analysis Results:")
        print(f"  - Suspicious: {len(df_suspicious)}")
        print(f"  - Normal: {len(df_normal)}")

        # Step 4: Save processed results locally AND to MinIO
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if len(df_normal) > 0:
            filename = f"processed_{timestamp}.csv"
            local_path = PROCESSED_FOLDER / filename
            df_normal.to_csv(local_path, index=False)

            # Subida a MinIO
            s3_client.upload_file(str(local_path), BUCKET_NAME, f"silver/processed/{filename}")
            print(f"Saved & Uploaded to Data Lake: silver/processed/{filename}")

            # Limpieza local
            if local_path.exists():
                local_path.unlink()
                print("Local processed file cleaned.")

        if len(df_suspicious) > 0:
            filename = f"suspicious_{timestamp}.csv"
            local_path = SUSPICIOUS_FOLDER / filename
            df_suspicious.to_csv(local_path, index=False)

            # Subida a MinIO
            s3_client.upload_file(str(local_path), BUCKET_NAME, f"silver/suspicious/{filename}")
            print(f"WARNING: Saved & Uploaded suspicious: silver/suspicious/{filename}")

            # Limpieza local
            if local_path.exists():
                local_path.unlink()
                print("Local suspicious file cleaned.")

        print("Batch processing completed successfully")

    except Exception as e:
        print(f"ERROR: Error processing batch: {e}")


def main():
    """Main loop - orchestrates generation, sync to MinIO, and ETL processing"""
    print("="*60)
    print("FINTECH DATA PIPELINE - PHASE 2: ETL & FRAUD DETECTION")
    print("="*60)

    setup_folders()

    try:
        setup_minio()
    except Exception as e:
        print(f"CRITICAL ERROR: Could not connect to MinIO/S3. {e}")
        return

    print("\nPipeline status: ACTIVE")
    print(f"Interval: {INTERVAL_SECONDS}s | Batch Size: {TRANSACTIONS_PER_BATCH}")
    print("Press Ctrl+C to stop safely\n")

    batch_count = 0

    try:
        while True:
            batch_count += 1
            print(f"\n{'#'*60}")
            print(f"PROCESSING BATCH #{batch_count}")
            print(f"{'#'*60}")

            df_to_process = generate_batch()

            if df_to_process is not None and not df_to_process.empty:
                process_batch(df_to_process)
            else:
                print("Skipping batch: No data received.")

            print(f"\nWaiting {INTERVAL_SECONDS} seconds for next sync...")
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n\n" + "!"*30)
        print("Pipeline manually stopped by user")
        print(f"Total batches processed: {batch_count}")
        print("!"*30)


if __name__ == "__main__":
    main()