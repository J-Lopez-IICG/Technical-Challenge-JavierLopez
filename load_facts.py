import os
import io
import logging
import pandas as pd
import boto3
from sqlalchemy import create_engine

# Configuracion de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuracion de base de datos
DB_USER = "admin"
DB_PASSWORD = "password123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "fintech_dw"
DATABASE_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Configuracion de MinIO
MINIO_URL = os.getenv("MINIO_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME = "datalake"
PREFIX = "silver/processed/"

def get_s3_client():
    """Inicializa y retorna el cliente S3 para la conexion con MinIO."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )

def get_latest_file_from_minio(s3_client):
    """
    Escanea el bucket y prefijo indicados para retornar la clave (key)
    del archivo CSV mas reciente.
    """
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)

    if 'Contents' not in response:
        logging.warning(f"No se encontraron archivos en la ruta s3://{BUCKET_NAME}/{PREFIX}")
        return None

    # Filtrar solo archivos CSV y ordenar por fecha de modificacion (descendente)
    objects = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
    if not objects:
        return None

    objects.sort(key=lambda x: x['LastModified'], reverse=True)
    latest_key = objects[0]['Key']

    return latest_key

def load_latest_fact():
    """
    Flujo principal ETL para la tabla de hechos:
    1. Extrae el archivo mas reciente de la capa Silver (processed).
    2. Transforma los datos calculando la llave dimensional de tiempo (time_key).
    3. Carga los registros en la tabla 'fact_transactions' agregando datos (append).
    """
    try:
        s3 = get_s3_client()
        latest_key = get_latest_file_from_minio(s3)

        if not latest_key:
            logging.info("No existen nuevos archivos para procesar.")
            return

        logging.info(f"Iniciando procesamiento del archivo: {latest_key}")

        # Descarga del archivo a memoria
        response = s3.get_object(Bucket=BUCKET_NAME, Key=latest_key)
        df_fact = pd.read_csv(io.BytesIO(response['Body'].read()))

        if df_fact.empty:
            logging.warning("El archivo descargado no contiene registros.")
            return

        # Generacion de la llave foranea 'time_key' para cruce con 'dim_time'
        if 'timestamp' in df_fact.columns:
            df_fact['timestamp'] = pd.to_datetime(df_fact['timestamp'])
            df_fact['time_key'] = df_fact['timestamp'].dt.strftime('%Y%m%d').astype(int)

        # Conexion y carga en el Data Warehouse (PostgreSQL)
        engine = create_engine(DATABASE_URI)

        # Uso de if_exists='append' para preservar el historial de transacciones
        df_fact.to_sql('fact_transactions', engine, if_exists='append', index=False)
        logging.info(f"Carga completada: {len(df_fact)} registros insertados en 'fact_transactions'.")

    except Exception as e:
        logging.error(f"Error critico en el procesamiento de la tabla de hechos: {str(e)}")

if __name__ == "__main__":
    load_latest_fact()