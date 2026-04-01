import pandas as pd
from sqlalchemy import create_engine
import logging

from scripts.generate_users import generate_users
from scripts.generate_companies import generate_companies
from scripts.generate_payment_methods import generate_payment_methods

# Configuracion de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuracion de base de datos
DB_USER = "admin"
DB_PASSWORD = "password123"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "fintech_dw"
DATABASE_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def generate_time_dimension(start_date="2022-01-01", end_date="2026-12-31"):
    """
    Genera la dimension temporal (dim_time) para el modelo dimensional.
    Implementa llaves subrogadas en formato entero (YYYYMMDD).
    """
    df_time = pd.DataFrame({"full_date": pd.date_range(start_date, end_date)})

    # Llave subrogada principal
    df_time["time_key"] = df_time["full_date"].dt.strftime("%Y%m%d").astype(int)

    # Atributos dimensionales
    df_time["year"] = df_time["full_date"].dt.year
    df_time["quarter"] = df_time["full_date"].dt.quarter
    df_time["month"] = df_time["full_date"].dt.month
    df_time["day"] = df_time["full_date"].dt.day
    df_time["day_of_week"] = df_time["full_date"].dt.dayofweek + 1
    df_time["day_name"] = df_time["full_date"].dt.day_name()
    df_time["month_name"] = df_time["full_date"].dt.month_name()
    df_time["is_weekend"] = df_time["day_of_week"].isin([6, 7])

    return df_time

def load_dimensions():
    """
    Orquesta la generacion y carga de tablas dimensionales hacia PostgreSQL.
    Asegura el cumplimiento de la nomenclatura requerida.
    """
    try:
        engine = create_engine(DATABASE_URI)
        logging.info("Conexion establecida con el Data Warehouse (PostgreSQL).")

        # 1. Dimension Temporal (dim_time)
        logging.info("Generando dimension temporal...")
        df_time = generate_time_dimension()
        df_time.to_sql('dim_time', engine, if_exists='replace', index=False)
        logging.info(f"Carga exitosa: {len(df_time)} registros en 'dim_time'.")

        # 2. Dimension Usuarios (dim_users)
        logging.info("Generando dimension de usuarios...")
        df_users = generate_users(10000)
        df_users.to_sql('dim_users', engine, if_exists='replace', index=False)
        logging.info(f"Carga exitosa: {len(df_users)} registros en 'dim_users'.")

        # 3. Dimension Comercios (dim_merchants)
        logging.info("Generando dimension de comercios...")
        df_companies = generate_companies(1000)
        df_companies.to_sql('dim_merchants', engine, if_exists='replace', index=False)
        logging.info(f"Carga exitosa: {len(df_companies)} registros en 'dim_merchants'.")

        # 4. Dimension Metodos de Pago (dim_payment_methods)
        logging.info("Generando dimension de metodos de pago...")
        df_payments = generate_payment_methods(5000)
        df_payments.to_sql('dim_payment_methods', engine, if_exists='replace', index=False)
        logging.info(f"Carga exitosa: {len(df_payments)} registros en 'dim_payment_methods'.")

        logging.info("Proceso de inicializacion dimensional finalizado con exito.")

    except Exception as e:
        logging.error(f"Error durante la carga dimensional: {str(e)}")

if __name__ == "__main__":
    load_dimensions()