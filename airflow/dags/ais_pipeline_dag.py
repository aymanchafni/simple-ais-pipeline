from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Ajouter le répertoire src au PATH
sys.path.append('/opt/airflow/dags/src')

from src.ingestion.data_loader import AISDataLoader
from src.transformation.data_processor import AISDataProcessor
from src.storage.database import DatabaseManager
from src.config import Config

default_args = {
    'owner': 'tanger-med',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ais_data_pipeline',
    default_args=default_args,
    description='Pipeline de traitement des données AIS',
    schedule_interval='0 6 * * *',  # Exécution quotidienne à 6h
    catchup=False,
    tags=['ais', 'maritime', 'tanger-med'],
)

def extract_ais_data(**context):
    """Tâche d'extraction des données AIS"""
    config = Config()
    loader = AISDataLoader(config)
    
    # URL exemple - à adapter selon la source réelle
    data_url = "https://example-ais-data-source.com/latest.csv"
    local_path = f"/tmp/ais_data_{context['ds']}.csv"
    
    success = loader.download_ais_data(data_url, local_path)
    if not success:
        raise Exception("Échec du téléchargement des données AIS")
    
    return local_path

def transform_ais_data(**context):
    """Tâche de transformation des données"""
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='extract_data')
    
    config = Config()
    loader = AISDataLoader(config)
    processor = AISDataProcessor()
    
    # Chargement
    df = loader.load_csv_data(file_path)
    if df is None:
        raise Exception("Échec du chargement des données")
    
    # Transformation
    cleaned_df = processor.clean_data(df)
    vessel_metrics = processor.calculate_vessel_metrics(cleaned_df)
    
    # Sauvegarde temporaire
    cleaned_path = f"/tmp/cleaned_ais_{context['ds']}.parquet"
    metrics_path = f"/tmp/vessel_metrics_{context['ds']}.parquet"
    
    cleaned_df.to_parquet(cleaned_path)
    vessel_metrics.to_parquet(metrics_path)
    
    return {'cleaned_data': cleaned_path, 'metrics_data': metrics_path}

def load_to_database(**context):
    """Tâche de chargement en base de données"""
    ti = context['ti']
    paths = ti.xcom_pull(task_ids='transform_data')
    
    config = Config()
    db_manager = DatabaseManager(config.database_url)
    
    # Création des tables si nécessaire
    db_manager.create_tables()
    
    # Chargement des données
    import pandas as pd
    cleaned_df = pd.read_parquet(paths['cleaned_data'])
    metrics_df = pd.read_parquet(paths['metrics_data'])
    
    db_manager.save_ais_data(cleaned_df)
    db_manager.save_vessel_metrics(metrics_df)

def generate_statistics(**context):
    """Génération des statistiques"""
    from src.analytics.statistics import StatisticsGenerator
    
    config = Config()
    generator = StatisticsGenerator(config)
    report = generator.generate_comprehensive_report()
    
    # Sauvegarde du rapport
    import json
    report_path = f"/tmp/ais_report_{context['ds']}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    return report_path

# Définition des tâches
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_ais_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_ais_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_to_database,
    dag=dag,
)

stats_task = PythonOperator(
    task_id='generate_statistics',
    python_callable=generate_statistics,
    dag=dag,
)

# Nettoyage des fichiers temporaires
cleanup_task = BashOperator(
    task_id='cleanup',
    bash_command='rm -f /tmp/ais_data_{{ ds }}* /tmp/cleaned_ais_{{ ds }}* /tmp/vessel_metrics_{{ ds }}*',
    dag=dag,
)

# Définition des dépendances
extract_task >> transform_task >> load_task >> stats_task >> cleanup_task