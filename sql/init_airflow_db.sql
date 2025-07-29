-- Script pour créer la base de données Airflow
-- Exécuté automatiquement par PostgreSQL au démarrage

-- Créer la base de données Airflow si elle n'existe pas
SELECT 'CREATE DATABASE airflow_db' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

-- Message de confirmation
DO $$ 
BEGIN
    RAISE NOTICE 'Base de données Airflow créée ou vérifiée avec succès';
END $$;