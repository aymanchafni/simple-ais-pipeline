#!/bin/bash
set -e

echo "🚢 Démarrage du pipeline Tanger Med..."

# Attendre que PostgreSQL soit prêt
echo "Attente de PostgreSQL..."
while ! pg_isready -h $DB_HOST -p $DB_PORT -U $DB_USER; do
  echo "PostgreSQL n'est pas encore prêt, attente..."
  sleep 2
done

echo "✅ PostgreSQL est prêt!"

# Initialiser la base de données
echo "🗄️ Initialisation de la base de données..."
python scripts/init_db.py

# Exécuter le pipeline avec les données d'exemple
echo "📊 Chargement des données d'exemple..."
if [ -f "sample_data/sample_ais.csv" ]; then
    python scripts/run_pipeline.py --noaa-year 2024 --noaa-zone "01_01" --max-records 100000 --verbose
    
    #python scripts/run_pipeline.py --local-file sample_data/sample_ais.csv --max-records 100 --verbose 
    echo "✅ Données d'exemple chargées!"
else
    echo "⚠️ Fichier de données d'exemple non trouvé, démarrage sans données"
fi

# Démarrer l'application selon le service
echo "🚀 Démarrage de l'application..."
exec "$@"