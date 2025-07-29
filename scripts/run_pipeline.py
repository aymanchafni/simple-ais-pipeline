#!/usr/bin/env python3
"""
Script principal pour exécuter le pipeline de données AIS Tanger Med
Support des fichiers ZIP NOAA et données locales
Usage: python scripts/run_pipeline.py [--url URL] [--local-file FILE] [--noaa-year YEAR] [options]
"""

import argparse
import logging
import sys
import os
from pathlib import Path
import time
import pandas as pd

# Ajouter src au path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.config import Config
from src.ingestion.data_loader import AISDataLoader
from src.transformation.data_processor import AISDataProcessor
from src.storage.database import DatabaseManager
from src.analytics.statistics import StatisticsGenerator

# Configuration du logging avec format détaillé
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/pipeline.log', mode='a') if os.path.exists('logs') else logging.NullHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse les arguments de ligne de commande"""
    parser = argparse.ArgumentParser(
        description='Pipeline de données AIS Tanger Med avec support NOAA',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  # Fichier local
  python scripts/run_pipeline.py --local-file sample_data/sample_ais.csv
  
  # Données NOAA 2024
  python scripts/run_pipeline.py --noaa-year 2024 --noaa-zone "01_01"
  
  # URL directe
  python scripts/run_pipeline.py --url "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/AIS_2024_01_01.zip"
  
  # Fichier ZIP local
  python scripts/run_pipeline.py --local-file data/AIS_2024_01_01.zip
  
  # Générer seulement les statistiques
  python scripts/run_pipeline.py --generate-stats-only
        """
    )
    
    # Groupe des sources de données
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        '--url', 
        help='URL des données AIS à télécharger (CSV ou ZIP)'
    )
    source_group.add_argument(
        '--local-file', 
        help='Fichier local de données AIS (CSV ou ZIP)'
    )
    source_group.add_argument(
        '--noaa-year',
        help='Année des données NOAA à télécharger (ex: 2024)'
    )
    
    # Options NOAA spécifiques
    parser.add_argument(
        '--noaa-zone',
        help='Zone NOAA spécifique (ex: "01_01", "02_15")'
    )
    
    # Options de traitement
    parser.add_argument(
        '--skip-download', 
        action='store_true',
        help='Ignorer le téléchargement (utiliser un fichier existant)'
    )
    parser.add_argument(
        '--skip-processing', 
        action='store_true',
        help='Ignorer le traitement (charger les données brutes)'
    )
    parser.add_argument(
        '--skip-stats', 
        action='store_true',
        help='Ignorer la génération des statistiques'
    )
    parser.add_argument(
        '--generate-stats-only', 
        action='store_true',
        help='Générer uniquement les statistiques (sans traitement de données)'
    )
    
    # Options de configuration
    parser.add_argument(
        '--batch-size', 
        type=int, 
        default=1000,
        help='Taille des lots pour l\'insertion en base (défaut: 1000)'
    )
    parser.add_argument(
        '--max-records', 
        type=int,
        help='Nombre maximum d\'enregistrements à traiter'
    )
    parser.add_argument(
        '--extract-dir',
        help='Répertoire d\'extraction pour les fichiers ZIP'
    )
    parser.add_argument(
        '--verbose', '-v', 
        action='store_true',
        help='Mode verbeux (logs détaillés)'
    )
    
    return parser.parse_args()

def setup_logging(verbose: bool):
    """Configure le niveau de logging"""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Mode verbeux activé")

def validate_file(file_path: str) -> bool:
    """Valide l'existence et la lisibilité d'un fichier"""
    if not os.path.exists(file_path):
        logger.error(f"❌ Fichier introuvable: {file_path}")
        return False
    
    if not os.path.isfile(file_path):
        logger.error(f"❌ Le chemin n'est pas un fichier: {file_path}")
        return False
    
    if not os.access(file_path, os.R_OK):
        logger.error(f"❌ Fichier non lisible: {file_path}")
        return False
    
    # Vérifier la taille du fichier
    file_size = os.path.getsize(file_path)
    logger.info(f"📊 Taille du fichier: {file_size:,} bytes ({file_size / (1024*1024):.1f} MB)")
    
    if file_size == 0:
        logger.error(f"❌ Fichier vide: {file_path}")
        return False
    
    return True

def build_noaa_url(year: str, zone: str = None) -> str:
    """Construit l'URL pour les données NOAA"""
    base_url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/"
    
    if zone:
        filename = f"AIS_{year}_{zone}.zip"
    else:
        # Fichier par défaut - premier de l'année
        filename = f"AIS_{year}_01_01.zip"
    
    return f"{base_url}{filename}"

def ingest_data(args, config: Config) -> str:
    """Étape d'ingestion des données avec support ZIP"""
    logger.info("🚀 ÉTAPE 1: Ingestion des données")
    logger.info("-" * 40)
    
    loader = AISDataLoader(config)
    
    if args.local_file:
        file_path = args.local_file
        logger.info(f"📁 Utilisation du fichier local: {file_path}")
        
        if not validate_file(file_path):
            raise ValueError(f"Fichier invalide: {file_path}")
            
    elif args.noaa_year:
        # Téléchargement NOAA
        url = build_noaa_url(args.noaa_year, args.noaa_zone)
        filename = os.path.basename(url)
        file_path = f"data/{filename}"
        
        os.makedirs("data", exist_ok=True)
        
        logger.info(f"🌊 Téléchargement des données NOAA {args.noaa_year}")
        logger.info(f"Zone: {args.noaa_zone if args.noaa_zone else 'par défaut (01_01)'}")
        logger.info(f"URL: {url}")
        
        start_time = time.time()
        
        if not loader.download_ais_data(url, file_path):
            raise Exception(f"Échec du téléchargement NOAA depuis {url}")
        
        download_time = time.time() - start_time
        logger.info(f"✅ Téléchargement NOAA terminé en {download_time:.1f}s")
        
    elif args.url and not args.skip_download:
        # URL directe
        file_path = f"data/{os.path.basename(args.url)}"
        os.makedirs("data", exist_ok=True)
        
        logger.info(f"🌐 Téléchargement depuis: {args.url}")
        start_time = time.time()
        
        if not loader.download_ais_data(args.url, file_path):
            raise Exception("Échec du téléchargement")
        
        download_time = time.time() - start_time
        logger.info(f"✅ Téléchargement terminé en {download_time:.1f}s")
        
    else:
        raise ValueError("Veuillez spécifier --url, --local-file ou --noaa-year")
    
    return file_path

def process_data(file_path: str, args, config: Config):
    """Étape de traitement des données avec support ZIP"""
    logger.info("🔄 ÉTAPE 2: Traitement des données")
    logger.info("-" * 40)
    
    # Chargement (avec support ZIP automatique)
    loader = AISDataLoader(config)
    logger.info(f"📖 Chargement du fichier: {file_path}")
    
    # Indiquer si c'est un ZIP
    if file_path.lower().endswith('.zip'):
        logger.info("🗜️ Fichier ZIP détecté - extraction automatique")
    
    start_time = time.time()
    
    df = loader.load_csv_data(file_path)
    if df is None:
        raise Exception("Échec du chargement des données")
    
    
    logger.info(df.head())

    load_time = time.time() - start_time
    logger.info(f"✅ Données chargées: {len(df):,} enregistrements en {load_time:.1f}s")
    
    # Limitation optionnelle
    if args.max_records and len(df) > args.max_records:
        logger.info(f"🔒 Limitation à {args.max_records:,} enregistrements")
        df = df.head(args.max_records)
    
    # Transformation
    processor = AISDataProcessor()
    
    if not args.skip_processing:
        logger.info("🧹 Nettoyage des données...")
        start_time = time.time()
        cleaned_df = processor.clean_data(df)
        clean_time = time.time() - start_time
        
        logger.info(f"✅ Nettoyage terminé: {len(cleaned_df):,} enregistrements valides en {clean_time:.1f}s")
        logger.info(cleaned_df.head())

        
        # Calcul des métriques par navire
        logger.info("📊 Calcul des métriques par navire...")
        start_time = time.time()
        vessel_metrics = processor.calculate_vessel_metrics(cleaned_df)
        metrics_time = time.time() - start_time
        
        logger.info(f"✅ Métriques calculées pour {len(vessel_metrics):,} navires en {metrics_time:.1f}s")
        logger.info(vessel_metrics.head())
    else:
        logger.info("⏭️ Nettoyage ignoré - utilisation des données brutes")
        cleaned_df = df
        vessel_metrics = pd.DataFrame()  # Vide si pas de traitement
    
    return cleaned_df, vessel_metrics

def store_data(cleaned_df, vessel_metrics, args, config: Config):
    """Étape de stockage en base de données"""
    logger.info("💾 ÉTAPE 3: Stockage en base de données")
    logger.info("-" * 40)
    
    db_manager = DatabaseManager(config.database_url)
    
    # Créer les tables si nécessaire
    logger.info("🏗️ Vérification/création des tables...")
    db_manager.create_tables()
    
    # Sauvegarde des données AIS
    if len(cleaned_df) > 0:
        logger.info(f"💾 Sauvegarde de {len(cleaned_df):,} enregistrements AIS...")
        start_time = time.time()
        
        # Sauvegarde par lots si le dataset est gros
        batch_size = args.batch_size
        if len(cleaned_df) > batch_size:
            logger.info(f"Sauvegarde par lots de {batch_size:,}")
            for i in range(0, len(cleaned_df), batch_size):
                batch = cleaned_df.iloc[i:i+batch_size]
                db_manager.save_ais_data(batch)
                logger.info(f"  Lot {i//batch_size + 1}: {len(batch):,} enregistrements")
        else:
            db_manager.save_ais_data(cleaned_df)
        
        save_time = time.time() - start_time
        logger.info(f"✅ Données AIS sauvegardées en {save_time:.1f}s")
    
    # Sauvegarde des métriques
    if len(vessel_metrics) > 0:
        logger.info(f"📊 Sauvegarde de {len(vessel_metrics):,} métriques de navires...")
        start_time = time.time()
        db_manager.save_vessel_metrics(vessel_metrics)
        metrics_save_time = time.time() - start_time
        logger.info(f"✅ Métriques sauvegardées en {metrics_save_time:.1f}s")

def generate_statistics(config: Config):
    """Étape de génération des statistiques"""
    logger.info("📈 ÉTAPE 4: Génération des statistiques")
    logger.info("-" * 40)
    
    generator = StatisticsGenerator(config)
    start_time = time.time()
    
    report = generator.generate_comprehensive_report()
    stats_time = time.time() - start_time
    
    logger.info(f"✅ Statistiques générées en {stats_time:.1f}s")
    
    # Afficher un résumé
    time_stats = report.get('time_analysis', {})
    logger.info(f"📊 Résumé: {time_stats.get('total_moving_time', 0):.1f}h en mouvement, "
               f"{time_stats.get('total_dock_time', 0):.1f}h à quai")
    
    return report

def main():
    """Fonction principale d'exécution du pipeline"""
    logger.info("🚢 Pipeline de Données AIS Tanger Med")
    logger.info("=" * 60)
    
    try:
        # Parse des arguments
        args = parse_arguments()
        setup_logging(args.verbose)
        
        # Charger la configuration
        config = Config()
        logger.info(f"Configuration chargée:")
        logger.info(f"  - Base de données: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
        logger.info(f"  - Utilisateur: {config.DB_USER}")
        
        # Créer les répertoires nécessaires
        os.makedirs("data", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # Cas spécial: génération des statistiques seulement
        if args.generate_stats_only:
            logger.info("🔄 Mode statistiques uniquement")
            report = generate_statistics(config)
            logger.info("🎉 Pipeline terminé avec succès!")
            return
        
        # Étape 1: Ingestion
        file_path = ingest_data(args, config)
        
        # Étape 2: Traitement
        cleaned_df, vessel_metrics = process_data(file_path, args, config)
        
        # Étape 3: Stockage
        store_data(cleaned_df, vessel_metrics, args, config)
        
        # Étape 4: Statistiques (optionnel)
        if not args.skip_stats:
            report = generate_statistics(config)
        
        # Nettoyage optionnel des fichiers temporaires
        if args.extract_dir and os.path.exists(args.extract_dir):
            logger.info(f"🧹 Nettoyage du répertoire temporaire: {args.extract_dir}")
            import shutil
            shutil.rmtree(args.extract_dir)
        
        logger.info("=" * 60)
        logger.info("🎉 Pipeline exécuté avec succès!")
        logger.info("💡 Prochaines étapes:")
        logger.info("  - Démarrer l'API: python -m uvicorn src.api.main:app --reload")
        logger.info("  - Accéder au dashboard: http://localhost:8501")
        logger.info("  - Tester l'API: curl http://localhost:8000/statistics")
        
    except KeyboardInterrupt:
        logger.info("❌ Interruption par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'exécution du pipeline: {e}")
        if args.verbose:
            import traceback
            logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()