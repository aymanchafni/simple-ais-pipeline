#!/usr/bin/env python3
"""
Script d'initialisation de la base de données
"""

import sys
import logging
from pathlib import Path

# Ajouter src au path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.config import Config
from src.storage.database import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Initialise la base de données avec les tables nécessaires"""
    try:
        config = Config()
        db_manager = DatabaseManager(config.database_url)
        
        logger.info("Création des tables...")
        db_manager.create_tables()
        logger.info("Tables créées avec succès!")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 