import requests
import pandas as pd
import logging
import zipfile
import os
import tempfile
import shutil
from typing import Optional, List
from pathlib import Path
from urllib.parse import urlparse
from src.config import Config

logger = logging.getLogger(__name__)

class AISDataLoader:
    def __init__(self, config: Config):
        self.config = config
        self.supported_formats = ['.csv', '.zip', '.gz']
        
    def download_ais_data(self, url: str, local_path: str) -> bool:
        """Télécharge les données AIS depuis la source publique (CSV ou ZIP)"""
        try:
            logger.info(f"Téléchargement des données depuis {url}")
            
            # Créer le répertoire de destination si nécessaire
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Headers pour simuler un navigateur (éviter les blocages)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, stream=True, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Obtenir la taille du fichier si disponible
            total_size = int(response.headers.get('content-length', 0))
            if total_size > 0:
                logger.info(f"Taille du fichier: {total_size / (1024*1024):.1f} MB")
            
            # Télécharger avec barre de progression
            downloaded = 0
            chunk_size = 8192
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Afficher le progrès tous les 10MB
                        if downloaded % (10 * 1024 * 1024) < chunk_size and total_size > 0:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"Téléchargement: {progress:.1f}% ({downloaded / (1024*1024):.1f} MB)")
            
            final_size = os.path.getsize(local_path)
            logger.info(f"✅ Fichier téléchargé: {final_size / (1024*1024):.1f} MB dans {local_path}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur réseau lors du téléchargement: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Erreur lors du téléchargement: {e}")
            return False
    
    def extract_zip_file(self, zip_path: str, extract_dir: str = None) -> List[str]:
        """Extrait un fichier ZIP et retourne la liste des fichiers CSV extraits"""
        try:
            if extract_dir is None:
                extract_dir = os.path.dirname(zip_path)
            
            logger.info(f"Extraction du fichier ZIP: {zip_path}")
            
            extracted_files = []
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Lister les fichiers dans le ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Fichiers dans le ZIP: {len(file_list)} fichiers")
                
                for file_info in zip_ref.filelist:
                    logger.info(f"  - {file_info.filename} ({file_info.file_size / (1024*1024):.1f} MB)")
                
                # Extraire tous les fichiers
                zip_ref.extractall(extract_dir)
                
                # Identifier les fichiers CSV extraits
                for filename in file_list:
                    if filename.lower().endswith('.csv'):
                        full_path = os.path.join(extract_dir, filename)
                        if os.path.exists(full_path):
                            extracted_files.append(full_path)
                            logger.info(f"✅ Fichier CSV extrait: {filename}")
            
            if not extracted_files:
                logger.warning("⚠️ Aucun fichier CSV trouvé dans l'archive ZIP")
            
            return extracted_files
            
        except zipfile.BadZipFile:
            logger.error(f"❌ Fichier ZIP corrompu: {zip_path}")
            return []
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'extraction: {e}")
            return []
    
    def detect_ais_format(self, file_path: str) -> str:
        """Détecte le format des données AIS en analysant les en-têtes"""
        try:
            # Lire les premières lignes pour détecter le format
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_lines = [f.readline().strip() for _ in range(5)]
            
            first_line = first_lines[0].lower()
            
            # Format NOAA/MarineCadastre typique
            if 'mmsi' in first_line and 'basedatetime' in first_line:
                logger.info("Format détecté: NOAA MarineCadastre")
                return 'noaa'
            
            # Format AIS standard
            elif 'mmsi' in first_line and ('timestamp' in first_line or 'time' in first_line):
                logger.info("Format détecté: AIS Standard")
                return 'standard'
            
            # Format sans en-tête (supposer NOAA)
            else:
                logger.info("Format détecté: NOAA (sans en-tête)")
                return 'noaa_no_header'
                
        except Exception as e:
            logger.warning(f"Impossible de détecter le format: {e}, utilisation du format par défaut")
            return 'noaa'
    
    def load_csv_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """Charge les données CSV en DataFrame avec détection automatique du format"""
        try:
            logger.info(f"Chargement du fichier: {file_path}")
            
            # Vérifier si c'est un fichier ZIP
            if file_path.lower().endswith('.zip'):
                logger.info("Fichier ZIP détecté, extraction en cours...")
                extracted_files = self.extract_zip_file(file_path)
                
                if not extracted_files:
                    logger.error("Aucun fichier CSV trouvé dans le ZIP")
                    return None
                
                # Utiliser le premier fichier CSV trouvé
                file_path = extracted_files[0]
                logger.info(f"Utilisation du fichier extrait: {file_path}")
                
                # Si plusieurs fichiers, les combiner
                if len(extracted_files) > 1:
                    logger.info(f"Combinaison de {len(extracted_files)} fichiers CSV...")
                    return self._combine_csv_files(extracted_files)
            
            # Détecter le format
            ais_format = self.detect_ais_format(file_path)
            
            # Colonnes selon le format NOAA MarineCadastre
            if ais_format in ['noaa', 'noaa_no_header']:
                # Colonnes typiques NOAA 2024
                ais_columns = [
                    'MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG', 'COG', 
                    'Heading', 'VesselName', 'IMO', 'CallSign', 'VesselType',
                    'Status', 'Length', 'Width', 'Draft', 'Cargo', 'TransceiverClass'
                ]
                
                # Paramètres de lecture
                read_params = {
                    'low_memory': False,
                    'encoding': 'utf-8',
                    'on_bad_lines': 'skip'  # Ignorer les lignes mal formées
                }
                
                if ais_format == 'noaa_no_header':
                    read_params['names'] = ais_columns
                    read_params['header'] = None
                else:
                    read_params['header'] = 0
            
            else:
                # Format standard - utiliser les en-têtes existants
                read_params = {
                    'low_memory': False,
                    'encoding': 'utf-8',
                    'header': 0,
                    'on_bad_lines': 'skip'
                }
            
            # Lecture du fichier avec gestion des erreurs
            logger.info("Lecture du fichier CSV...")
            df = pd.read_csv(file_path, **read_params)
            
            logger.info(f"✅ Données chargées: {len(df):,} lignes, {len(df.columns)} colonnes")
            logger.info(f"Colonnes: {list(df.columns)}")
            
            # Vérification basique
            if len(df) == 0:
                logger.error("❌ Fichier vide")
                return None
            
            # Afficher des informations sur les données
            self._log_data_info(df)
            
            return df
            
        except pd.errors.EmptyDataError:
            logger.error(f"❌ Fichier CSV vide: {file_path}")
            return None
        except pd.errors.ParserError as e:
            logger.error(f"❌ Erreur de parsing CSV: {e}")
            return None
        except FileNotFoundError:
            logger.error(f"❌ Fichier non trouvé: {file_path}")
            return None
        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement: {e}")
            return None
    
    def _combine_csv_files(self, file_paths: List[str]) -> Optional[pd.DataFrame]:
        """Combine plusieurs fichiers CSV en un seul DataFrame"""
        try:
            dataframes = []
            total_rows = 0
            
            for file_path in file_paths:
                logger.info(f"Lecture de {os.path.basename(file_path)}...")
                df = pd.read_csv(file_path, low_memory=False, on_bad_lines='skip')
                
                if len(df) > 0:
                    dataframes.append(df)
                    total_rows += len(df)
                    logger.info(f"  - {len(df):,} lignes ajoutées")
            
            if not dataframes:
                logger.error("Aucune donnée valide trouvée dans les fichiers")
                return None
            
            # Combiner tous les DataFrames
            logger.info("Combinaison des DataFrames...")
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            logger.info(f"✅ {len(file_paths)} fichiers combinés: {total_rows:,} lignes totales")
            
            return combined_df
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la combinaison: {e}")
            return None
    
    def _log_data_info(self, df: pd.DataFrame):
        """Affiche des informations sur les données chargées"""
        try:
            logger.info("📊 Informations sur les données:")
            logger.info(f"  - Taille: {df.shape}")
            logger.info(f"  - Mémoire: {df.memory_usage(deep=True).sum() / (1024*1024):.1f} MB")
            
            # Compter les navires uniques
            if 'MMSI' in df.columns:
                unique_vessels = df['MMSI'].nunique()
                logger.info(f"  - Navires uniques: {unique_vessels:,}")
            
            # Plage de dates
            if 'BaseDateTime' in df.columns:
                try:
                    df['BaseDateTime'] = pd.to_datetime(df['BaseDateTime'], errors='coerce')
                    date_range = f"{df['BaseDateTime'].min()} à {df['BaseDateTime'].max()}"
                    logger.info(f"  - Période: {date_range}")
                except:
                    pass
            
            # Types de navires
            if 'VesselType' in df.columns:
                vessel_types = df['VesselType'].value_counts().head(5)
                logger.info(f"  - Types principaux: {dict(vessel_types)}")
                
        except Exception as e:
            logger.debug(f"Erreur lors de l'affichage des infos: {e}")
    
    def download_noaa_ais_data(self, year: str = "2024", zone: str = None) -> str:
        """Télécharge les données AIS NOAA pour une année et zone spécifiques"""
        try:
            base_url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/"
            
            if zone:
                # URL spécifique pour une zone
                filename = f"AIS_{year}_{zone}.zip"
                url = f"{base_url}{filename}"
            else:
                # Essayer de télécharger un fichier général ou le premier disponible
                filename = f"AIS_{year}_Zone01_01.zip"  # Exemple
                url = f"{base_url}{filename}"
            
            local_path = f"data/{filename}"
            
            logger.info(f"🌊 Téléchargement des données NOAA AIS {year}")
            logger.info(f"URL: {url}")
            
            if self.download_ais_data(url, local_path):
                return local_path
            
            else:
                raise Exception(f"Échec du téléchargement depuis {url}")
                
        except Exception as e:
            logger.error(f"❌ Erreur téléchargement NOAA: {e}")
            raise

# Exemple d'utilisation pour les données NOAA
def download_sample_noaa_data():
    """Fonction utilitaire pour télécharger des données NOAA d'exemple"""
    config = Config()
    loader = AISDataLoader(config)
    
    # URLs d'exemple pour 2024
    sample_urls = [
        "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/AIS_2024_01_01.zip",
        "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/AIS_2024_01_02.zip"
    ]
    
    for url in sample_urls:
        try:
            filename = os.path.basename(url)
            local_path = f"data/{filename}"
            
            if loader.download_ais_data(url, local_path):
                logger.info(f"✅ Téléchargé: {filename}")
                
                # Tester le chargement
                df = loader.load_csv_data(local_path)
                if df is not None:
                    logger.info(f"✅ Données validées: {len(df)} enregistrements")
                    return local_path
            
        except Exception as e:
            logger.warning(f"⚠️ Échec pour {url}: {e}")
            continue
    
    return None