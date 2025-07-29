#!/usr/bin/env python3
"""
Script pour explorer et télécharger les données AIS NOAA disponibles
Usage: python scripts/explore_noaa_data.py [--year YEAR] [--list] [--download ZONE]
"""

import requests
import argparse
import logging
import sys
import os
import re
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Ajouter src au path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.config import Config
from src.ingestion.data_loader import AISDataLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NOAADataExplorer:
    def __init__(self):
        self.base_url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_available_years(self):
        """Récupère la liste des années disponibles"""
        try:
            logger.info("🔍 Recherche des années disponibles...")
            response = self.session.get(self.base_url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher les liens vers les répertoires d'années
            years = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Matcher les années (4 chiffres)
                year_match = re.match(r'^(\d{4})/?$', href)
                if year_match:
                    years.append(year_match.group(1))
            
            years.sort(reverse=True)  # Plus récentes en premier
            logger.info(f"✅ Années trouvées: {', '.join(years)}")
            return years
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des années: {e}")
            return []
    
    def get_available_files(self, year: str):
        """Récupère la liste des fichiers disponibles pour une année"""
        try:
            year_url = f"{self.base_url}{year}/"
            logger.info(f"🔍 Exploration des données {year}...")
            logger.info(f"URL: {year_url}")
            
            response = self.session.get(year_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Chercher les fichiers ZIP AIS
            files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and 'AIS' in href:
                    file_info = self._parse_filename(href)
                    if file_info:
                        # Obtenir la taille du fichier si disponible
                        file_size = self._extract_file_size(link.parent)
                        file_info['size'] = file_size
                        file_info['url'] = urljoin(year_url, href)
                        files.append(file_info)
            
            files.sort(key=lambda x: (x.get('month', 0), x.get('day', 0)))
            logger.info(f"✅ {len(files)} fichiers trouvés pour {year}")
            return files
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'exploration de {year}: {e}")
            return []
    
    def _parse_filename(self, filename):
        """Parse le nom de fichier AIS NOAA pour extraire les informations"""
        # Format typique: AIS_2024_01_01.zip ou AIS_2024_Zone01_01.zip
        patterns = [
            r'AIS_(\d{4})_(\d{2})_(\d{2})\.zip',  # AIS_YYYY_MM_DD.zip
            r'AIS_(\d{4})_Zone(\d{2})_(\d{2})\.zip',  # AIS_YYYY_ZoneXX_YY.zip
            r'AIS_(\d{4})_(\w+)\.zip'  # AIS_YYYY_REGION.zip
        ]
        
        for pattern in patterns:
            match = re.match(pattern, filename)
            if match:
                groups = match.groups()
                
                if len(groups) == 3 and groups[1].isdigit() and groups[2].isdigit():
                    return {
                        'filename': filename,
                        'year': groups[0],
                        'month': int(groups[1]),
                        'day': int(groups[2]),
                        'zone': None,
                        'type': 'daily'
                    }
                elif len(groups) == 3 and 'Zone' in pattern:
                    return {
                        'filename': filename,
                        'year': groups[0],
                        'zone': groups[1],
                        'sequence': groups[2],
                        'type': 'zone'
                    }
                elif len(groups) == 2:
                    return {
                        'filename': filename,
                        'year': groups[0],
                        'region': groups[1],
                        'type': 'regional'
                    }
        
        return None
    
    def _extract_file_size(self, element):
        """Extrait la taille du fichier depuis l'élément HTML"""
        try:
            # Chercher la taille dans le texte de l'élément ou ses voisins
            text = element.get_text() if element else ""
            
            # Patterns pour les tailles (ex: "123.4M", "1.2G", "456K")
            size_match = re.search(r'(\d+\.?\d*)\s*([KMGT]?)B?', text, re.IGNORECASE)
            if size_match:
                value = float(size_match.group(1))
                unit = size_match.group(2).upper()
                
                multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
                if unit in multipliers:
                    value *= multipliers[unit]
                
                return int(value)
            
            return None
        except:
            return None
    
    def display_files(self, files, show_details=True):
        """Affiche la liste des fichiers de manière formatée"""
        if not files:
            logger.warning("Aucun fichier trouvé")
            return
        
        print("\n📁 Fichiers AIS NOAA disponibles:")
        print("=" * 80)
        
        total_size = 0
        
        for i, file_info in enumerate(files, 1):
            filename = file_info['filename']
            size = file_info.get('size', 0)
            
            # Formatage de la taille
            if size:
                if size > 1024**3:
                    size_str = f"{size / (1024**3):.1f} GB"
                elif size > 1024**2:
                    size_str = f"{size / (1024**2):.1f} MB"
                else:
                    size_str = f"{size / 1024:.1f} KB"
                total_size += size
            else:
                size_str = "Taille inconnue"
            
            # Informations supplémentaires selon le type
            details = ""
            if file_info['type'] == 'daily':
                details = f"Date: {file_info['year']}-{file_info['month']:02d}-{file_info['day']:02d}"
            elif file_info['type'] == 'zone':
                details = f"Zone: {file_info['zone']}, Séquence: {file_info['sequence']}"
            elif file_info['type'] == 'regional':
                details = f"Région: {file_info['region']}"
            
            print(f"{i:3d}. {filename:<30} | {size_str:>10} | {details}")
        
        print("=" * 80)
        if total_size > 0:
            if total_size > 1024**3:
                total_str = f"{total_size / (1024**3):.1f} GB"
            else:
                total_str = f"{total_size / (1024**2):.1f} MB"
            print(f"Total: {len(files)} fichiers, {total_str}")
    
    def download_file(self, file_info, output_dir="data"):
        """Télécharge un fichier spécifique"""
        try:
            config = Config()
            loader = AISDataLoader(config)
            
            url = file_info['url']
            filename = file_info['filename']
            local_path = os.path.join(output_dir, filename)
            
            logger.info(f"📥 Téléchargement de {filename}...")
            
            if loader.download_ais_data(url, local_path):
                logger.info(f"✅ Téléchargement réussi: {local_path}")
                return local_path
            else:
                logger.error(f"❌ Échec du téléchargement")
                return None
                
        except Exception as e:
            logger.error(f"❌ Erreur lors du téléchargement: {e}")
            return None

def main():
    parser = argparse.ArgumentParser(
        description='Explorateur des données AIS NOAA',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python scripts/explore_noaa_data.py --list
  python scripts/explore_noaa_data.py --year 2024
  python scripts/explore_noaa_data.py --year 2024 --download 1
  python scripts/explore_noaa_data.py --year 2024 --download-pattern "01_01"
        """
    )
    
    parser.add_argument('--year', help='Année à explorer (ex: 2024)')
    parser.add_argument('--list', action='store_true', help='Lister les années disponibles')
    parser.add_argument('--download', type=int, help='Télécharger le fichier N (numéro dans la liste)')
    parser.add_argument('--download-pattern', help='Télécharger les fichiers correspondant au pattern')
    parser.add_argument('--output-dir', default='data', help='Répertoire de téléchargement')
    parser.add_argument('--max-files', type=int, default=5, help='Nombre max de fichiers à afficher')
    
    args = parser.parse_args()
    
    explorer = NOAADataExplorer()
    
    try:
        if args.list:
            # Lister les années disponibles
            years = explorer.get_available_years()
            if years:
                print("\n📅 Années disponibles:")
                for year in years:
                    print(f"  - {year}")
            return
        
        if not args.year:
            logger.error("❌ Veuillez spécifier une année avec --year")
            return
        
        # Explorer les fichiers pour l'année
        files = explorer.get_available_files(args.year)
        
        if not files:
            logger.error(f"❌ Aucun fichier trouvé pour {args.year}")
            return
        
        # Limiter l'affichage si trop de fichiers
        display_files = files[:args.max_files] if len(files) > args.max_files else files
        explorer.display_files(display_files)
        
        if len(files) > args.max_files:
            print(f"\n... et {len(files) - args.max_files} fichiers supplémentaires")
        
        # Téléchargement
        if args.download:
            if 1 <= args.download <= len(files):
                file_to_download = files[args.download - 1]
                os.makedirs(args.output_dir, exist_ok=True)
                downloaded_path = explorer.download_file(file_to_download, args.output_dir)
                
                if downloaded_path:
                    print(f"\n🎉 Fichier téléchargé: {downloaded_path}")
                    print(f"💡 Pour traiter ce fichier:")
                    print(f"    python scripts/run_pipeline.py --local-file {downloaded_path}")
            else:
                logger.error(f"❌ Numéro invalide: {args.download} (1-{len(files)})")
        
        elif args.download_pattern:
            matching_files = [f for f in files if args.download_pattern in f['filename']]
            
            if matching_files:
                print(f"\n📥 Téléchargement de {len(matching_files)} fichier(s) correspondant à '{args.download_pattern}'")
                os.makedirs(args.output_dir, exist_ok=True)
                
                for file_info in matching_files:
                    downloaded_path = explorer.download_file(file_info, args.output_dir)
                    if downloaded_path:
                        print(f"✅ {file_info['filename']}")
            else:
                logger.error(f"❌ Aucun fichier ne correspond au pattern '{args.download_pattern}'")
        
    except KeyboardInterrupt:
        logger.info("❌ Interruption par l'utilisateur")
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")

if __name__ == "__main__":
    main()