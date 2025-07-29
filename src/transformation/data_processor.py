import pandas as pd
import numpy as np
from datetime import datetime
import logging
from geopy.distance import geodesic

logger = logging.getLogger(__name__)

class AISDataProcessor:
    def __init__(self):
        self.valid_speed_range = (0, 50)  # Vitesse réaliste en nœuds
        self.valid_lat_range = (-90, 90)
        self.valid_lon_range = (-180, 180)
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie et valide les données AIS"""
        logger.info("Début du nettoyage des données")
        initial_count = len(df)
        
        # Conversion des types
        df['BaseDateTime'] = pd.to_datetime(df['BaseDateTime'], errors='coerce')
        df['LAT'] = pd.to_numeric(df['LAT'], errors='coerce')
        df['LON'] = pd.to_numeric(df['LON'], errors='coerce')
        df['SOG'] = pd.to_numeric(df['SOG'], errors='coerce')
        df['COG'] = pd.to_numeric(df['COG'], errors='coerce')
        
        # Filtrage des valeurs invalides
        df = df.dropna(subset=['BaseDateTime', 'LAT', 'LON', 'MMSI'])
        
        # Validation des coordonnées
        df = df[
            (df['LAT'].between(*self.valid_lat_range)) &
            (df['LON'].between(*self.valid_lon_range))
        ]
        
        # Validation de la vitesse
        df = df[df['SOG'].between(*self.valid_speed_range)]
        
        # Suppression des doublons
        df = df.drop_duplicates(subset=['MMSI', 'BaseDateTime'])
        
        final_count = len(df)
        logger.info(f"Nettoyage terminé: {initial_count} → {final_count} lignes")
        
        return df.sort_values(['MMSI', 'BaseDateTime'])
    
    def calculate_vessel_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcule les métriques par navire"""
        logger.info("Calcul des métriques par navire")
        
        vessel_metrics = []
        
        for mmsi, vessel_data in df.groupby('MMSI'):
            vessel_data = vessel_data.sort_values('BaseDateTime')
            
            # Calcul de la distance totale
            total_distance = self._calculate_total_distance(vessel_data)
            
            # Temps total et temps en mouvement
            time_metrics = self._calculate_time_metrics(vessel_data)
            
            # Nombre de points
            point_count = len(vessel_data)
            
            metrics = {
                'mmsi': mmsi,
                'vessel_name': vessel_data['VesselName'].iloc[0] if pd.notna(vessel_data['VesselName'].iloc[0]) else 'Unknown',
                'total_distance_nm': total_distance,
                'total_time_hours': time_metrics['total_time'],
                'moving_time_hours': time_metrics['moving_time'],
                'at_dock_time_hours': time_metrics['at_dock_time'],
                'point_count': point_count,
                'avg_speed_knots': vessel_data['SOG'].mean(),
                'max_speed_knots': vessel_data['SOG'].max()
            }
            
            vessel_metrics.append(metrics)
        
        return pd.DataFrame(vessel_metrics)
    
    def _calculate_total_distance(self, vessel_data: pd.DataFrame) -> float:
        """Calcule la distance totale parcourue par un navire"""
        if len(vessel_data) < 2:
            return 0.0
        
        total_distance = 0.0
        for i in range(1, len(vessel_data)):
            try:
                point1 = (vessel_data.iloc[i-1]['LAT'], vessel_data.iloc[i-1]['LON'])
                point2 = (vessel_data.iloc[i]['LAT'], vessel_data.iloc[i]['LON'])
                distance = geodesic(point1, point2).nautical
                total_distance += distance
            except:
                continue
        
        return total_distance
    
    def _calculate_time_metrics(self, vessel_data: pd.DataFrame) -> dict:
        """Calcule les métriques temporelles"""
        if len(vessel_data) < 2:
            return {'total_time': 0, 'moving_time': 0, 'at_dock_time': 0}
        
        # Temps total
        total_time = (vessel_data['BaseDateTime'].max() - vessel_data['BaseDateTime'].min()).total_seconds() / 3600
        
        # Seuil pour considérer qu'un navire est en mouvement (>= 1 nœud)
        moving_threshold = 1.0
        
        moving_time = 0.0
        for i in range(1, len(vessel_data)):
            time_diff = (vessel_data.iloc[i]['BaseDateTime'] - vessel_data.iloc[i-1]['BaseDateTime']).total_seconds() / 3600
            avg_speed = (vessel_data.iloc[i]['SOG'] + vessel_data.iloc[i-1]['SOG']) / 2
            
            if avg_speed >= moving_threshold:
                moving_time += time_diff
        
        at_dock_time = total_time - moving_time
        
        return {
            'total_time': total_time,
            'moving_time': moving_time,
            'at_dock_time': at_dock_time
        }