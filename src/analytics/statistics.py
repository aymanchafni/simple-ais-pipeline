import pandas as pd
from sqlalchemy import text
from src.storage.database import DatabaseManager
from src.config import Config
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class StatisticsGenerator:
    def __init__(self, config: Config):
        self.db_manager = DatabaseManager(config.database_url)
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Génère un rapport statistique complet"""
        try:
            with self.db_manager.engine.connect() as conn:
                # 1. Temps total à quai vs en mouvement
                time_stats = self._get_time_statistics(conn)
                
                # 2. Top 5 navires avec la plus grande distance
                top_distance = self._get_top_vessels_by_distance(conn)
                
                # 3. Pourcentage de données valides
                data_quality = self._get_data_quality_metrics(conn)
                
                # 4. Nombre de points par navire
                point_stats = self._get_point_statistics(conn)
                
                # 5. Statistiques supplémentaires
                additional_stats = self._get_additional_statistics(conn)
            
            return {
                "time_analysis": time_stats,
                "top_vessels_by_distance": top_distance,
                "data_quality": data_quality,
                "point_statistics": point_stats,
                "additional_metrics": additional_stats,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération du rapport: {e}")
            raise
    
    def _get_time_statistics(self, conn) -> Dict[str, Any]:
        """Temps total à quai vs en mouvement"""
        try:
            query = """
            SELECT 
                SUM(moving_time_hours) as total_moving_time,
                SUM(at_dock_time_hours) as total_dock_time,
                SUM(total_time_hours) as total_time,
                AVG(moving_time_hours) as avg_moving_time_per_vessel,
                AVG(at_dock_time_hours) as avg_dock_time_per_vessel,
                COUNT(*) as vessels_count
            FROM vessel_metrics
            WHERE total_time_hours > 0
            """
            
            result = conn.execute(text(query)).fetchone()
            
            if not result:
                logger.warning("Aucune donnée temporelle trouvée")
                return {
                    "total_moving_time": 0,
                    "total_dock_time": 0,
                    "total_time": 0,
                    "avg_moving_time_per_vessel": 0,
                    "avg_dock_time_per_vessel": 0,
                    "vessels_count": 0,
                    "moving_time_percentage": 0,
                    "dock_time_percentage": 0
                }
            
            stats = dict(result._mapping)
            
            # Gérer les valeurs None
            for key in stats:
                if stats[key] is None:
                    stats[key] = 0
            
            # Calcul des pourcentages (avec protection contre division par zéro)
            total_time = stats.get('total_time', 0)
            if total_time and total_time > 0:
                stats['moving_time_percentage'] = (stats.get('total_moving_time', 0) / total_time) * 100
                stats['dock_time_percentage'] = (stats.get('total_dock_time', 0) / total_time) * 100
            else:
                stats['moving_time_percentage'] = 0
                stats['dock_time_percentage'] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Erreur dans _get_time_statistics: {e}")
            return {
                "total_moving_time": 0,
                "total_dock_time": 0,
                "total_time": 0,
                "avg_moving_time_per_vessel": 0,
                "avg_dock_time_per_vessel": 0,
                "vessels_count": 0,
                "moving_time_percentage": 0,
                "dock_time_percentage": 0
            }
    
    def _get_top_vessels_by_distance(self, conn) -> list:
        """Top 5 navires avec la plus grande distance parcourue"""
        try:
            query = """
            SELECT 
                mmsi,
                vessel_name,
                total_distance_nm,
                total_time_hours,
                avg_speed_knots,
                point_count
            FROM vessel_metrics
            WHERE total_distance_nm IS NOT NULL AND total_distance_nm > 0
            ORDER BY total_distance_nm DESC
            LIMIT 5
            """
            
            result = conn.execute(text(query)).fetchall()
            
            vessels = []
            for row in result:
                vessel_data = dict(row._mapping)
                
                # Gérer les valeurs None
                for key in vessel_data:
                    if vessel_data[key] is None:
                        if key in ['mmsi', 'point_count']:
                            vessel_data[key] = 0
                        elif key in ['total_distance_nm', 'total_time_hours', 'avg_speed_knots']:
                            vessel_data[key] = 0.0
                        else:
                            vessel_data[key] = ""
                
                vessels.append(vessel_data)
            
            return vessels
            
        except Exception as e:
            logger.error(f"Erreur dans _get_top_vessels_by_distance: {e}")
            return []
    
    def _get_data_quality_metrics(self, conn) -> Dict[str, Any]:
        """Pourcentage de données valides"""
        try:
            query = """
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
                    AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180 
                    THEN 1 ELSE 0 END) as valid_positions,
                SUM(CASE WHEN sog IS NOT NULL AND sog >= 0 AND sog <= 50 THEN 1 ELSE 0 END) as valid_speeds,
                SUM(CASE WHEN vessel_name IS NOT NULL AND vessel_name != '' THEN 1 ELSE 0 END) as valid_names,
                SUM(CASE WHEN base_datetime IS NOT NULL THEN 1 ELSE 0 END) as valid_timestamps,
                COUNT(DISTINCT mmsi) as unique_vessels
            FROM ais_data
            """
            
            result = conn.execute(text(query)).fetchone()
            
            if not result:
                logger.warning("Aucune donnée AIS trouvée pour le calcul de qualité")
                return {
                    "total_records": 0,
                    "valid_positions": 0,
                    "valid_speeds": 0,
                    "valid_names": 0,
                    "valid_timestamps": 0,
                    "unique_vessels": 0,
                    "valid_positions_percentage": 0,
                    "valid_speeds_percentage": 0,
                    "valid_names_percentage": 0,
                    "valid_timestamps_percentage": 0,
                    "overall_quality_score": 0
                }
            
            stats = dict(result._mapping)
            
            # Gérer les valeurs None
            for key in stats:
                if stats[key] is None:
                    stats[key] = 0
            
            total = stats.get('total_records', 0)
            if total and total > 0:
                stats['valid_positions_percentage'] = (stats.get('valid_positions', 0) / total) * 100
                stats['valid_speeds_percentage'] = (stats.get('valid_speeds', 0) / total) * 100
                stats['valid_names_percentage'] = (stats.get('valid_names', 0) / total) * 100
                stats['valid_timestamps_percentage'] = (stats.get('valid_timestamps', 0) / total) * 100
                stats['overall_quality_score'] = (
                    stats['valid_positions_percentage'] + 
                    stats['valid_speeds_percentage'] + 
                    stats['valid_timestamps_percentage']
                ) / 3
            else:
                stats['valid_positions_percentage'] = 0
                stats['valid_speeds_percentage'] = 0
                stats['valid_names_percentage'] = 0
                stats['valid_timestamps_percentage'] = 0
                stats['overall_quality_score'] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Erreur dans _get_data_quality_metrics: {e}")
            return {
                "total_records": 0,
                "valid_positions": 0,
                "valid_speeds": 0,
                "valid_names": 0,
                "valid_timestamps": 0,
                "unique_vessels": 0,
                "valid_positions_percentage": 0,
                "valid_speeds_percentage": 0,
                "valid_names_percentage": 0,
                "valid_timestamps_percentage": 0,
                "overall_quality_score": 0
            }
    
    def _get_point_statistics(self, conn) -> Dict[str, Any]:
        """Nombre de points par navire"""
        try:
            query = """
            SELECT 
                COUNT(DISTINCT mmsi) as total_vessels,
                AVG(point_count) as avg_points_per_vessel,
                MIN(point_count) as min_points_per_vessel,
                MAX(point_count) as max_points_per_vessel,
                STDDEV(point_count) as stddev_points_per_vessel
            FROM vessel_metrics
            WHERE point_count IS NOT NULL
            """
            
            result = conn.execute(text(query)).fetchone()
            
            if not result:
                logger.warning("Aucune donnée de points trouvée")
                return {
                    "total_vessels": 0,
                    "avg_points_per_vessel": 0,
                    "min_points_per_vessel": 0,
                    "max_points_per_vessel": 0,
                    "stddev_points_per_vessel": 0
                }
            
            stats = dict(result._mapping)
            
            # Gérer les valeurs None
            for key in stats:
                if stats[key] is None:
                    stats[key] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Erreur dans _get_point_statistics: {e}")
            return {
                "total_vessels": 0,
                "avg_points_per_vessel": 0,
                "min_points_per_vessel": 0,
                "max_points_per_vessel": 0,
                "stddev_points_per_vessel": 0
            }
    
    def _get_additional_statistics(self, conn) -> Dict[str, Any]:
        """Statistiques supplémentaires pour l'analyse"""
        try:
            query = """
            SELECT 
                AVG(avg_speed_knots) as fleet_avg_speed,
                MAX(max_speed_knots) as fleet_max_speed,
                AVG(total_distance_nm) as avg_distance_per_vessel,
                SUM(total_distance_nm) as total_fleet_distance,
                COUNT(DISTINCT vessel_name) as unique_vessel_names
            FROM vessel_metrics
            WHERE total_distance_nm IS NOT NULL AND total_distance_nm > 0
            """
            
            result = conn.execute(text(query)).fetchone()
            
            if not result:
                logger.warning("Aucune donnée supplémentaire trouvée")
                return {
                    "fleet_avg_speed": 0,
                    "fleet_max_speed": 0,
                    "avg_distance_per_vessel": 0,
                    "total_fleet_distance": 0,
                    "unique_vessel_names": 0
                }
            
            stats = dict(result._mapping)
            
            # Gérer les valeurs None
            for key in stats:
                if stats[key] is None:
                    stats[key] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Erreur dans _get_additional_statistics: {e}")
            return {
                "fleet_avg_speed": 0,
                "fleet_max_speed": 0,
                "avg_distance_per_vessel": 0,
                "total_fleet_distance": 0,
                "unique_vessel_names": 0
            }

def main():
    """Script principal pour générer les statistiques"""
    config = Config()
    generator = StatisticsGenerator(config)
    
    try:
        report = generator.generate_comprehensive_report()
        
        print("=" * 60)
        print("RAPPORT STATISTIQUE - TRAFIC MARITIME AIS")
        print("=" * 60)
        
        print("\n📊 ANALYSE TEMPORELLE")
        time_stats = report['time_analysis']
        print(f"• Temps total en mouvement: {time_stats.get('total_moving_time', 0):.1f} heures")
        print(f"• Temps total à quai: {time_stats.get('total_dock_time', 0):.1f} heures")
        print(f"• Pourcentage en mouvement: {time_stats.get('moving_time_percentage', 0):.1f}%")
        print(f"• Pourcentage à quai: {time_stats.get('dock_time_percentage', 0):.1f}%")
        
        print("\n🚢 TOP 5 NAVIRES PAR DISTANCE")
        for i, vessel in enumerate(report['top_vessels_by_distance'], 1):
            vessel_name = vessel.get('vessel_name', 'N/A')
            mmsi = vessel.get('mmsi', 0)
            distance = vessel.get('total_distance_nm', 0)
            speed = vessel.get('avg_speed_knots', 0)
            
            print(f"{i}. {vessel_name} (MMSI: {mmsi})")
            print(f"   Distance: {distance:.1f} milles nautiques")
            print(f"   Vitesse moyenne: {speed:.1f} nœuds")
        
        print("\n📈 QUALITÉ DES DONNÉES")
        quality = report['data_quality']
        print(f"• Total d'enregistrements: {quality.get('total_records', 0):,}")
        print(f"• Positions valides: {quality.get('valid_positions_percentage', 0):.1f}%")
        print(f"• Vitesses valides: {quality.get('valid_speeds_percentage', 0):.1f}%")
        print(f"• Score qualité global: {quality.get('overall_quality_score', 0):.1f}%")
        
        print("\n📍 STATISTIQUES DES POINTS")
        points = report['point_statistics']
        print(f"• Nombre total de navires: {points.get('total_vessels', 0)}")
        print(f"• Points moyens par navire: {points.get('avg_points_per_vessel', 0):.1f}")
        print(f"• Points min/max par navire: {points.get('min_points_per_vessel', 0)} / {points.get('max_points_per_vessel', 0)}")
        
        print("\n🌊 MÉTRIQUES ADDITIONNELLES")
        additional = report['additional_metrics']
        print(f"• Vitesse moyenne de la flotte: {additional.get('fleet_avg_speed', 0):.1f} nœuds")
        print(f"• Distance totale parcourue: {additional.get('total_fleet_distance', 0):,.1f} milles nautiques")
        print(f"• Noms de navires uniques: {additional.get('unique_vessel_names', 0)}")
        
        print(f"\n📅 Rapport généré le: {report['generated_at']}")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Erreur lors de la génération du rapport: {e}")
        print(f"Erreur: {e}")

if __name__ == "__main__":
    main()