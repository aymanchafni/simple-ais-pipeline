from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from src.storage.database import DatabaseManager
from src.config import Config
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Tanger Med AIS API", 
    version="1.0.0",
    description="API pour le pipeline de données AIS de Tanger Med"
)

# Configuration CORS pour le dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = Config()
db_manager = DatabaseManager(config.database_url)

@app.get("/")
async def root():
    return {
        "message": "Tanger Med AIS Data Pipeline API",
        "version": "1.0.0",
        "endpoints": [
            "/health",
            "/statistics",
            "/vessels",
            "/vessels/{mmsi}",
            "/vessels/search",
            "/metrics/time-analysis",
            "/metrics/quality"
        ]
    }

@app.get("/health")
async def health_check():
    """Vérification de l'état de l'API et de la base de données"""
    try:
        with db_manager.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "status": "healthy", 
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy", 
            "database": "disconnected", 
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/vessels")
async def get_vessels(
    limit: Optional[int] = Query(100, le=1000, description="Nombre maximum de navires à retourner"),
    offset: Optional[int] = Query(0, ge=0, description="Décalage pour la pagination")
):
    """Liste des navires avec leurs métriques - Colonnes exactes de vessel_metrics"""
    try:
        query = f"""
        SELECT mmsi, vessel_name, total_distance_nm, total_time_hours, 
               moving_time_hours, at_dock_time_hours, point_count,
               avg_speed_knots, max_speed_knots, last_updated
        FROM vessel_metrics 
        ORDER BY total_distance_nm DESC
        LIMIT {limit} OFFSET {offset}
        """
        
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(query)).fetchall()
            
        vessels = [dict(row._mapping) for row in result]
        
        # Convertir les timestamps en strings pour JSON
        for vessel in vessels:
            for key, value in vessel.items():
                if isinstance(value, datetime):
                    vessel[key] = value.isoformat() if value else None
        
        return {
            "vessels": vessels, 
            "count": len(vessels),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des navires: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vessels/{mmsi}")
async def get_vessel_details(mmsi: int):
    """Détails d'un navire spécifique avec ses positions récentes"""
    try:
        # Métriques du navire - colonnes exactes
        metrics_query = f"""
        SELECT mmsi, vessel_name, total_distance_nm, total_time_hours,
               moving_time_hours, at_dock_time_hours, point_count,
               avg_speed_knots, max_speed_knots, last_updated
        FROM vessel_metrics 
        WHERE mmsi = {mmsi}
        """
        
        # Dernières positions (limitées à 100)
        positions_query = f"""
        SELECT base_datetime, latitude, longitude, sog, cog, heading, 
               vessel_name, status
        FROM ais_data 
        WHERE mmsi = {mmsi}
        ORDER BY base_datetime DESC
        LIMIT 100
        """
        
        with db_manager.engine.connect() as conn:
            metrics = conn.execute(text(metrics_query)).fetchone()
            positions = conn.execute(text(positions_query)).fetchall()
        
        if not metrics:
            raise HTTPException(status_code=404, detail="Navire non trouvé")
        
        # Convertir les résultats
        vessel_data = dict(metrics._mapping)
        positions_data = [dict(pos._mapping) for pos in positions]
        
        # Convertir les timestamps
        for key, value in vessel_data.items():
            if isinstance(value, datetime):
                vessel_data[key] = value.isoformat() if value else None
                
        for pos in positions_data:
            for key, value in pos.items():
                if isinstance(value, datetime):
                    pos[key] = value.isoformat() if value else None
        
        return {
            "metrics": vessel_data,
            "recent_positions": positions_data,
            "positions_count": len(positions_data)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du navire {mmsi}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vessels/search")
async def search_vessels(
    name: Optional[str] = Query(None, description="Nom du navire (recherche partielle)"),
    vessel_type: Optional[str] = Query(None, description="Type de navire"),
    min_distance: Optional[float] = Query(None, ge=0, description="Distance minimum en milles nautiques"),
    max_distance: Optional[float] = Query(None, ge=0, description="Distance maximum en milles nautiques"),
    limit: Optional[int] = Query(50, le=500, description="Nombre maximum de résultats")
):
    """Recherche de navires avec filtres - colonnes exactes"""
    try:
        conditions = []
        if name:
            conditions.append(f"vessel_name ILIKE '%{name}%'")
        if min_distance:
            conditions.append(f"total_distance_nm >= {min_distance}")
        if max_distance:
            conditions.append(f"total_distance_nm <= {max_distance}")
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
        SELECT mmsi, vessel_name, total_distance_nm, point_count,
               avg_speed_knots, max_speed_knots, moving_time_hours, at_dock_time_hours
        FROM vessel_metrics
        {where_clause}
        ORDER BY total_distance_nm DESC
        LIMIT {limit}
        """
        
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(query)).fetchall()
        
        vessels = [dict(row._mapping) for row in result]
        
        return {
            "vessels": vessels,
            "count": len(vessels),
            "filters_applied": {
                "name": name,
                "min_distance": min_distance,
                "max_distance": max_distance
            }
        }
        
    except Exception as e:
        logger.error(f"Erreur lors de la recherche: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/statistics")
async def get_statistics():
    """Statistiques globales du trafic maritime - basées sur les vraies colonnes"""
    try:
        # Statistiques globales
        global_stats_query = """
        SELECT 
            COUNT(DISTINCT vm.mmsi) as total_vessels,
            COUNT(ad.id) as total_positions,
            AVG(vm.total_distance_nm) as avg_distance,
            AVG(vm.moving_time_hours) as avg_moving_time,
            AVG(vm.at_dock_time_hours) as avg_dock_time,
            AVG(vm.avg_speed_knots) as avg_speed_fleet,
            MAX(vm.max_speed_knots) as max_speed_recorded,
            COUNT(CASE WHEN ad.latitude IS NOT NULL AND ad.longitude IS NOT NULL THEN 1 END) * 100.0 / NULLIF(COUNT(ad.id), 0) as valid_position_percentage
        FROM vessel_metrics vm
        LEFT JOIN ais_data ad ON vm.mmsi = ad.mmsi
        """
        
        # Top 5 navires par distance - colonnes exactes
        top_vessels_query = """
        SELECT mmsi, vessel_name, total_distance_nm, avg_speed_knots, 
               point_count, moving_time_hours, at_dock_time_hours
        FROM vessel_metrics
        WHERE total_distance_nm > 0
        ORDER BY total_distance_nm DESC
        LIMIT 5
        """
        
        with db_manager.engine.connect() as conn:
            global_stats = conn.execute(text(global_stats_query)).fetchone()
            top_vessels = conn.execute(text(top_vessels_query)).fetchall()
        
        return {
            "global_statistics": dict(global_stats._mapping) if global_stats else {},
            "top_vessels_by_distance": [dict(vessel._mapping) for vessel in top_vessels],
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erreur lors du calcul des statistiques: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/time-analysis")
async def get_time_analysis():
    """Analyse détaillée des temps (en mouvement vs à quai) - colonnes exactes"""
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
        
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
        
        stats = dict(result._mapping) if result else {}
        
        # Calcul des pourcentages
        if stats.get('total_time', 0) > 0:
            stats['moving_time_percentage'] = (stats.get('total_moving_time', 0) / stats['total_time']) * 100
            stats['dock_time_percentage'] = (stats.get('total_dock_time', 0) / stats['total_time']) * 100
        else:
            stats['moving_time_percentage'] = 0
            stats['dock_time_percentage'] = 0
        
        return {
            "time_analysis": stats,
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse temporelle: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/quality")
async def get_data_quality():
    """Métriques de qualité des données"""
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
        
        with db_manager.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
        
        stats = dict(result._mapping) if result else {}
        
        total = stats.get('total_records', 0)
        if total > 0:
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
            for key in ['valid_positions_percentage', 'valid_speeds_percentage', 
                       'valid_names_percentage', 'valid_timestamps_percentage', 'overall_quality_score']:
                stats[key] = 0
        
        return {
            "data_quality": stats,
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erreur lors du calcul de la qualité: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/summary")
async def get_metrics_summary():
    """Résumé de toutes les métriques pour le dashboard"""
    try:
        # Combiner toutes les métriques en une seule réponse
        stats_data = await get_statistics()
        time_data = await get_time_analysis()
        quality_data = await get_data_quality()
        
        return {
            "summary": {
                "statistics": stats_data,
                "time_analysis": time_data,
                "data_quality": quality_data
            },
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erreur lors du résumé des métriques: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)