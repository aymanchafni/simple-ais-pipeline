from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import logging

logger = logging.getLogger(__name__)
Base = declarative_base()

class AISRecord(Base):
    __tablename__ = 'ais_data'
    
    id = Column(Integer, primary_key=True)
    mmsi = Column(Integer, nullable=False)
    base_datetime = Column(DateTime, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    sog = Column(Float)  # Speed Over Ground
    cog = Column(Float)  # Course Over Ground
    heading = Column(Float)
    vessel_name = Column(String(100))
    imo = Column(String(20))
    call_sign = Column(String(20))
    vessel_type = Column(String(50))
    status = Column(String(50))
    length = Column(Float)
    width = Column(Float)
    draft = Column(Float)
    cargo = Column(Float)
    transceiver_class = Column(String(5))
    
    # Index pour optimiser les requêtes
    __table_args__ = (
        Index('idx_mmsi_datetime', 'mmsi', 'base_datetime'),
        Index('idx_datetime', 'base_datetime'),
        Index('idx_location', 'latitude', 'longitude'),
    )

class VesselMetrics(Base):
    __tablename__ = 'vessel_metrics'
    
    id = Column(Integer, primary_key=True)
    mmsi = Column(Integer, unique=True, nullable=False)
    vessel_name = Column(String(100))
    total_distance_nm = Column(Float)
    total_time_hours = Column(Float)
    moving_time_hours = Column(Float)
    at_dock_time_hours = Column(Float)
    point_count = Column(Integer)
    avg_speed_knots = Column(Float)
    max_speed_knots = Column(Float)
    last_updated = Column(DateTime)

class DatabaseManager:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
    def create_tables(self):
        """Crée les tables si elles n'existent pas"""
        Base.metadata.create_all(bind=self.engine)
        logger.info("Tables créées avec succès")
    
    def save_ais_data(self, df: pd.DataFrame):
        """Sauvegarde les données AIS nettoyées"""
        try:
            # Mapping des colonnes
            df_mapped = df.rename(columns={
                'MMSI' : 'mmsi',
                'BaseDateTime': 'base_datetime',
                'LAT': 'latitude',
                'LON': 'longitude',
                'SOG': 'sog',
                'COG': 'cog',
                'Heading': 'heading',
                'VesselName': 'vessel_name',
                'IMO': 'imo',
                'CallSign': 'call_sign',
                'VesselType': 'vessel_type',
                'Status': 'status',
                'Length': 'length',
                'Width': 'width',
                'Draft': 'draft',
                'Cargo': 'cargo',
                'TransceiverClass' : 'transceiver_class'
            })
            
            # Conversion en minuscules pour correspondre au modèle
            df_mapped.columns = df_mapped.columns.str.lower()
            
            df_mapped.to_sql('ais_data', self.engine, if_exists='append', index=False)
            logger.info(f"{len(df_mapped)} enregistrements AIS sauvegardés")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde AIS: {e}")
            raise
    
    def save_vessel_metrics(self, df: pd.DataFrame):
        """Sauvegarde les métriques par navire"""
        try:
            df['last_updated'] = pd.Timestamp.now()
            df.to_sql('vessel_metrics', self.engine, if_exists='replace', index=False)
            logger.info(f"{len(df)} métriques de navires sauvegardées")
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des métriques: {e}")
            raise