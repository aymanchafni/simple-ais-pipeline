-- Création de la base de données Tanger Med AIS
-- Ce script est exécuté automatiquement par PostgreSQL au démarrage

-- Extensions utiles
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis" SCHEMA public;

-- Table principale des données AIS
CREATE TABLE IF NOT EXISTS ais_data (
    id SERIAL PRIMARY KEY,
    mmsi BIGINT NOT NULL,
    base_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    sog DOUBLE PRECISION, -- Speed Over Ground (nœuds)
    cog DOUBLE PRECISION, -- Course Over Ground (degrés)
    heading DOUBLE PRECISION, -- Cap (degrés)
    vessel_name VARCHAR(100),
    imo VARCHAR(20),
    call_sign VARCHAR(20),
    vessel_type VARCHAR(50),
    status VARCHAR(50),
    length DOUBLE PRECISION, -- Longueur (mètres)
    width DOUBLE PRECISION,  -- Largeur (mètres)
    draft DOUBLE PRECISION,  -- Tirant d'eau (mètres)
    cargo DOUBLE PRECISION,  -- Cargaison (tonnes)
    transceiver_class VARCHAR(5),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table des métriques par navire
CREATE TABLE IF NOT EXISTS vessel_metrics (
    id SERIAL PRIMARY KEY,
    mmsi BIGINT UNIQUE NOT NULL,
    vessel_name VARCHAR(100),
    total_distance_nm DOUBLE PRECISION DEFAULT 0,
    total_time_hours DOUBLE PRECISION DEFAULT 0,
    moving_time_hours DOUBLE PRECISION DEFAULT 0,
    at_dock_time_hours DOUBLE PRECISION DEFAULT 0,
    point_count INTEGER DEFAULT 0,
    avg_speed_knots DOUBLE PRECISION DEFAULT 0,
    max_speed_knots DOUBLE PRECISION DEFAULT 0,
    min_speed_knots DOUBLE PRECISION DEFAULT 0,
    first_position_time TIMESTAMP WITH TIME ZONE,
    last_position_time TIMESTAMP WITH TIME ZONE,
    start_latitude DOUBLE PRECISION,
    start_longitude DOUBLE PRECISION,
    end_latitude DOUBLE PRECISION,
    end_longitude DOUBLE PRECISION,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index pour optimiser les performances
CREATE INDEX IF NOT EXISTS idx_ais_mmsi_datetime ON ais_data(mmsi, base_datetime);
CREATE INDEX IF NOT EXISTS idx_ais_datetime ON ais_data(base_datetime);
CREATE INDEX IF NOT EXISTS idx_ais_location ON ais_data(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_ais_mmsi ON ais_data(mmsi);
CREATE INDEX IF NOT EXISTS idx_ais_vessel_name ON ais_data(vessel_name);
CREATE INDEX IF NOT EXISTS idx_ais_vessel_type ON ais_data(vessel_type);

CREATE INDEX IF NOT EXISTS idx_metrics_mmsi ON vessel_metrics(mmsi);
CREATE INDEX IF NOT EXISTS idx_metrics_distance ON vessel_metrics(total_distance_nm);
CREATE INDEX IF NOT EXISTS idx_metrics_vessel_name ON vessel_metrics(vessel_name);

-- Contraintes de validation
ALTER TABLE ais_data ADD CONSTRAINT chk_latitude CHECK (latitude BETWEEN -90 AND 90);
ALTER TABLE ais_data ADD CONSTRAINT chk_longitude CHECK (longitude BETWEEN -180 AND 180);
ALTER TABLE ais_data ADD CONSTRAINT chk_sog CHECK (sog IS NULL OR sog BETWEEN 0 AND 100);
ALTER TABLE ais_data ADD CONSTRAINT chk_cog CHECK (cog IS NULL OR cog BETWEEN 0 AND 360);
ALTER TABLE ais_data ADD CONSTRAINT chk_heading CHECK (heading IS NULL OR heading BETWEEN 0 AND 360);

-- Fonction pour mettre à jour automatiquement updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers pour mise à jour automatique des timestamps
CREATE TRIGGER update_ais_data_updated_at 
    BEFORE UPDATE ON ais_data 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vessel_metrics_updated_at 
    BEFORE UPDATE ON vessel_metrics 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Vue pour les statistiques rapides
CREATE OR REPLACE VIEW v_traffic_summary AS
SELECT 
    COUNT(DISTINCT mmsi) as total_vessels,
    COUNT(*) as total_positions,
    MIN(base_datetime) as earliest_position,
    MAX(base_datetime) as latest_position,
    AVG(sog) as avg_speed_fleet,
    MAX(sog) as max_speed_recorded,
    COUNT(CASE WHEN sog > 1 THEN 1 END) * 100.0 / COUNT(*) as moving_percentage
FROM ais_data
WHERE base_datetime >= CURRENT_DATE - INTERVAL '30 days';

-- Vue pour les navires actifs récents
CREATE OR REPLACE VIEW v_active_vessels AS
SELECT 
    ad.mmsi,
    ad.vessel_name,
    COUNT(*) as recent_positions,
    MAX(ad.base_datetime) as last_seen,
    AVG(ad.sog) as avg_speed,
    vm.total_distance_nm,
    vm.point_count
FROM ais_data ad
LEFT JOIN vessel_metrics vm ON ad.mmsi = vm.mmsi
WHERE ad.base_datetime >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY ad.mmsi, ad.vessel_name, vm.total_distance_nm, vm.point_count
ORDER BY MAX(ad.base_datetime) DESC;

-- Table pour les logs d'audit (optionnel)
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    operation VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    table_name VARCHAR(50) NOT NULL,
    record_id INTEGER,
    old_values JSONB,
    new_values JSONB,
    user_name VARCHAR(100),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Fonction d'audit (optionnel)
CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (operation, table_name, record_id, old_values)
        VALUES (TG_OP, TG_TABLE_NAME, OLD.id, row_to_json(OLD));
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (operation, table_name, record_id, old_values, new_values)
        VALUES (TG_OP, TG_TABLE_NAME, NEW.id, row_to_json(OLD), row_to_json(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (operation, table_name, record_id, new_values)
        VALUES (TG_OP, TG_TABLE_NAME, NEW.id, row_to_json(NEW));
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Commentaires sur les tables
COMMENT ON TABLE ais_data IS 'Table principale contenant toutes les positions AIS des navires';
COMMENT ON TABLE vessel_metrics IS 'Métriques agrégées par navire (distance, temps, etc.)';
COMMENT ON VIEW v_traffic_summary IS 'Vue résumé du trafic maritime des 30 derniers jours';
COMMENT ON VIEW v_active_vessels IS 'Vue des navires actifs dans les dernières 24h';

-- Commentaires sur les colonnes principales
COMMENT ON COLUMN ais_data.mmsi IS 'Maritime Mobile Service Identity - Identifiant unique du navire';
COMMENT ON COLUMN ais_data.sog IS 'Speed Over Ground en nœuds';
COMMENT ON COLUMN ais_data.cog IS 'Course Over Ground en degrés (0-360)';
COMMENT ON COLUMN ais_data.latitude IS 'Latitude en degrés décimaux (-90 à +90)';
COMMENT ON COLUMN ais_data.longitude IS 'Longitude en degrés décimaux (-180 à +180)';

-- Insertion de quelques données de référence (types de navires)
CREATE TABLE IF NOT EXISTS vessel_type_codes (
    code INTEGER PRIMARY KEY,
    description VARCHAR(100) NOT NULL,
    category VARCHAR(50)
);

INSERT INTO vessel_type_codes (code, description, category) VALUES
(30, 'Fishing', 'Fishing'),
(31, 'Towing', 'Tug/Pilot'),
(32, 'Towing length > 200m', 'Tug/Pilot'),
(33, 'Dredging/Underwater operations', 'Other'),
(34, 'Diving operations', 'Other'),
(35, 'Military operations', 'Military'),
(36, 'Sailing', 'Pleasure'),
(37, 'Pleasure craft', 'Pleasure'),
(40, 'High speed craft (HSC)', 'Passenger'),
(50, 'Pilot vessel', 'Tug/Pilot'),
(51, 'Search and rescue vessel', 'Other'),
(52, 'Tug', 'Tug/Pilot'),
(53, 'Port tender', 'Tug/Pilot'),
(54, 'Anti-pollution equipment', 'Other'),
(55, 'Law enforcement', 'Government'),
(60, 'Passenger', 'Passenger'),
(70, 'Cargo', 'Cargo'),
(71, 'Cargo, all ships carrying DG', 'Cargo'),
(72, 'Cargo, Hazard cat A', 'Cargo'),
(73, 'Cargo, Hazard cat B', 'Cargo'),
(74, 'Cargo, Hazard cat C', 'Cargo'),
(75, 'Cargo, Hazard cat D', 'Cargo'),
(80, 'Tanker', 'Tanker'),
(81, 'Tanker, Hazard cat A', 'Tanker'),
(82, 'Tanker, Hazard cat B', 'Tanker'),
(83, 'Tanker, Hazard cat C', 'Tanker'),
(84, 'Tanker, Hazard cat D', 'Tanker'),
(89, 'Other type', 'Other')
ON CONFLICT (code) DO NOTHING;

-- Données de référence pour les status de navigation
CREATE TABLE IF NOT EXISTS navigation_status_codes (
    code INTEGER PRIMARY KEY,
    description VARCHAR(100) NOT NULL
);

INSERT INTO navigation_status_codes (code, description) VALUES
(0, 'Under way using engine'),
(1, 'At anchor'),
(2, 'Not under command'),
(3, 'Restricted manoeuvrability'),
(4, 'Constrained by her draught'),
(5, 'Moored'),
(6, 'Aground'),
(7, 'Engaged in fishing'),
(8, 'Under way sailing'),
(11, 'Power-driven vessel towing astern'),
(12, 'Power-driven vessel pushing ahead'),
(14, 'AIS-SART (active)'),
(15, 'Undefined (default)')
ON CONFLICT (code) DO NOTHING;

-- Fonction utilitaire pour calculer la distance entre deux points
CREATE OR REPLACE FUNCTION calculate_distance_nm(
    lat1 DOUBLE PRECISION, 
    lon1 DOUBLE PRECISION, 
    lat2 DOUBLE PRECISION, 
    lon2 DOUBLE PRECISION
) RETURNS DOUBLE PRECISION AS $$
DECLARE
    earth_radius_nm CONSTANT DOUBLE PRECISION := 3440.065; -- Rayon terrestre en milles nautiques
    dlat DOUBLE PRECISION;
    dlon DOUBLE PRECISION;
    a DOUBLE PRECISION;
    c DOUBLE PRECISION;
BEGIN
    -- Conversion en radians
    dlat := radians(lat2 - lat1);
    dlon := radians(lon2 - lon1);
    
    -- Formule de Haversine
    a := sin(dlat/2) * sin(dlat/2) + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2) * sin(dlon/2);
    c := 2 * atan2(sqrt(a), sqrt(1-a));
    
    RETURN earth_radius_nm * c;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Vue matérialisée pour les performances (optionnel)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_traffic_stats AS
SELECT 
    DATE(base_datetime) as traffic_date,
    COUNT(DISTINCT mmsi) as unique_vessels,
    COUNT(*) as total_positions,
    AVG(sog) as avg_speed,
    MAX(sog) as max_speed,
    COUNT(CASE WHEN sog > 1 THEN 1 END) as moving_positions,
    COUNT(CASE WHEN sog <= 1 THEN 1 END) as stationary_positions
FROM ais_data
GROUP BY DATE(base_datetime)
ORDER BY traffic_date DESC;

-- Index sur la vue matérialisée
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_traffic_date ON mv_daily_traffic_stats(traffic_date);

-- Messages informatifs
DO $$ 
BEGIN
    RAISE NOTICE 'Base de données Tanger Med AIS initialisée avec succès !';
    RAISE NOTICE 'Tables créées : ais_data, vessel_metrics, vessel_type_codes, navigation_status_codes';
    RAISE NOTICE 'Vues créées : v_traffic_summary, v_active_vessels';
    RAISE NOTICE 'Fonctions utilitaires : calculate_distance_nm, update_updated_at_column';
END $$;