-- =============================================================================
-- AIS pipeline schema — single source of truth.
-- Idempotent: safe to run on every pipeline start (and by Postgres at init).
-- =============================================================================

-- Raw-but-cleaned AIS positions. One row per (vessel, timestamp).
CREATE TABLE IF NOT EXISTS ais_data (
    id                BIGSERIAL PRIMARY KEY,
    mmsi              BIGINT           NOT NULL CHECK (mmsi BETWEEN 100000000 AND 999999999),
    base_datetime     TIMESTAMPTZ      NOT NULL,
    latitude          DOUBLE PRECISION NOT NULL CHECK (latitude  BETWEEN -90  AND 90),
    longitude         DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    sog               DOUBLE PRECISION CHECK (sog IS NULL OR sog BETWEEN 0 AND 102.3),   -- speed over ground (kn)
    cog               DOUBLE PRECISION CHECK (cog IS NULL OR cog BETWEEN 0 AND 360),     -- course over ground (deg)
    heading           DOUBLE PRECISION CHECK (heading IS NULL OR heading BETWEEN 0 AND 360),
    vessel_name       VARCHAR(100),
    imo               VARCHAR(20),
    call_sign         VARCHAR(20),
    vessel_type       SMALLINT,        -- ITU vessel type code, see vessel_type_codes
    status            SMALLINT,        -- navigational status, see navigation_status_codes
    length            DOUBLE PRECISION,
    width             DOUBLE PRECISION,
    draft             DOUBLE PRECISION,
    cargo             SMALLINT,
    transceiver_class VARCHAR(5),
    ingested_at       TIMESTAMPTZ      NOT NULL DEFAULT now(),
    -- Natural key: makes loads idempotent (re-running a day inserts nothing new).
    CONSTRAINT uq_ais_position UNIQUE (mmsi, base_datetime)
);

CREATE INDEX IF NOT EXISTS idx_ais_datetime    ON ais_data (base_datetime);
CREATE INDEX IF NOT EXISTS idx_ais_vessel_type ON ais_data (vessel_type);

-- Per-vessel aggregates, recomputed from the full history of each vessel touched by a load.
CREATE TABLE IF NOT EXISTS vessel_metrics (
    mmsi               BIGINT PRIMARY KEY,
    vessel_name        VARCHAR(100),
    vessel_type        SMALLINT,
    total_distance_nm  DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_time_hours   DOUBLE PRECISION NOT NULL DEFAULT 0,
    moving_time_hours  DOUBLE PRECISION NOT NULL DEFAULT 0,
    at_dock_time_hours DOUBLE PRECISION NOT NULL DEFAULT 0,
    point_count        INTEGER          NOT NULL DEFAULT 0,
    avg_speed_knots    DOUBLE PRECISION,
    max_speed_knots    DOUBLE PRECISION,
    first_seen         TIMESTAMPTZ,
    last_seen          TIMESTAMPTZ,
    last_updated       TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metrics_distance    ON vessel_metrics (total_distance_nm DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_vessel_name ON vessel_metrics (vessel_name);
CREATE INDEX IF NOT EXISTS idx_metrics_vessel_type ON vessel_metrics (vessel_type);

-- ---------------------------------------------------------------------------
-- Reference data
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vessel_type_codes (
    code        INTEGER PRIMARY KEY,
    description VARCHAR(100) NOT NULL,
    category    VARCHAR(50)
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

CREATE TABLE IF NOT EXISTS navigation_status_codes (
    code        INTEGER PRIMARY KEY,
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

-- ---------------------------------------------------------------------------
-- Views (relative to the data, not to now(), so historical datasets work)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_traffic_summary AS
SELECT
    COUNT(DISTINCT mmsi)                                     AS total_vessels,
    COUNT(*)                                                 AS total_positions,
    MIN(base_datetime)                                       AS earliest_position,
    MAX(base_datetime)                                       AS latest_position,
    AVG(sog)                                                 AS avg_speed_fleet,
    MAX(sog)                                                 AS max_speed_recorded,
    COUNT(*) FILTER (WHERE sog > 1) * 100.0 / NULLIF(COUNT(*), 0) AS moving_percentage
FROM ais_data;

CREATE OR REPLACE VIEW v_active_vessels AS
SELECT
    ad.mmsi,
    MAX(ad.vessel_name)    AS vessel_name,
    COUNT(*)               AS recent_positions,
    MAX(ad.base_datetime)  AS last_seen,
    AVG(ad.sog)            AS avg_speed
FROM ais_data ad
WHERE ad.base_datetime >= (SELECT MAX(base_datetime) FROM ais_data) - INTERVAL '24 hours'
GROUP BY ad.mmsi;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_traffic_stats AS
SELECT
    (base_datetime AT TIME ZONE 'UTC')::date  AS traffic_date,
    COUNT(DISTINCT mmsi)                      AS unique_vessels,
    COUNT(*)                                  AS total_positions,
    AVG(sog)                                  AS avg_speed,
    MAX(sog)                                  AS max_speed,
    COUNT(*) FILTER (WHERE sog > 1)           AS moving_positions,
    COUNT(*) FILTER (WHERE sog <= 1)          AS stationary_positions
FROM ais_data
GROUP BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_traffic_date ON mv_daily_traffic_stats (traffic_date);
