-- ══════════════════════════════════════════════════════════════
-- INIT POSTGIS — Taller 1 Clustering Geoespacial Cali
-- ══════════════════════════════════════════════════════════════

-- Habilitar extensión PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- ──────────────────────────────────────────────────────────────
-- TABLA: IPS (Instituciones Prestadoras de Salud)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ips (
    id              SERIAL PRIMARY KEY,
    ips_id          VARCHAR(50) UNIQUE NOT NULL,
    nombre          TEXT NOT NULL,
    tipo            VARCHAR(30),         -- HOSPITAL, CLINICA, CENTRO_SALUD, IPS_EPS
    nivel_atencion  INTEGER,             -- 1, 2, 3, 4
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    geom            GEOMETRY(POINT, 4326),
    comuna          INTEGER,
    direccion       TEXT,
    municipio       VARCHAR(100) DEFAULT 'Santiago de Cali',
    departamento    VARCHAR(100) DEFAULT 'Valle del Cauca',
    habilitada      BOOLEAN DEFAULT TRUE,
    capacidad_camas INTEGER,
    telefono        VARCHAR(50),
    email           VARCHAR(100),
    fhir_location_id TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- TABLA: PACIENTES
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS patients (
    id              SERIAL PRIMARY KEY,
    patient_id      VARCHAR(50) UNIQUE NOT NULL,
    nombre          VARCHAR(100),
    apellido        VARCHAR(100),
    genero          VARCHAR(10),
    fecha_nacimiento DATE,
    tipo_sangre     VARCHAR(5),
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    geom            GEOMETRY(POINT, 4326),
    comuna          INTEGER,
    barrio          VARCHAR(100),
    direccion       TEXT,
    telefono        VARCHAR(50),
    email           VARCHAR(150),
    documento       VARCHAR(20),
    ips_asignada    VARCHAR(50),         -- FK a ips.ips_id
    condicion_preexistente TEXT,
    cluster_kmeans  INTEGER,
    cluster_dbscan  INTEGER,
    cluster_gmm     INTEGER,
    fhir_patient_id TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- TABLA: ENCOUNTERS (Ingresos/Egresos FHIR)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS encounters (
    id              SERIAL PRIMARY KEY,
    encounter_id    VARCHAR(50) UNIQUE NOT NULL,
    patient_id      VARCHAR(50),
    ips_id          VARCHAR(50),
    status          VARCHAR(20) DEFAULT 'in-progress', -- in-progress, finished, cancelled
    class_code      VARCHAR(20) DEFAULT 'IMP',
    period_start    TIMESTAMP DEFAULT NOW(),
    period_end      TIMESTAMP,
    fhir_resource   JSONB,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- TABLA: CLUSTERING RESULTS
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS cluster_results (
    id              SERIAL PRIMARY KEY,
    algorithm       VARCHAR(20) NOT NULL,  -- kmeans, dbscan, gmm
    k_clusters      INTEGER,
    params          JSONB,
    silhouette      DOUBLE PRECISION,
    davies_bouldin  DOUBLE PRECISION,
    calinski_harabasz DOUBLE PRECISION,
    ejecutado_at    TIMESTAMP DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────────
-- ÍNDICES ESPACIALES GIST (obligatorios según el taller)
-- ──────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_ips_geom     ON ips     USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_patients_geom ON patients USING GIST(geom);

-- Índices adicionales
CREATE INDEX IF NOT EXISTS idx_patients_cluster_kmeans  ON patients(cluster_kmeans);
CREATE INDEX IF NOT EXISTS idx_patients_cluster_dbscan  ON patients(cluster_dbscan);
CREATE INDEX IF NOT EXISTS idx_patients_cluster_gmm     ON patients(cluster_gmm);
CREATE INDEX IF NOT EXISTS idx_patients_ips_asignada    ON patients(ips_asignada);
CREATE INDEX IF NOT EXISTS idx_encounters_patient       ON encounters(patient_id);
CREATE INDEX IF NOT EXISTS idx_encounters_ips           ON encounters(ips_id);
CREATE INDEX IF NOT EXISTS idx_encounters_status        ON encounters(status);

-- ──────────────────────────────────────────────────────────────
-- FUNCIÓN: Actualizar geom automáticamente desde lat/lng
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION sync_geom_patients()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.lat IS NOT NULL AND NEW.lng IS NOT NULL THEN
        NEW.geom = ST_SetSRID(ST_MakePoint(NEW.lng, NEW.lat), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_geom_ips()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.lat IS NOT NULL AND NEW.lng IS NOT NULL THEN
        NEW.geom = ST_SetSRID(ST_MakePoint(NEW.lng, NEW.lat), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers para auto-calcular geometría
CREATE TRIGGER trg_patients_geom
    BEFORE INSERT OR UPDATE ON patients
    FOR EACH ROW EXECUTE FUNCTION sync_geom_patients();

CREATE TRIGGER trg_ips_geom
    BEFORE INSERT OR UPDATE ON ips
    FOR EACH ROW EXECUTE FUNCTION sync_geom_ips();

-- ──────────────────────────────────────────────────────────────
-- FUNCIÓN: Trigger updated_at en encounters
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_encounters_updated_at
    BEFORE UPDATE ON encounters
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Confirmación
SELECT 'PostGIS inicializado correctamente para Taller 1 - UAO 2026' AS status;
