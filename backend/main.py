"""
main.py — FastAPI Clustering API
Taller 1: Sistema de Clustering Geoespacial para Red de Salud Cali
UAO 2026-1

Endpoints:
  GET  /health                    — Estado del servidor
  POST /clustering/kmeans         — K-Means clustering
  POST /clustering/dbscan         — DBSCAN clustering
  POST /clustering/gmm            — Gaussian Mixture Model
  GET  /clustering/results        — Comparativa de métricas
  GET  /ips                       — Lista de IPS con ocupación
  GET  /patients/geojson          — Pacientes como GeoJSON
  POST /fhir/Encounter            — Registrar ingreso paciente (FHIR)
  PATCH /fhir/Encounter/{id}      — Dar de alta (FHIR)
  GET  /fhir/Location             — IPS cercanas (geoespacial)
  GET  /ws/encounters             — WebSocket (tiempo real)
"""

import json
import math
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import numpy as np
import psycopg2
import psycopg2.extras
import requests
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sklearn.cluster import KMeans, DBSCAN
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.preprocessing import StandardScaler

# ─── Configuración ────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "cali_geo"),
    "user": os.getenv("DB_USER", "geo_user"),
    "password": os.getenv("DB_PASSWORD", "geo_pass_2026"),
}
FHIR_URL = os.getenv("FHIR_URL", "http://localhost:8080/fhir")


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ─── WebSocket Manager ────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        for ws in list(self.active):
            try:
                await ws.send_json(message)
            except Exception:
                self.disconnect(ws)


manager = ConnectionManager()


# ─── App ──────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Clustering API iniciada — UAO Taller 1")
    yield
    print("🛑 Shutting down...")


app = FastAPI(
    title="Clustering Geoespacial Cali — UAO 2026",
    description="API de clustering para red de salud Cali. K-Means, DBSCAN, GMM + FHIR R4",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Schemas ──────────────────────────────────────────────────
class KMeansParams(BaseModel):
    k: int = 5
    max_iter: int = 300
    n_init: int = 10


class DBSCANParams(BaseModel):
    eps_km: float = 0.5    # radio en km
    min_samples: int = 5


class GMMParams(BaseModel):
    n_components: int = 5
    covariance_type: str = "full"   # full, tied, diag, spherical
    max_iter: int = 100


class EncounterCreate(BaseModel):
    patient_id: str
    ips_id: str
    class_code: str = "IMP"         # IMP=internación, AMB=ambulatorio


class EncounterDischarge(BaseModel):
    encounter_id: str


# ─── Helpers ──────────────────────────────────────────────────
def fetch_coordinates() -> tuple[np.ndarray, list[str]]:
    """Obtiene lat/lng de todos los pacientes desde PostGIS."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT patient_id, lat, lng FROM patients WHERE lat IS NOT NULL AND lng IS NOT NULL")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    coords = np.array([[r["lat"], r["lng"]] for r in rows])
    ids = [r["patient_id"] for r in rows]
    return coords, ids


def km_to_degrees(km: float) -> float:
    """Convierte km a grados aproximados (para Colombia ~lat 3°)."""
    return km / 111.0


def compute_metrics(coords: np.ndarray, labels: np.ndarray) -> dict:
    """Calcula métricas de clustering. Ignora noise (-1) de DBSCAN."""
    mask = labels != -1
    if mask.sum() < 2 or len(set(labels[mask])) < 2:
        return {"silhouette": None, "davies_bouldin": None, "calinski_harabasz": None}
    try:
        return {
            "silhouette":         round(float(silhouette_score(coords[mask], labels[mask])), 4),
            "davies_bouldin":     round(float(davies_bouldin_score(coords[mask], labels[mask])), 4),
            "calinski_harabasz":  round(float(calinski_harabasz_score(coords[mask], labels[mask])), 4),
        }
    except Exception:
        return {"silhouette": None, "davies_bouldin": None, "calinski_harabasz": None}


def save_cluster_result(algorithm: str, k: int, params: dict, metrics: dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cluster_results (algorithm, k_clusters, params, silhouette, davies_bouldin, calinski_harabasz)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        algorithm, k, json.dumps(params),
        metrics.get("silhouette"), metrics.get("davies_bouldin"), metrics.get("calinski_harabasz"),
    ))
    conn.commit()
    cur.close()
    conn.close()


def update_patient_clusters(col: str, ids: list, labels: list):
    conn = get_conn()
    cur = conn.cursor()
    for pid, label in zip(ids, labels):
        cur.execute(f"UPDATE patients SET {col} = %s WHERE patient_id = %s", (int(label), pid))
    conn.commit()
    cur.close()
    conn.close()


def cluster_summary(coords: np.ndarray, labels: np.ndarray) -> list:
    """Resumen por cluster: centroide, cantidad de pacientes."""
    unique = sorted(int(x) for x in set(labels.tolist()))
    summary = []
    for c in unique:
        mask = labels == c
        pts = coords[mask]
        summary.append({
            "cluster_id": int(c),
            "n_patients": int(mask.sum()),
            "centroid_lat": round(float(pts[:, 0].mean()), 6),
            "centroid_lng": round(float(pts[:, 1].mean()), 6),
            "is_noise": (c == -1),
        })
    return summary


# ─── ENDPOINTS ────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM patients")
        n_pat = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ips")
        n_ips = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"status": "ok", "patients": n_pat, "ips": n_ips}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ── CLUSTERING: K-MEANS ───────────────────────────────────────

@app.post("/clustering/kmeans")
def run_kmeans(params: KMeansParams):
    """K-Means clustering sobre coordenadas lat/lng de pacientes."""
    t0 = time.time()
    coords, ids = fetch_coordinates()
    if len(coords) == 0:
        raise HTTPException(404, "No hay pacientes en la BD. Ejecuta el ETL primero.")

    model = KMeans(n_clusters=params.k, max_iter=params.max_iter,
                   n_init=params.n_init, random_state=42)
    labels = model.fit_predict(coords)

    metrics = compute_metrics(coords, labels)
    save_cluster_result("kmeans", params.k, params.dict(), metrics)
    update_patient_clusters("cluster_kmeans", ids, labels.tolist())

    elapsed = round(time.time() - t0, 2)
    return {
        "algorithm": "kmeans",
        "k": int(params.k),
        "n_patients": int(len(coords)),
        "elapsed_s": elapsed,
        "metrics": metrics,
        "clusters": cluster_summary(coords, labels),
        "centroids": [
            {"cluster_id": int(i), "lat": round(float(c[0]), 6), "lng": round(float(c[1]), 6)}
            for i, c in enumerate(model.cluster_centers_)
        ],
        "inertia": round(float(model.inertia_), 2),
    }


# ── CLUSTERING: DBSCAN ───────────────────────────────────────

@app.post("/clustering/dbscan")
def run_dbscan(params: DBSCANParams):
    """DBSCAN clustering. eps en km, se convierte a grados."""
    t0 = time.time()
    coords, ids = fetch_coordinates()
    if len(coords) == 0:
        raise HTTPException(404, "No hay pacientes en la BD.")

    eps_deg = km_to_degrees(params.eps_km)
    model = DBSCAN(eps=eps_deg, min_samples=params.min_samples, algorithm="ball_tree", metric="haversine")
    labels = model.fit_predict(np.radians(coords))   # haversine requiere radianes

    n_noise = int((labels == -1).sum())
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    metrics = compute_metrics(coords, labels)
    save_cluster_result("dbscan", n_clusters, params.dict(), metrics)
    update_patient_clusters("cluster_dbscan", ids, labels.tolist())

    elapsed = round(time.time() - t0, 2)
    return {
        "algorithm": "dbscan",
        "eps_km": float(params.eps_km),
        "min_samples": int(params.min_samples),
        "n_clusters_found": int(n_clusters),
        "n_noise_points": int(n_noise),
        "n_patients": int(len(coords)),
        "elapsed_s": elapsed,
        "metrics": metrics,
        "clusters": cluster_summary(coords, labels),
    }


# ── CLUSTERING: GMM ──────────────────────────────────────────

@app.post("/clustering/gmm")
def run_gmm(params: GMMParams):
    """Gaussian Mixture Model clustering."""
    t0 = time.time()
    coords, ids = fetch_coordinates()
    if len(coords) == 0:
        raise HTTPException(404, "No hay pacientes en la BD.")

    model = GaussianMixture(
        n_components=params.n_components,
        covariance_type=params.covariance_type,
        max_iter=params.max_iter,
        random_state=42,
    )
    labels = model.fit_predict(coords)
    probas = model.predict_proba(coords)    # probabilidad de pertenencia por cluster

    metrics = compute_metrics(coords, labels)
    save_cluster_result("gmm", params.n_components, params.dict(), metrics)
    update_patient_clusters("cluster_gmm", ids, labels.tolist())

    elapsed = round(time.time() - t0, 2)
    return {
        "algorithm": "gmm",
        "n_components": params.n_components,
        "covariance_type": params.covariance_type,
        "n_patients": len(coords),
        "elapsed_s": elapsed,
        "converged": bool(model.converged_),
        "n_iter": int(model.n_iter_),
        "bic": round(float(model.bic(coords)), 2),
        "aic": round(float(model.aic(coords)), 2),
        "metrics": metrics,
        "clusters": cluster_summary(coords, labels),
    }


# ── COMPARATIVA DE MÉTRICAS ───────────────────────────────────

@app.get("/clustering/results")
def get_clustering_results():
    """Retorna las últimas ejecuciones de cada algoritmo con métricas."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT DISTINCT ON (algorithm)
            algorithm, k_clusters, params,
            silhouette, davies_bouldin, calinski_harabasz, ejecutado_at
        FROM cluster_results
        ORDER BY algorithm, ejecutado_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"results": [dict(r) for r in rows]}


# ── IPS ───────────────────────────────────────────────────────

@app.get("/ips")
def get_ips():
    """Retorna todas las IPS con ocupación calculada desde Encounters."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            i.*,
            COUNT(e.id) AS pacientes_activos,
            CASE
                WHEN i.capacidad_camas > 0
                THEN ROUND(COUNT(e.id)::numeric / i.capacidad_camas * 100, 1)
                ELSE 0
            END AS pct_ocupacion
        FROM ips i
        LEFT JOIN encounters e ON e.ips_id = i.ips_id AND e.status = 'in-progress'
        GROUP BY i.id
        ORDER BY i.ips_id
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"ips": [dict(r) for r in rows], "total": len(rows)}


# ── PATIENTS GEOJSON ─────────────────────────────────────────

@app.get("/patients/geojson")
def get_patients_geojson(algorithm: str = "kmeans", limit: int = 3500):
    """Retorna pacientes como GeoJSON para Leaflet, coloreados por cluster."""
    col_map = {"kmeans": "cluster_kmeans", "dbscan": "cluster_dbscan", "gmm": "cluster_gmm"}
    col = col_map.get(algorithm, "cluster_kmeans")

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""
        SELECT patient_id, nombre, apellido, lat, lng,
               comuna, barrio, ips_asignada,
               {col} AS cluster_id
        FROM patients
        WHERE lat IS NOT NULL AND lng IS NOT NULL
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [r["lng"], r["lat"]]},
            "properties": {
                "id": r["patient_id"],
                "nombre": f"{r['nombre']} {r['apellido']}",
                "comuna": r["comuna"],
                "barrio": r["barrio"],
                "ips": r["ips_asignada"],
                "cluster": r["cluster_id"],
            },
        }
        for r in rows
    ]
    return {"type": "FeatureCollection", "features": features, "algorithm": algorithm}


# ── FHIR: ENCOUNTER (INGRESO) ─────────────────────────────────

@app.post("/fhir/Encounter")
async def create_encounter(data: EncounterCreate):
    """
    Registra el ingreso de un paciente a una IPS.
    Flujo 1 del taller: POST /fhir/Encounter
    Broadcast WebSocket para actualizar mapa en tiempo real.
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Verificar que existen patient e IPS
    cur.execute("SELECT patient_id, nombre, apellido FROM patients WHERE patient_id = %s", (data.patient_id,))
    patient = cur.fetchone()
    if not patient:
        raise HTTPException(404, f"Paciente {data.patient_id} no encontrado")

    cur.execute("SELECT ips_id, nombre FROM ips WHERE ips_id = %s", (data.ips_id,))
    ips = cur.fetchone()
    if not ips:
        raise HTTPException(404, f"IPS {data.ips_id} no encontrada")

    # Crear Encounter en PostGIS
    from datetime import datetime
    import uuid
    enc_id = f"enc-{uuid.uuid4().hex[:8]}"
    now = datetime.utcnow()

    fhir_resource = {
        "resourceType": "Encounter",
        "id": enc_id,
        "status": "in-progress",
        "class": {"code": data.class_code, "display": "Internación" if data.class_code == "IMP" else "Ambulatorio"},
        "subject": {"reference": f"Patient/{data.patient_id}"},
        "serviceProvider": {"reference": f"Location/{data.ips_id}"},
        "period": {"start": now.isoformat() + "-05:00"},
    }

    cur.execute("""
        INSERT INTO encounters (encounter_id, patient_id, ips_id, status, class_code, fhir_resource)
        VALUES (%s, %s, %s, 'in-progress', %s, %s)
    """, (enc_id, data.patient_id, data.ips_id, data.class_code, json.dumps(fhir_resource)))
    conn.commit()

    # Publicar en HAPI FHIR
    try:
        requests.put(
            f"{FHIR_URL}/Encounter/{enc_id}",
            json=fhir_resource,
            headers={"Content-Type": "application/fhir+json"},
            timeout=5,
        )
    except Exception:
        pass  # HAPI puede no estar listo

    # WebSocket broadcast → actualiza color de marcador en mapa
    await manager.broadcast({
        "event": "encounter_created",
        "encounter_id": enc_id,
        "patient_id": data.patient_id,
        "ips_id": data.ips_id,
        "patient_name": f"{patient['nombre']} {patient['apellido']}",
        "ips_name": ips["nombre"],
        "status": "in-progress",
        "timestamp": now.isoformat(),
    })

    cur.close()
    conn.close()

    return {
        "encounter_id": enc_id,
        "status": "in-progress",
        "fhir_resource": fhir_resource,
        "message": f"Ingreso registrado: {patient['nombre']} → {ips['nombre']}",
    }


# ── FHIR: ENCOUNTER DISCHARGE (ALTA) ─────────────────────────

@app.patch("/fhir/Encounter/{encounter_id}/discharge")
async def discharge_encounter(encounter_id: str):
    """
    Da de alta a un paciente. Flujo 2 del taller: PATCH /fhir/Encounter/{id}
    """
    from datetime import datetime
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT * FROM encounters WHERE encounter_id = %s", (encounter_id,))
    enc = cur.fetchone()
    if not enc:
        raise HTTPException(404, f"Encounter {encounter_id} no encontrado")

    now = datetime.utcnow()
    cur.execute("""
        UPDATE encounters
        SET status = 'finished', period_end = %s
        WHERE encounter_id = %s
    """, (now, encounter_id))
    conn.commit()

    # WebSocket broadcast
    await manager.broadcast({
        "event": "encounter_discharged",
        "encounter_id": encounter_id,
        "ips_id": enc["ips_id"],
        "patient_id": enc["patient_id"],
        "status": "finished",
        "timestamp": now.isoformat(),
    })

    cur.close()
    conn.close()

    return {
        "encounter_id": encounter_id,
        "status": "finished",
        "period_end": now.isoformat(),
        "message": "Alta registrada correctamente",
    }


# ── FHIR: LOCATION QUERY (GEOESPACIAL) ───────────────────────

@app.get("/fhir/Location")
def query_locations(near_lat: float = 3.4516, near_lng: float = -76.5320,
                    radius_km: float = 5.0, status: str = "active"):
    """
    Flujo 3: Query geoespacial FHIR.
    IPS cercanas a una ubicación usando PostGIS ST_DWithin.
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            i.*,
            ST_Distance(
                geom::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            ) / 1000.0 AS distancia_km,
            COUNT(e.id) AS pacientes_activos
        FROM ips i
        LEFT JOIN encounters e ON e.ips_id = i.ips_id AND e.status = 'in-progress'
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            %s
        )
        AND (%s = 'all' OR (habilitada = TRUE))
        GROUP BY i.id
        ORDER BY distancia_km
    """, (near_lng, near_lat, near_lng, near_lat, radius_km * 1000, status))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return {
        "query": {"lat": near_lat, "lng": near_lng, "radius_km": radius_km},
        "total": len(rows),
        "locations": [
            {
                "resourceType": "Location",
                "id": r["ips_id"],
                "name": r["nombre"],
                "tipo": r["tipo"],
                "nivel": r["nivel_atencion"],
                "distancia_km": round(float(r["distancia_km"]), 2),
                "pacientes_activos": r["pacientes_activos"],
                "capacidad_camas": r["capacidad_camas"],
                "pct_ocupacion": round(r["pacientes_activos"] / max(r["capacidad_camas"], 1) * 100, 1),
                "position": {"latitude": r["lat"], "longitude": r["lng"]},
                "commune": r["comuna"],
            }
            for r in rows
        ],
    }


# ── WEBSOCKET ─────────────────────────────────────────────────

@app.websocket("/ws/encounters")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket para actualizaciones en tiempo real del mapa (Bonus +1.0)."""
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()   # mantener conexión viva
    except WebSocketDisconnect:
        manager.disconnect(ws)