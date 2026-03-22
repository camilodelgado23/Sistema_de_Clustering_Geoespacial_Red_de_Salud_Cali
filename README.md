# 🏥 Taller 1 — Sistema de Clustering Geoespacial para Red de Salud Cali
**UAO · Salud Digital · 2026-1**

---

## 📁 Estructura del proyecto

```
Taller_1_Sistema_de_Clustering_Geoespacial_Red_de_Salud_Cali/
├── docker-compose.yml
├── README.md
│
├── data/
│   ├── reps_cali_real.csv      ← NUEVO: fuente real del REPS Minsalud
│   ├── cali_ips.csv            ← generado desde reps_cali_real.csv
│   ├── cali_patients.csv       ← 3.491 pacientes sintéticos
│   ├── comunas_cali.geojson    ← 22 comunas GeoJSON
│   └── generate_data.py        ← lee reps_cali_real.csv y genera los 3 archivos
│
├── database/
│   └── init.sql
│
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   └── etl/
│       ├── __init__.py
│       └── parse_fhir.py
│
└── frontend/
    ├── index.html
    └── comunas_cali.geojson
```

---

## 🚀 Inicio rápido (3 pasos)

### Paso 1 — Levantar el stack

```bash
cd Taller_1_Sistema_de_Clustering_Geoespacial_Red_de_Salud_Cali
docker-compose up --build
```
Espera ~2 minutos la primera vez (descarga imágenes Docker).

**Servicios que levanta:**

| Servicio | URL | Descripción |
|---|---|---|
| Frontend Mapa | http://localhost:3000 | Mapa Leaflet con clustering |
| FastAPI Docs | http://localhost:8001/docs | Swagger UI interactivo |
| HAPI FHIR | http://localhost:8080/fhir | Servidor FHIR R4 |
| PostgreSQL/PostGIS | localhost:5432 | BD espacial |

### Paso 2 — Cargar los datos (ETL)

En otra terminal:

```bash
docker exec cali_fastapi python /app/etl/parse_fhir.py
```

Esto:
- Inserta 45 IPS en PostGIS (con índice GIST)
- Inserta 3.491 pacientes en PostGIS (con índice GIST)
- Publica recursos FHIR Location y Patient en HAPI

### Paso 3 — Ejecutar clustering

**Opción A — Desde el mapa (http://localhost:3000):**
1. Seleccionar algoritmo (K-Means / DBSCAN / GMM)
2. Ajustar parámetros con el slider
3. Click "▶ Ejecutar Clustering"
4. Ver puntos coloreados por cluster en el mapa

**Opción B — Desde Swagger (http://localhost:8001/docs):**
```
POST /clustering/kmeans   {"k": 5}
POST /clustering/dbscan   {"eps_km": 0.5, "min_samples": 5}
POST /clustering/gmm      {"n_components": 5, "covariance_type": "full"}
```

---

## 📊 Endpoints principales

### Clustering
```
POST /clustering/kmeans      → K-Means con métricas
POST /clustering/dbscan      → DBSCAN con métricas
POST /clustering/gmm         → GMM con métricas
GET  /clustering/results     → Comparativa de los 3 modelos
```

### FHIR
```
POST /fhir/Encounter                    → Registrar ingreso paciente
PATCH /fhir/Encounter/{id}/discharge    → Dar de alta
GET  /fhir/Location?near_lat=3.45&near_lng=-76.53&radius_km=5
```

### Datos
```
GET  /ips                    → IPS con ocupación en tiempo real
GET  /patients/geojson       → Pacientes como GeoJSON (para Leaflet)
GET  /health                 → Estado de la BD
```

---

## 🔬 Flujos FHIR implementados

### Flujo 1 — Registrar ingreso
```bash
curl -X POST http://localhost:8001/fhir/Encounter \
  -H "Content-Type: application/json" \
  -d '{"patient_id": "pat-0001", "ips_id": "ips-huc-001"}'
```

### Flujo 2 — Dar de alta
```bash
curl -X PATCH http://localhost:8001/fhir/Encounter/enc-abc123/discharge
```

### Flujo 3 — IPS cercanas (geoespacial)
```bash
curl "http://localhost:8001/fhir/Location?near_lat=3.4516&near_lng=-76.5320&radius_km=3"
```

---

## 📐 Métricas de clustering

| Métrica | Interpretación | Ideal |
|---|---|---|
| **Silhouette** | Cohesión y separación (-1 a 1) | Más alto mejor |
| **Davies-Bouldin** | Similitud entre clusters | Más bajo mejor |
| **Calinski-Harabasz** | Densidad vs separación | Más alto mejor |

---

## ⚡ Bonus: WebSocket tiempo real

El mapa se conecta automáticamente a `ws://localhost:8001/ws/encounters`.
Cuando registras un Encounter (ingreso), el marcador de la IPS cambia de color en tiempo real sin recargar la página.

---

## 🛑 Detener

```bash
docker-compose down          # Mantiene los datos
docker-compose down -v       # Borra TODO 
```

---

## 📦 Datos incluidos

- **45 IPS** basadas en el REPS de Minsalud (datos públicos de Cali)
- **3.491 pacientes sintéticos** generados con Faker + distribución gaussiana por barrio
- **22 comunas** de Cali como polígonos GeoJSON
- Índices espaciales GIST en PostGIS (obligatorio según criterios del taller)
