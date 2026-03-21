"""
ETL: parse_fhir.py
Taller 1 — Sistema de Clustering Geoespacial para Red de Salud Cali
UAO 2026-1

Flujo:
  CSV (cali_ips + cali_patients)
    → Genera recursos FHIR (Location + Patient)
    → Inserta en PostgreSQL/PostGIS
    → Publica en HAPI FHIR (opcional)

Ejecución:
  python etl/parse_fhir.py
  # o desde Docker:
  docker exec cali_fastapi python /app/etl/parse_fhir.py
"""

import csv
import json
import os
import psycopg2
import requests
from datetime import datetime

# ─── Configuración ────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "cali_geo"),
    "user": os.getenv("DB_USER", "geo_user"),
    "password": os.getenv("DB_PASSWORD", "geo_pass_2026"),
}

FHIR_URL = os.getenv("FHIR_URL", "http://localhost:8080/fhir")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

IPS_CSV = f"{DATA_DIR}/cali_ips.csv"
PAT_CSV = f"{DATA_DIR}/cali_patients.csv"


# ─── FHIR Resource Builders ───────────────────────────────────

def build_fhir_location(row: dict) -> dict:
    """Construye recurso FHIR R4 Location desde una fila de IPS."""
    return {
        "resourceType": "Location",
        "id": row["ips_id"],
        "status": "active" if row["habilitada"] == "True" else "inactive",
        "name": row["nombre"],
        "description": f"IPS nivel {row['nivel_atencion']} - {row['tipo']}",
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": "HOSP" if row["tipo"] == "HOSPITAL" else "OUTPHARM",
                        "display": row["tipo"],
                    }
                ]
            }
        ],
        "telecom": [
            {"system": "phone", "value": row["telefono"], "use": "work"},
            {"system": "email", "value": row["email"], "use": "work"},
        ],
        "address": {
            "use": "work",
            "line": [row["direccion"]],
            "city": row["municipio"],
            "state": row["departamento"],
            "country": "CO",
        },
        "position": {
            "longitude": float(row["lng"]),
            "latitude": float(row["lat"]),
        },
        "managingOrganization": {
            "display": row["nombre"],
        },
        "extension": [
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/nivel-atencion",
                "valueInteger": int(row["nivel_atencion"]),
            },
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/capacidad-camas",
                "valueInteger": int(row["capacidad_camas"]),
            },
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/comuna",
                "valueInteger": int(row["comuna"]),
            },
        ],
    }


def build_fhir_patient(row: dict) -> dict:
    """Construye recurso FHIR R4 Patient desde una fila de paciente."""
    return {
        "resourceType": "Patient",
        "id": row["patient_id"],
        "identifier": [
            {
                "system": "https://www.registraduria.gov.co",
                "value": row["documento"],
            }
        ],
        "name": [
            {
                "use": "official",
                "family": row["apellido"],
                "given": [row["nombre"]],
            }
        ],
        "gender": row["genero"],
        "birthDate": row["fecha_nacimiento"],
        "telecom": [
            {"system": "phone", "value": row["telefono"], "use": "home"},
        ],
        "address": [
            {
                "use": "home",
                "line": [row["direccion"]],
                "city": "Santiago de Cali",
                "state": "Valle del Cauca",
                "country": "CO",
            }
        ],
        "extension": [
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/comuna",
                "valueInteger": int(row["comuna"]),
            },
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/barrio",
                "valueString": row["barrio"],
            },
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/tipo-sangre",
                "valueString": row["tipo_sangre"],
            },
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/ips-asignada",
                "valueString": row["ips_asignada"],
            },
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/latitud",
                "valueDecimal": float(row["lat"]),
            },
            {
                "url": "http://uao.edu.co/fhir/StructureDefinition/longitud",
                "valueDecimal": float(row["lng"]),
            },
        ],
    }


# ─── PostgreSQL Loaders ───────────────────────────────────────

def load_ips_to_postgis(conn, csv_path: str) -> int:
    """Inserta IPS en PostGIS."""
    cur = conn.cursor()
    count = 0
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute("""
                INSERT INTO ips (
                    ips_id, nombre, tipo, nivel_atencion,
                    lat, lng, comuna, direccion,
                    municipio, departamento, habilitada,
                    capacidad_camas, telefono, email, fhir_location_id
                ) VALUES (
                    %(ips_id)s, %(nombre)s, %(tipo)s, %(nivel_atencion)s,
                    %(lat)s, %(lng)s, %(comuna)s, %(direccion)s,
                    %(municipio)s, %(departamento)s, %(habilitada)s,
                    %(capacidad_camas)s, %(telefono)s, %(email)s, %(fhir_location_id)s
                )
                ON CONFLICT (ips_id) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    lat = EXCLUDED.lat,
                    lng = EXCLUDED.lng;
            """, {
                **row,
                "lat": float(row["lat"]),
                "lng": float(row["lng"]),
                "nivel_atencion": int(row["nivel_atencion"]),
                "capacidad_camas": int(row["capacidad_camas"]),
                "habilitada": row["habilitada"] == "True",
                "comuna": int(row["comuna"]),
            })
            count += 1
    conn.commit()
    cur.close()
    return count


def load_patients_to_postgis(conn, csv_path: str, batch_size=500) -> int:
    """Inserta pacientes en PostGIS por lotes."""
    cur = conn.cursor()
    count = 0
    batch = []

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                _insert_patient_batch(cur, batch)
                conn.commit()
                count += len(batch)
                print(f"  Insertados {count} pacientes...", end="\r")
                batch = []

    if batch:
        _insert_patient_batch(cur, batch)
        conn.commit()
        count += len(batch)

    cur.close()
    return count


def _insert_patient_batch(cur, batch):
    for row in batch:
        cur.execute("""
            INSERT INTO patients (
                patient_id, nombre, apellido, genero,
                fecha_nacimiento, tipo_sangre,
                lat, lng, comuna, barrio, direccion,
                telefono, email, documento,
                ips_asignada, condicion_preexistente, fhir_patient_id
            ) VALUES (
                %(patient_id)s, %(nombre)s, %(apellido)s, %(genero)s,
                %(fecha_nacimiento)s, %(tipo_sangre)s,
                %(lat)s, %(lng)s, %(comuna)s, %(barrio)s, %(direccion)s,
                %(telefono)s, %(email)s, %(documento)s,
                %(ips_asignada)s, %(condicion_preexistente)s, %(fhir_patient_id)s
            )
            ON CONFLICT (patient_id) DO NOTHING;
        """, {
            **row,
            "lat": float(row["lat"]),
            "lng": float(row["lng"]),
            "comuna": int(row["comuna"]),
        })


# ─── FHIR Publisher (HAPI) ────────────────────────────────────

def publish_to_hapi(resources: list, resource_type: str) -> dict:
    """Envía recursos al HAPI FHIR server usando Bundle transaction."""
    bundle = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "fullUrl": f"{FHIR_URL}/{resource_type}/{r['id']}",
                "resource": r,
                "request": {
                    "method": "PUT",
                    "url": f"{resource_type}/{r['id']}",
                },
            }
            for r in resources
        ],
    }
    resp = requests.post(
        f"{FHIR_URL}",
        json=bundle,
        headers={"Content-Type": "application/fhir+json"},
        timeout=60,
    )
    return {"status": resp.status_code, "ok": resp.status_code in (200, 201)}


# ─── MAIN ─────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("ETL Taller 1 — Clustering Geoespacial Cali")
    print("UAO 2026-1")
    print("=" * 60)

    # 1. Conectar a PostGIS
    print("\n[1/4] Conectando a PostGIS...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("  ✅ Conexión OK")

    # 2. Cargar IPS
    print("\n[2/4] Cargando IPS a PostGIS...")
    n_ips = load_ips_to_postgis(conn, IPS_CSV)
    print(f"  ✅ {n_ips} IPS insertadas (con índice GIST)")

    # 3. Cargar Pacientes
    print("\n[3/4] Cargando pacientes a PostGIS...")
    n_pat = load_patients_to_postgis(conn, PAT_CSV)
    print(f"\n  ✅ {n_pat} pacientes insertados (con índice GIST)")

    # 4. Publicar en HAPI FHIR
    print("\n[4/4] Publicando recursos en HAPI FHIR...")
    try:
        # Location resources
        ips_resources = []
        with open(IPS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ips_resources.append(build_fhir_location(row))

        result = publish_to_hapi(ips_resources, "Location")
        if result["ok"]:
            print(f"  ✅ {len(ips_resources)} Location publicados en HAPI FHIR")
        else:
            print(f"  ⚠️  HAPI FHIR devolvió status {result['status']} (puede no estar listo aún)")

        # Patient resources — solo publicar primeros 100 para no saturar
        pat_resources = []
        with open(PAT_CSV, encoding="utf-8") as f:
            for i, row in enumerate(csv.DictReader(f)):
                pat_resources.append(build_fhir_patient(row))
                if i >= 99:
                    break

        result = publish_to_hapi(pat_resources, "Patient")
        if result["ok"]:
            print(f"  ✅ 100 Patient publicados en HAPI FHIR (muestra)")
        else:
            print(f"  ⚠️  HAPI FHIR devolvió status {result['status']}")

    except Exception as e:
        print(f"  ⚠️  No se pudo conectar a HAPI FHIR: {e}")
        print("     (Normal si HAPI aún está iniciando. Vuelve a ejecutar en 60s)")

    conn.close()
    print("\n" + "=" * 60)
    print("✅ ETL completado")
    print(f"   IPS en PostGIS:      {n_ips}")
    print(f"   Pacientes en PostGIS: {n_pat}")
    print("=" * 60)


if __name__ == "__main__":
    main()