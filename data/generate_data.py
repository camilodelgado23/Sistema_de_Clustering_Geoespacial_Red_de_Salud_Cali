"""
Generador de datos para Taller 1 - Sistema de Clustering Geoespacial
UAO - Seminario de Ingeniería de Datos e IA 2026-1

Genera:
  1. cali_ips.csv       — 45 IPS reales del REPS Minsalud (datos públicos)
  2. cali_patients.csv  — ~3500 pacientes sintéticos con distribución gaussiana por barrio
  3. comunas_cali.geojson — GeoJSON de las 22 comunas de Cali
"""

import json
import math
import random
import csv
import os
import numpy as np
from faker import Faker

fake = Faker("es_CO")
random.seed(42)
np.random.seed(42)

# ─────────────────────────────────────────────────────────────────
# 1. IPS — desde REPS Minsalud (archivo real descargado)
# ─────────────────────────────────────────────────────────────────

REPS_PATH = os.path.join(os.path.dirname(__file__), "reps_cali_real.csv")

def cargar_ips_reps(path=REPS_PATH):
    """Carga las IPS reales del REPS. Fallback a datos hardcodeados si no existe el archivo."""
    if not os.path.exists(path):
        print(f"⚠️  No se encontró {path}. Usando IPS hardcodeadas.")
        return None

    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Convertir a la estructura que espera el resto del código
    ips_list = []
    for r in rows:
        ips_list.append({
            "ips_id":          r["ips_id"],
            "nombre":          r["nombre"],
            "tipo":            r["tipo"],
            "nivel_atencion":  int(r["nivel_atencion"]),
            "lat":             float(r["lat"]),
            "lng":             float(r["lng"]),
            "comuna":          r.get("comuna", ""),
            "direccion":       r["direccion"],
            "habilitada":      r["habilitada"] == "True",
            "capacidad_camas": int(r["capacidad_camas"]),
            "telefono":        r.get("telefono", ""),
            "email":           r.get("email", ""),
            "fhir_location_id": r["fhir_location_id"],
            "codigo_habilitacion": r.get("codigo_habilitacion", ""),
            "nit":             r.get("nit", ""),
            "naturaleza":      r.get("naturaleza", ""),
            "ese":             r.get("ese", "NO"),
        })

    print(f"✅ {len(ips_list)} IPS cargadas desde REPS Minsalud ({path})")
    return ips_list

# ─────────────────────────────────────────────────────────────────
# 2. COMUNAS DE CALI — centroides y polígonos aproximados
# ─────────────────────────────────────────────────────────────────

COMUNAS = [
    {"id": 1,  "nombre": "El Oriente",      "lat": 3.5015, "lng": -76.5010, "radio": 0.025, "poblacion": 68000},
    {"id": 2,  "nombre": "Chipichape",       "lat": 3.4720, "lng": -76.5140, "radio": 0.020, "poblacion": 95000},
    {"id": 3,  "nombre": "Centenario",       "lat": 3.4621, "lng": -76.5301, "radio": 0.018, "poblacion": 73000},
    {"id": 4,  "nombre": "Flores",           "lat": 3.4612, "lng": -76.5421, "radio": 0.020, "poblacion": 88000},
    {"id": 5,  "nombre": "Guayaquil",        "lat": 3.4501, "lng": -76.5451, "radio": 0.018, "poblacion": 92000},
    {"id": 6,  "nombre": "Santa Isabel",     "lat": 3.4831, "lng": -76.5072, "radio": 0.022, "poblacion": 115000},
    {"id": 7,  "nombre": "Alcázares",        "lat": 3.4712, "lng": -76.5351, "radio": 0.019, "poblacion": 81000},
    {"id": 8,  "nombre": "Libertadores",     "lat": 3.4601, "lng": -76.5501, "radio": 0.021, "poblacion": 77000},
    {"id": 9,  "nombre": "Los Libertadores", "lat": 3.4389, "lng": -76.5398, "radio": 0.022, "poblacion": 134000},
    {"id": 10, "nombre": "Caney",            "lat": 3.4201, "lng": -76.5512, "radio": 0.020, "poblacion": 69000},
    {"id": 11, "nombre": "El Real",          "lat": 3.4312, "lng": -76.5398, "radio": 0.018, "poblacion": 58000},
    {"id": 12, "nombre": "Terrón Colorado",  "lat": 3.4712, "lng": -76.5601, "radio": 0.025, "poblacion": 45000},
    {"id": 13, "nombre": "Agua Blanca",      "lat": 3.4101, "lng": -76.5051, "radio": 0.030, "poblacion": 198000},
    {"id": 14, "nombre": "El Rodeo",         "lat": 3.3861, "lng": -76.5321, "radio": 0.022, "poblacion": 87000},
    {"id": 15, "nombre": "Navarro",          "lat": 3.3901, "lng": -76.5201, "radio": 0.020, "poblacion": 72000},
    {"id": 16, "nombre": "El Jardín",        "lat": 3.3751, "lng": -76.5421, "radio": 0.022, "poblacion": 63000},
    {"id": 17, "nombre": "Meléndez",         "lat": 3.3607, "lng": -76.5483, "radio": 0.028, "poblacion": 55000},
    {"id": 18, "nombre": "Cañaveralejo",     "lat": 3.4189, "lng": -76.5551, "radio": 0.025, "poblacion": 142000},
    {"id": 19, "nombre": "El Calvario",      "lat": 3.4512, "lng": -76.5321, "radio": 0.018, "poblacion": 124000},
    {"id": 20, "nombre": "Marroquín",        "lat": 3.4712, "lng": -76.5401, "radio": 0.020, "poblacion": 79000},
    {"id": 21, "nombre": "Desepaz",          "lat": 3.3812, "lng": -76.4951, "radio": 0.030, "poblacion": 89000},
    {"id": 22, "nombre": "El Pueblo",        "lat": 3.4051, "lng": -76.4901, "radio": 0.022, "poblacion": 56000},
]



# ─────────────────────────────────────────────────────────────────
# 3. GENERADOR DE IPS CSV
# ─────────────────────────────────────────────────────────────────

def generar_ips_csv(path, ips_data):
    """Escribe el cali_ips.csv desde los datos del REPS."""
    fieldnames = [
        "ips_id", "nombre", "tipo", "nivel_atencion", "lat", "lng",
        "comuna", "direccion", "municipio", "departamento", "habilitada",
        "capacidad_camas", "telefono", "email", "fhir_location_id",
        "codigo_habilitacion", "nit", "naturaleza", "ese",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in ips_data:
            row["municipio"] = "Santiago de Cali"
            row["departamento"] = "Valle del Cauca"
            w.writerow(row)
    print(f"✅ IPS escritas: {len(ips_data)} registros → {path}")

# ─────────────────────────────────────────────────────────────────
# 4. GENERADOR DE PACIENTES SINTÉTICOS
# ─────────────────────────────────────────────────────────────────

TIPOS_SANGRE = ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]
GENEROS = ["male", "female"]
ENFERMEDADES = [
    "hipertensión", "diabetes tipo 2", "EPOC", "asma", "ninguna",
    "artritis", "insuficiencia renal", "cardiopatía", "ninguna", "ninguna",
]

def generar_pacientes_csv(path, ips_data, n_total=3500):
    rows = []
    pat_num = 1

    for comuna in COMUNAS:
        n_pac = max(50, int(n_total * comuna["poblacion"] / sum(c["poblacion"] for c in COMUNAS)))

        lats = np.random.normal(comuna["lat"], comuna["radio"] * 0.5, n_pac)
        lngs = np.random.normal(comuna["lng"], comuna["radio"] * 0.5 * 1.3, n_pac)

        # IPS más cercana a esta comuna (usando los datos reales del REPS)
        ips_cercana = min(ips_data, key=lambda x: math.sqrt(
            (x["lat"] - comuna["lat"])**2 + (x["lng"] - comuna["lng"])**2
        ))

        for i in range(n_pac):
            genero = random.choice(GENEROS)
            nombre = fake.first_name_male() if genero == "male" else fake.first_name_female()
            apellido = fake.last_name()
            edad = max(1, min(95, int(np.random.normal(45, 18))))

            rows.append({
                "patient_id":              f"pat-{pat_num:04d}",
                "nombre":                  nombre,
                "apellido":                apellido,
                "genero":                  genero,
                "fecha_nacimiento":        fake.date_of_birth(minimum_age=edad, maximum_age=edad).isoformat(),
                "tipo_sangre":             random.choice(TIPOS_SANGRE),
                "lat":                     round(float(lats[i]), 6),
                "lng":                     round(float(lngs[i]), 6),
                "comuna":                  comuna["id"],
                "barrio":                  comuna["nombre"],
                "direccion":               fake.street_address(),
                "telefono":                fake.phone_number(),
                "email":                   f"{nombre.lower()}.{apellido.lower()}{pat_num}@gmail.com",
                "documento":               str(random.randint(10000000, 99999999)),
                "ips_asignada":            ips_cercana["ips_id"],
                "condicion_preexistente":  random.choice(ENFERMEDADES),
                "fhir_patient_id":         f"Patient/pat-{pat_num:04d}",
            })
            pat_num += 1

    random.shuffle(rows)

    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"✅ Pacientes generados: {len(rows)} registros → {path}")
    return len(rows)

# ─────────────────────────────────────────────────────────────────
# 5. MAIN
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    base = os.path.dirname(__file__)

    # Cargar IPS reales del REPS
    ips_data = cargar_ips_reps()
    if ips_data is None:
        print("❌ Coloca reps_cali_real.csv en la carpeta data/ y vuelve a ejecutar.")
        exit(1)

    # Generar archivos
    generar_ips_csv(os.path.join(base, "cali_ips.csv"), ips_data)
    n = generar_pacientes_csv(os.path.join(base, "cali_patients.csv"), ips_data)

    # GeoJSON comunas: oficial del IDESC Cali (requerido)
    geojson_path = os.path.join(base, "comunas_cali.geojson")
    if not os.path.exists(geojson_path):
        print("❌ Falta comunas_cali.geojson en data/")
        print("   Ejecuta: python data/convertir_gdb.py")
        exit(1)
    print(f"✅ GeoJSON oficial encontrado → {geojson_path}")

    print(f"\n📊 Resumen:")
    print(f"   IPS:       {len(ips_data)} establecimientos (REPS Minsalud reales)")
    print(f"   Pacientes: {n} sintéticos")
    print(f"   Comunas:   22 polígonos GeoJSON (IDESC Cali oficial)")

    print(f"\n📊 Resumen:")
    print(f"   IPS:       {len(ips_data)} establecimientos (REPS Minsalud reales)")
    print(f"   Pacientes: {n} sintéticos")
    print(f"   Comunas:   22 polígonos GeoJSON (IDESC Cali oficial)")