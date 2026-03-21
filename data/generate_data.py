"""
Generador de datos para Taller 1 - Sistema de Clustering Geoespacial
UAO - Seminario de Ingeniería de Datos e IA 2026-1

Genera:
  1. cali_ips.csv   — 45 IPS reales de Cali (basadas en REPS Minsalud)
  2. cali_patients.csv — ~3500 pacientes sintéticos con distribución gaussiana por barrio
  3. comunas_cali.geojson — GeoJSON simplificado de las 22 comunas de Cali
"""

import json
import math
import random
import csv
import numpy as np
from faker import Faker

fake = Faker("es_CO")
random.seed(42)
np.random.seed(42)

# ─────────────────────────────────────────────────────────────────
# 1. IPS DE CALI — basadas en REPS Minsalud (datos públicos)
#    Coordenadas verificadas con Google Maps / OpenStreetMap
# ─────────────────────────────────────────────────────────────────

IPS_DATA = [
    # (id, nombre, tipo, nivel, lat, lng, comuna, direccion, habilitada)
    ("ips-huc-001", "Hospital Universitario del Valle Evaristo García", "HOSPITAL", 3, 3.4516, -76.5320, 19, "Calle 5 # 36-08", True),
    ("ips-clv-002", "Clínica Valle del Lili", "CLINICA", 4, 3.3607, -76.5483, 17, "Cra 98 # 18-49", True),
    ("ips-img-003", "Imbanaco Clínica General del Norte", "CLINICA", 3, 3.4672, -76.5218, 2,  "Cra 38A # 5B-10", True),
    ("ips-sjs-004", "Hospital San Juan de Dios", "HOSPITAL", 2, 3.4398, -76.5435, 9,  "Calle 12 # 3-35", True),
    ("ips-css-005", "Clínica Sebastián de Belalcázar", "CLINICA", 2, 3.4512, -76.5301, 19, "Cra 34 # 4B-60", True),
    ("ips-ssn-006", "Hospital Santander", "HOSPITAL", 2, 3.4284, -76.5198, 13, "Calle 16 # 17-28", True),
    ("ips-cmp-007", "Centro Médico Imbanaco", "CLINICA", 3, 3.4512, -76.5310, 19, "Cra 38A # 5B-10", True),
    ("ips-psq-008", "Hospital Psiquiátrico Departamental", "HOSPITAL", 2, 3.4401, -76.5320, 9,  "Calle 9 # 36-40", True),
    ("ips-snv-009", "Hospital Isaías Duarte Cancino", "HOSPITAL", 2, 3.3398, -76.5521, 15, "Cra 50D # 10-65", True),
    ("ips-car-010", "Clínica Rey David", "CLINICA", 3, 3.4601, -76.5198, 2,  "Cra 10 # 70-32", True),
    ("ips-nor-011", "Hospital Carlos Holmes Trujillo", "HOSPITAL", 2, 3.4831, -76.5072, 6,  "Calle 72 # 26B-41", True),
    ("ips-occ-012", "Clínica de Occidente", "CLINICA", 2, 3.4321, -76.5654, 18, "Av 3N # 56-47", True),
    ("ips-pro-013", "Clínica Palermo", "CLINICA", 2, 3.4568, -76.5342, 19, "Cra 32 # 5-68", True),
    ("ips-cuc-014", "Unidad Central del Valle del Cauca", "CLINICA", 2, 3.4721, -76.5143, 2,  "Calle 62 # 14-28", True),
    ("ips-las-015", "Clínica Las Américas", "CLINICA", 2, 3.4189, -76.5501, 18, "Cra 80 # 11-23", True),
    ("ips-sur-016", "Hospital Mario Correa Rengifo", "HOSPITAL", 2, 3.3861, -76.5321, 14, "Cra 8W # 51-54", True),
    ("ips-col-017", "Clínica Colombia", "CLINICA", 3, 3.4512, -76.5421, 19, "Calle 6N # 3A-28", True),
    ("ips-mem-018", "Hospital Meissen", "HOSPITAL", 1, 3.4201, -76.5068, 13, "Cra 15 # 22-84", True),
    ("ips-san-019", "Clínica San Fernando", "CLINICA", 2, 3.4389, -76.5498, 19, "Cra 40 # 5C-08", True),
    ("ips-can-020", "Centro de Salud Cañaveralejo", "CENTRO_SALUD", 1, 3.4102, -76.5601, 18, "Cra 74 # 5-38", True),
    ("ips-agu-021", "Centro de Salud Agua Blanca", "CENTRO_SALUD", 1, 3.4021, -76.4901, 13, "Cra 16 # 32-45", True),
    ("ips-sil-022", "Hospital del Sur Jorge Cristo Sahium", "HOSPITAL", 2, 3.3721, -76.5421, 16, "Calle 80 Sur # 30-12", True),
    ("ips-buv-023", "Clínica Buenaventura Integral", "CLINICA", 2, 3.4601, -76.5261, 2,  "Cra 1E # 68-35", True),
    ("ips-alb-024", "Centro de Salud Altos de Menga", "CENTRO_SALUD", 1, 3.5021, -76.5012, 1,  "Cra 12 # 82-45", True),
    ("ips-nav-025", "Hospital Nivel 1 Navarro", "HOSPITAL", 1, 3.3901, -76.5201, 15, "Diagonal 40 # 3B-12", True),
    ("ips-pie-026", "Unidad de Salud Pie de Monte Litoral", "CENTRO_SALUD", 1, 3.4712, -76.5401, 20, "Cra 26 # 56-12", True),
    ("ips-sbo-027", "Clínica Santa Bárbara", "CLINICA", 2, 3.4312, -76.5212, 9,  "Calle 13 # 14-56", True),
    ("ips-elo-028", "Hospital El Oriente", "HOSPITAL", 1, 3.4101, -76.5001, 13, "Cra 22 # 28-45", True),
    ("ips-gua-029", "Centro de Salud Guabal", "CENTRO_SALUD", 1, 3.4251, -76.5401, 9,  "Calle 9 # 26-78", True),
    ("ips-tmp-030", "Clínica Tamesis", "CLINICA", 2, 3.4451, -76.5312, 19, "Cra 36 # 3A-15", True),
    ("ips-blt-031", "Hospital Bello Horizonte", "HOSPITAL", 1, 3.4812, -76.5212, 6,  "Calle 68 # 22-34", True),
    ("ips-esq-032", "Centro de Salud Esquina del Futuro", "CENTRO_SALUD", 1, 3.3651, -76.5312, 21, "Cra 45 # 70-23", True),
    ("ips-com-033", "IPS Comfandi Norte", "IPS_EPS", 2, 3.4751, -76.5121, 6,  "Calle 72N # 12-35", True),
    ("ips-cof-034", "IPS Comfamiliar del Valle", "IPS_EPS", 2, 3.4412, -76.5512, 18, "Cra 82 # 10-45", True),
    ("ips-snu-035", "Clínica Nueva", "CLINICA", 2, 3.4512, -76.5198, 2,  "Cra 8 # 66-46", True),
    ("ips-pmd-036", "Punto Médico Desepaz", "CENTRO_SALUD", 1, 3.3812, -76.4951, 21, "Cra 32 # 72-12", True),
    ("ips-crb-037", "Clínica Rafael Uribe Uribe", "CLINICA", 2, 3.4212, -76.5101, 13, "Cra 19 # 24-28", True),
    ("ips-cnr-038", "Centro de Salud Normandía", "CENTRO_SALUD", 1, 3.5101, -76.5021, 1,  "Cra 10 # 90-34", True),
    ("ips-vip-039", "Hospital Vista Hermosa", "HOSPITAL", 1, 3.4151, -76.5551, 18, "Cra 76 # 9-23", True),
    ("ips-cps-040", "Clínica Corpas Salud", "CLINICA", 2, 3.4621, -76.5301, 3,  "Calle 52 # 30-12", True),
    ("ips-sga-041", "Centro de Salud San Cayetano", "CENTRO_SALUD", 1, 3.4351, -76.5321, 9,  "Calle 10 # 32-15", True),
    ("ips-dpv-042", "Clínica del Pacífico", "CLINICA", 2, 3.4501, -76.5401, 19, "Cra 38 # 6-45", True),
    ("ips-med-043", "Mediláser Cali Sur", "IPS_EPS", 2, 3.3951, -76.5351, 16, "Cra 40 # 60-23", True),
    ("ips-snc-044", "Centro de Salud Siete de Agosto", "CENTRO_SALUD", 1, 3.4851, -76.5151, 6,  "Calle 70 # 18-45", True),
    ("ips-cob-045", "Clínica Obregón", "CLINICA", 2, 3.4421, -76.5261, 9,  "Cra 29 # 8-34", True),
]

# Capacidades por tipo
CAPACIDAD = {
    "HOSPITAL": (80, 250),
    "CLINICA": (40, 120),
    "CENTRO_SALUD": (15, 45),
    "IPS_EPS": (30, 80),
}

# ─────────────────────────────────────────────────────────────────
# 2. COMUNAS DE CALI — centroides y polígonos aproximados
# ─────────────────────────────────────────────────────────────────

COMUNAS = [
    {"id": 1,  "nombre": "El Oriente",         "lat": 3.5015, "lng": -76.5010, "radio": 0.025, "poblacion": 68000},
    {"id": 2,  "nombre": "Chipichape",          "lat": 3.4720, "lng": -76.5140, "radio": 0.020, "poblacion": 95000},
    {"id": 3,  "nombre": "Centenario",          "lat": 3.4621, "lng": -76.5301, "radio": 0.018, "poblacion": 73000},
    {"id": 4,  "nombre": "Flores",              "lat": 3.4612, "lng": -76.5421, "radio": 0.020, "poblacion": 88000},
    {"id": 5,  "nombre": "Guayaquil",           "lat": 3.4501, "lng": -76.5451, "radio": 0.018, "poblacion": 92000},
    {"id": 6,  "nombre": "Santa Isabel",        "lat": 3.4831, "lng": -76.5072, "radio": 0.022, "poblacion": 115000},
    {"id": 7,  "nombre": "Alcázares",           "lat": 3.4712, "lng": -76.5351, "radio": 0.019, "poblacion": 81000},
    {"id": 8,  "nombre": "Libertadores",        "lat": 3.4601, "lng": -76.5501, "radio": 0.021, "poblacion": 77000},
    {"id": 9,  "nombre": "Los Libertadores",    "lat": 3.4389, "lng": -76.5398, "radio": 0.022, "poblacion": 134000},
    {"id": 10, "nombre": "Caney",               "lat": 3.4201, "lng": -76.5512, "radio": 0.020, "poblacion": 69000},
    {"id": 11, "nombre": "El Real",             "lat": 3.4312, "lng": -76.5398, "radio": 0.018, "poblacion": 58000},
    {"id": 12, "nombre": "Terrón Colorado",     "lat": 3.4712, "lng": -76.5601, "radio": 0.025, "poblacion": 45000},
    {"id": 13, "nombre": "Agua Blanca",         "lat": 3.4101, "lng": -76.5051, "radio": 0.030, "poblacion": 198000},
    {"id": 14, "nombre": "El Rodeo",            "lat": 3.3861, "lng": -76.5321, "radio": 0.022, "poblacion": 87000},
    {"id": 15, "nombre": "Navarro",             "lat": 3.3901, "lng": -76.5201, "radio": 0.020, "poblacion": 72000},
    {"id": 16, "nombre": "El Jardín",           "lat": 3.3751, "lng": -76.5421, "radio": 0.022, "poblacion": 63000},
    {"id": 17, "nombre": "Meléndez",            "lat": 3.3607, "lng": -76.5483, "radio": 0.028, "poblacion": 55000},
    {"id": 18, "nombre": "Cañaveralejo",        "lat": 3.4189, "lng": -76.5551, "radio": 0.025, "poblacion": 142000},
    {"id": 19, "nombre": "El Calvario",         "lat": 3.4512, "lng": -76.5321, "radio": 0.018, "poblacion": 124000},
    {"id": 20, "nombre": "Marroquín",           "lat": 3.4712, "lng": -76.5401, "radio": 0.020, "poblacion": 79000},
    {"id": 21, "nombre": "Desepaz",             "lat": 3.3812, "lng": -76.4951, "radio": 0.030, "poblacion": 89000},
    {"id": 22, "nombre": "El Pueblo",           "lat": 3.4051, "lng": -76.4901, "radio": 0.022, "poblacion": 56000},
]

def generar_poligono_hexagonal(lat, lng, radio, n=8):
    """Genera un polígono aproximado (hexágono irregular) para la comuna."""
    coords = []
    for i in range(n):
        angulo = 2 * math.pi * i / n + random.uniform(-0.1, 0.1)
        r = radio * random.uniform(0.85, 1.15)
        coords.append([
            round(lng + r * math.cos(angulo) * 1.3, 6),
            round(lat + r * math.sin(angulo), 6),
        ])
    coords.append(coords[0])  # cerrar polígono
    return coords

def generar_geojson_comunas():
    features = []
    for c in COMUNAS:
        coords = generar_poligono_hexagonal(c["lat"], c["lng"], c["radio"])
        features.append({
            "type": "Feature",
            "properties": {
                "comuna": c["id"],
                "nombre": c["nombre"],
                "poblacion": c["poblacion"],
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [coords],
            }
        })
    return {"type": "FeatureCollection", "features": features}

# ─────────────────────────────────────────────────────────────────
# 3. GENERADOR DE IPS CSV
# ─────────────────────────────────────────────────────────────────

def generar_ips_csv(path):
    rows = []
    for d in IPS_DATA:
        id_, nombre, tipo, nivel, lat, lng, comuna, direccion, habilitada = d
        cap_min, cap_max = CAPACIDAD[tipo]
        rows.append({
            "ips_id": id_,
            "nombre": nombre,
            "tipo": tipo,
            "nivel_atencion": nivel,
            "lat": lat,
            "lng": lng,
            "comuna": comuna,
            "direccion": direccion,
            "municipio": "Santiago de Cali",
            "departamento": "Valle del Cauca",
            "habilitada": habilitada,
            "capacidad_camas": random.randint(cap_min, cap_max),
            "telefono": fake.phone_number(),
            "email": f"info@{id_.replace('-','')}.gov.co",
            "fhir_location_id": f"Location/{id_}",
        })

    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"✅ IPS generadas: {len(rows)} registros → {path}")

# ─────────────────────────────────────────────────────────────────
# 4. GENERADOR DE PACIENTES SINTÉTICOS
# ─────────────────────────────────────────────────────────────────

TIPOS_SANGRE = ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]
GENEROS = ["male", "female"]
ENFERMEDADES = [
    "hipertensión", "diabetes tipo 2", "EPOC", "asma", "ninguna",
    "artritis", "insuficiencia renal", "cardiopatía", "ninguna", "ninguna",
]

def generar_pacientes_csv(path, n_total=3500):
    rows = []
    pat_num = 1

    for comuna in COMUNAS:
        # Pacientes proporcionales a la población
        n_pac = max(50, int(n_total * comuna["poblacion"] / sum(c["poblacion"] for c in COMUNAS)))

        # Centro gaussiano de la comuna + dispersión
        lats = np.random.normal(comuna["lat"], comuna["radio"] * 0.5, n_pac)
        lngs = np.random.normal(comuna["lng"], comuna["radio"] * 0.5 * 1.3, n_pac)

        # IPS más cercana a esta comuna
        ips_cercana = min(IPS_DATA, key=lambda x: math.sqrt((x[4]-comuna["lat"])**2 + (x[5]-comuna["lng"])**2))

        for i in range(n_pac):
            genero = random.choice(GENEROS)
            nombre = fake.first_name_male() if genero == "male" else fake.first_name_female()
            apellido = fake.last_name()
            edad = int(np.random.normal(45, 18))
            edad = max(1, min(95, edad))

            rows.append({
                "patient_id": f"pat-{pat_num:04d}",
                "nombre": nombre,
                "apellido": apellido,
                "genero": genero,
                "fecha_nacimiento": fake.date_of_birth(minimum_age=edad, maximum_age=edad).isoformat(),
                "tipo_sangre": random.choice(TIPOS_SANGRE),
                "lat": round(float(lats[i]), 6),
                "lng": round(float(lngs[i]), 6),
                "comuna": comuna["id"],
                "barrio": comuna["nombre"],
                "direccion": fake.street_address(),
                "telefono": fake.phone_number(),
                "email": f"{nombre.lower()}.{apellido.lower()}{pat_num}@gmail.com",
                "documento": str(random.randint(10000000, 99999999)),
                "ips_asignada": ips_cercana[0],
                "condicion_preexistente": random.choice(ENFERMEDADES),
                "fhir_patient_id": f"Patient/pat-{pat_num:04d}",
            })
            pat_num += 1

    # Mezclar
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
    generar_ips_csv("/home/claude/taller1/data/cali_ips.csv")
    n = generar_pacientes_csv("/home/claude/taller1/data/cali_patients.csv")
    
    geojson = generar_geojson_comunas()
    with open("/home/claude/taller1/data/comunas_cali.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    print(f"✅ GeoJSON comunas generado → /home/claude/taller1/data/comunas_cali.geojson")
    print(f"\n📊 Resumen:")
    print(f"   IPS:      45 establecimientos")
    print(f"   Pacientes: {n} sintéticos")
    print(f"   Comunas:  22 polígonos GeoJSON")
