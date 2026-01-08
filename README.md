# Logística Agro — Data Platform Demo

Pipeline de datos end-to-end basado en arquitectura Medallion
utilizando Python y Apache Spark.

## 🎯 Objetivo
Demostrar diseño y ejecución de pipelines de datos reales:
- Ingesta RAW
- Transformación BRONZE → SILVER
- Agregaciones GOLD (KPIs)
- Validaciones de contratos de datos

## 🧱 Arquitectura
RAW → BRONZE → SILVER → GOLD

Cada capa cumple un rol claro:
- RAW: datos crudos simulados
- BRONZE: persistencia confiable
- SILVER: validación y normalización
- GOLD: métricas de negocio

## 🚀 Ejecución rápida (demo)

```bash
# RAW
python -m generators.cerdos.gen_cerdos_events --date 2026-01-09

# BRONZE
spark-submit spark/jobs/bronze/bronze_cerdos_ingest.py 2026-01-09

# SILVER
spark-submit spark/jobs/silver/silver_cerdos_transform.py 2026-01-09

# GOLD
spark-submit spark/jobs/gold/gold_cerdos_kpis.py 2026-01-09

🛠 Stack

Python 3.10

Apache Spark 4.x

Arquitectura Medallion

Preparado para ejecución en GCP