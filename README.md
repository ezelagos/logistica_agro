# Logistica Agro – Spark Data Platform (Medallion Architecture)

Este repositorio presenta una plataforma de datos orientada a logística agroindustrial, implementada con Apache Spark y organizada bajo la arquitectura Medallion (RAW → BRONZE → SILVER → GOLD).

El objetivo del proyecto es demostrar el diseño de pipelines de datos, limpieza, transformación, control de calidad y generación de métricas de negocio, con foco en criterios técnicos y escalabilidad, no en volumen de datos.

---

## Arquitectura General

La plataforma sigue el patrón Medallion:

- RAW: generación e ingestión de eventos crudos (CSV / Parquet)
- BRONZE: estandarización, tipado y persistencia estructurada
- SILVER: limpieza, validaciones, reglas de negocio y contratos de datos
- GOLD: KPIs y modelos analíticos orientados a negocio

Cada capa está implementada como jobs Spark independientes, pensados para ser orquestados por Airflow o ejecutados de forma manual.

---

## Dominio implementado end-to-end

El dominio `cerdos` está implementado completamente de punta a punta:

- Generación de eventos RAW
- Ingesta BRONZE en formato Parquet
- Transformaciones SILVER con validaciones de calidad
- KPIs GOLD (producción, conversión alimentaria, mortandad y ciclo productivo)

Este dominio se utiliza como caso de referencia completo para mostrar:

- diseño de contratos de datos
- control de errores entre capas
- consistencia del modelo
- interpretación de métricas desde la lógica de negocio

---

## Otros dominios

Los dominios:

- camiones  
- cereales  
- ganaderia  
- insumos  
- semillero  

se incluyen como estructura base (scaffolding) para demostrar cómo la plataforma escala a múltiples verticales manteniendo los mismos principios arquitectónicos.

No todos los dominios están ejecutados end-to-end por una decisión deliberada de alcance.

---

## Estructura del repositorio

generators/ Generación de datos RAW sintéticos
spark/jobs/
├─ bronze/ Ingesta y normalización inicial
├─ silver/ Limpieza y validaciones
└─ gold/ KPIs y modelos analíticos
architecture/ Decisiones y documentación de diseño
docker/ Entorno Spark
scripts/ Utilidades de bootstrap

---

## Ejecución local (ejemplo dominio cerdos)

```bash
# 1) Generar RAW
python -m generators.cerdos.gen_cerdos_events --date 2026-01-09

# 2) BRONZE
spark-submit spark/jobs/bronze/bronze_cerdos_ingest.py 2026-01-09

# 3) SILVER
spark-submit spark/jobs/silver/silver_cerdos_transform.py 2026-01-09

# 4) GOLD
spark-submit spark/jobs/gold/gold_cerdos_kpis.py 2026-01-09


Preparado para Cloud

Las rutas de datos utilizan variables de entorno (RAW_BASE, BRONZE_BASE, SILVER_BASE, GOLD_BASE), permitiendo una migración directa a Google Cloud Storage u otros object stores sin modificar la lógica de negocio.

Enfoque del proyecto

Este repositorio prioriza:

diseño de pipelines de datos

claridad arquitectónica

separación de responsabilidades

control de calidad de datos

métricas de negocio accionables

No busca ser un sistema productivo completo, sino una demostración técnica sólida de prácticas de Data Engineering con Spark.

Autor

Ezequiel Lagos
Data Engineer
GitHub: https://github.com/ezelagos