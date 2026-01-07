#Arquitectura Medallion

Este proyecto implementa una arquitectura Medallion (Bronze/Silver/Gold)
para garantizar trazabilidad, calidad y escalabilidad de los datos logisticos

- Bronze: datos crudos, append-only
- Silver: datos limpios y normalizados
- Gold: modelos listos para consumo analitico

El diseño es local-first pero cloud-ready para GCP