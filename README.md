# Proyecto: Ecobici CDMX — Databricks Lakehouse

## Objetivo

Proyecto de portafolio que simula el trabajo de un Data Engineer en una empresa real, usando datos públicos de movilidad de la Ciudad de México (Ecobici). El objetivo es doble:

1. Construir un pipeline completo tipo Lakehouse (Bronze → Silver → Gold) sobre Databricks.
2. Servir como preparación práctica para la certificación **Databricks Data Engineer Associate**, cubriendo específicamente: Unity Catalog (gobernanza y permisos), tablas externas vs gestionadas, ingesta incremental, Lakeflow Jobs y monitoreo con System Tables.

## Fuentes de datos

- **API GBFS de Ecobici** (`https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json`): estado de estaciones en tiempo real (snapshots periódicos vía script de Python).
- **CSVs históricos de viajes**: datos abiertos publicados por la Ciudad de México, descargados manualmente y almacenados en un Volume.

## Arquitectura

Arquitectura medallion sobre Unity Catalog:

```
cdmx_project (catalog)
├── bronze   (datos crudos, sin transformar)
├── silver   (datos limpios y validados)
└── gold     (datos agregados, listos para análisis/dashboard)
```

Los archivos crudos (CSVs descargados y snapshots JSON de la API) se almacenan primero en un **Volume** (`cdmx_project.bronze.raw_data`) antes de convertirse en tablas Delta.

## Decisiones técnicas

### Gobernanza y permisos (Unity Catalog)

Se configuró un esquema de permisos diferenciado por rol:
- Rol **engineer** (yo): control total sobre `bronze` y `silver`.
- Rol **analista** (usuario de prueba): acceso de solo lectura (`SELECT`) sobre `gold` únicamente.

```sql
GRANT USE CATALOG ON CATALOG cdmx_project TO `analista_demo`;
GRANT USE SCHEMA ON SCHEMA cdmx_project.gold TO `analista_demo`;
GRANT SELECT ON SCHEMA cdmx_project.gold TO `analista_demo`;
```

### Tablas externas vs gestionadas

Se usaron ambos tipos deliberadamente para entender la diferencia en el ciclo de vida de los datos:

| Tabla | Tipo | Por qué |
|---|---|---|
| `bronze.trips_history` | **Externa** (`LOCATION` explícito) | Los datos ya existían como archivos en el Volume; Databricks solo referencia la ubicación, no la posee. |
| `bronze.estaciones_snapshot` | **Gestionada** (sin `LOCATION`) | Datos generados por el propio pipeline (snapshots de la API); tiene sentido que Databricks controle su ciclo de vida completo. |

**Prueba realizada:** al ejecutar `DROP TABLE` sobre la tabla gestionada, los archivos Delta físicos se eliminaron. Al ejecutar `DROP TABLE` sobre la tabla externa, solo se eliminó el metadata — los archivos Delta en el Volume permanecieron intactos.

```sql
-- Tabla externa (los archivos sobreviven al DROP TABLE)
CREATE TABLE IF NOT EXISTS cdmx_project.bronze.trips_history
USING DELTA
LOCATION '/Volumes/cdmx_project/bronze/external_tables/trips_history/'
AS SELECT * FROM read_files(
  '/Volumes/cdmx_project/bronze/raw_data/trips/',
  format => 'csv',
  header => true,
  inferSchema => true
)
```

```python
# Tabla gestionada (los archivos se borran junto con la tabla)
df = spark.sql("""
    SELECT * FROM read_files(
        '/Volumes/cdmx_project/bronze/raw_data/*station_status.json',
        format => 'json'
    )
""")
df.write.format("delta").mode("append").saveAsTable("cdmx_project.bronze.estaciones_snapshot")
```

### Ingesta de snapshots de la API

Se usa un script de Python que descarga el feed GBFS con timestamp en el nombre del archivo, permitiendo acumular snapshots históricos y, más adelante, habilitar ingesta incremental (Auto Loader) sobre los archivos nuevos.

## Próximos pasos

- [ ] Transformación y validación de datos (capa Silver)
- [ ] Ingesta incremental con Auto Loader / Delta Live Tables
- [ ] Orquestación con Lakeflow Jobs
- [ ] Capa Gold y dashboard en Databricks SQL
- [ ] Monitoreo con System Tables
- [ ] Preparación final para el examen de certificación

## Cómo correrlo

*(pendiente de documentar una vez que el pipeline esté completo)*
