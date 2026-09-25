# ElectroCasa - Plataforma Lakehouse en Azure Databricks

## 1. Descripción del proyecto

ElectroCasa es un proyecto de Data Engineering desarrollado sobre Azure Databricks.

El objetivo es implementar una plataforma Lakehouse que permita integrar,
procesar, depurar y disponibilizar información proveniente de diferentes
fuentes de datos utilizando una arquitectura Medallion.

La solución comprende:

- ingesta de archivos;
- integración con Azure SQL Database;
- transformación de datos en capas Bronze, Silver y Gold;
- controles de calidad;
- manejo de registros rechazados;
- Slowly Changing Dimension Tipo 2;
- gobierno mediante Unity Catalog;
- control de acceso;
- masking de información sensible;
- orquestación mediante Databricks Jobs;
- despliegue mediante Databricks Asset Bundles.

---

## 2. Arquitectura

La solución utiliza una arquitectura Lakehouse basada en el patrón Medallion.

```text
                    FUENTES
                       |
        +--------------+---------------+
        |                              |
        v                              v
   Archivos CSV                 Azure SQL Database
        |                              |
        v                              v
 Unity Catalog Volume          Lakehouse Federation
        |                              |
        +---------------+--------------+
                        |
                        v
                     BRONZE
                        |
                        v
                     SILVER
                        |
                        v
                      GOLD
                        |
                        v
                Consumo analítico

