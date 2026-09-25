# ElectroCasa - Plataforma de Datos en Azure Databricks

## Descripción del proyecto

ElectroCasa es una plataforma de datos desarrollada en Azure Databricks para centralizar y procesar información de una cadena de tiendas de electrodomésticos.

El escenario original presentaba información distribuida entre archivos de ventas, productos, empleados, devoluciones y reseñas, además de información de seguimiento de envíos almacenada en Azure SQL Database. Esta distribución dificultaba disponer de una fuente única y confiable para análisis.

La solución implementa un flujo de datos de extremo a extremo que integra las fuentes, aplica controles de calidad, conserva la trazabilidad de los registros y genera información preparada para consumo analítico.

El proyecto utiliza una arquitectura Lakehouse basada en el patrón Medallion y comprende los siguientes componentes:

- ingesta de datos desde archivos y Azure SQL Database;
- almacenamiento y procesamiento en Bronze, Silver y Gold;
- controles de calidad y cuarentena;
- historización de empleados mediante SCD Tipo 2;
- gobierno y seguridad mediante Unity Catalog;
- protección de información sensible;
- orquestación mediante Databricks Jobs;
- monitoreo de ejecuciones;
- despliegue mediante Databricks Asset Bundles;
- versionamiento mediante Git y GitHub.

---

## 1. Arquitectura

La solución se implementó bajo una arquitectura Lakehouse con patrón Medallion.

```text
                     FUENTES
                        |
          +-------------+-------------+
          |                           |
          v                           v
     CSV / JSON               Azure SQL Database
          |                           |
          v                           v
 Unity Catalog Volume       Lakehouse Federation
          |                           |
          +-------------+-------------+
                        |
                        v
                     BRONZE
                        |
                        v
                     SILVER
                        |
                        +----> CUARENTENA
                        |
                        v
                      GOLD
                        |
                        v
               Consumo analítico
```

El catálogo principal es:

`electrocasa`

La organización por capas utiliza los siguientes esquemas:

- `electrocasa.bronze`
- `electrocasa.silver`
- `electrocasa.gold`

Para la integración con Azure SQL Database se utiliza el catálogo federado:

`electrocasa_tracking`

Este catálogo permite acceder a la información de tracking sin copiar ni administrar manualmente los datos de la fuente externa.

---

## 2. Fuentes de datos

La solución integra seis fuentes:

| Fuente | Formato / origen | Contenido |
|---|---|---|
| Ventas | CSV | Operaciones de venta por sucursal |
| Productos | JSON | Catálogo de productos |
| Empleados | CSV | Eventos de recursos humanos |
| Reseñas | JSON | Opiniones y valoraciones de clientes |
| Devoluciones | CSV | Devoluciones y reembolsos |
| Tracking de envíos | Azure SQL Database | Estado de los despachos |

Los archivos se almacenan en un Volume de Unity Catalog utilizado como zona de landing.

La fuente de tracking se integra mediante Lakehouse Federation utilizando una conexión a Azure SQL Database.

Las credenciales se almacenan en el Secret Scope:

`electrocasa-sql`

El usuario y la contraseña de Azure SQL se recuperan desde el Secret Scope y no se encuentran hardcodeados en notebooks, archivos de configuración ni código versionado en GitHub.

---

## 3. Arquitectura Medallion

### Bronze

Bronze conserva los datos de origen con el menor nivel posible de transformación.

Esta capa recibe información de:

- ventas;
- productos;
- empleados;
- reseñas;
- devoluciones;
- tracking de envíos.

Los registros incluyen información técnica para mantener la trazabilidad de cada carga, como fecha de ingesta, sistema o archivo de origen e identificador de lote.

Bronze constituye el punto de partida para las validaciones posteriores sin modificar la información original necesaria para auditoría.

### Silver

Silver contiene información depurada, normalizada y validada.

Las transformaciones implementadas incluyen:

- eliminación de duplicados;
- tratamiento de valores nulos;
- conversión de tipos;
- estandarización de datos;
- normalización de valores;
- validación de cantidades;
- validación de importes;
- validación de precios;
- tratamiento de fechas;
- validación de referencias;
- identificación de inconsistencias.

Los registros que incumplen reglas críticas se separan del flujo válido y permanecen disponibles en estructuras de cuarentena.

### Gold

Gold contiene datasets orientados al análisis y consumo de información.

Entre las estructuras generadas se encuentran:

- resumen de ventas;
- ventas por categoría;
- tracking por estado;
- resumen de empleados;
- histórico SCD2 de empleados;
- información de devoluciones;
- resumen de reseñas;
- análisis de reseñas por tags.

La capa Gold representa la información consolidada y preparada para consultas analíticas.

---

## 4. Calidad de datos y cuarentena

La exploración de Bronze permitió identificar problemas reales en las fuentes antes de definir las reglas de limpieza.

Entre las principales incidencias tratadas se encuentran:

- registros duplicados;
- importes nulos;
- importes negativos;
- cantidades inválidas;
- sucursales inválidas;
- precios no positivos;
- conflictos de precio para un mismo producto;
- valores no convertibles;
- fechas inválidas.

Los registros válidos continúan hacia Silver. Los registros que incumplen reglas críticas se almacenan en cuarentena, evitando su eliminación silenciosa y conservando información para trazabilidad y auditoría.

### Resultado de las validaciones

| Fuente | Bronze | Silver | Cuarentena |
|---|---:|---:|---:|
| Ventas | 15,225 | 13,886 | 1,114 |
| Productos | 3,030 | 2,849 | 181 |
| Empleados | 4,078 | 4,032 | 46 |
| Devoluciones | 4,040 | 3,892 | 108 |
| Reseñas | 8,080 | 7,705 | 295 |
| Tracking | 5,050 | 5,000 | 0 |

Durante la depuración también se identificaron reingestas y duplicados exactos. Estos registros se eliminan durante el proceso de deduplicación y no forman parte de las tablas Silver finales.

---

## 5. Historización de empleados

La entidad Empleados utiliza una estrategia **Slowly Changing Dimension Tipo 2 (SCD2)**.

El origen contiene diferentes eventos asociados a un mismo empleado, como altas, transferencias, cambios salariales y bajas. Sobrescribir el registro anterior habría eliminado información histórica relevante.

SCD2 conserva cada versión del empleado y permite identificar:

- versiones históricas;
- versión vigente;
- fecha de inicio de vigencia;
- fecha de fin de vigencia;
- estado actual del registro.

La dimensión generada conserva 4,017 versiones correspondientes a 2,000 empleados, incluyendo los cambios registrados durante el periodo procesado.

---

## 6. Unity Catalog y gobierno de datos

Unity Catalog centraliza el gobierno de los objetos utilizados por ElectroCasa.

La estructura principal está compuesta por:

```text
electrocasa
├── bronze
├── silver
└── gold
```

La zona de landing utiliza un Volume de Unity Catalog y los objetos generados por el pipeline quedan administrados dentro del catálogo del proyecto.

La fuente Azure SQL conserva su naturaleza externa y se consulta mediante:

`electrocasa_tracking`

Esta separación mantiene el origen externo desacoplado de las tablas y vistas administradas por el Lakehouse.

---

## 7. Control de acceso

Se definieron tres grupos con responsabilidades diferenciadas:

### `electrocasa_ingenieria`

Acceso de lectura y escritura sobre:

- Bronze
- Silver
- Gold

### `electrocasa_analistas`

Acceso de solo lectura sobre:

- Gold

No cuenta con acceso funcional a Bronze ni Silver.

### `electrocasa_auditoria`

Acceso de solo lectura sobre:

- Gold

Su acceso está orientado a revisión y auditoría de la información publicada.

Los permisos fueron aplicados mediante sentencias `GRANT` y `REVOKE` sobre los objetos de Unity Catalog.

---

## 8. Protección de información sensible

El campo `dni` de Empleados se protege mediante una política de column masking en Unity Catalog.

La función de masking valida la pertenencia al grupo:

`electrocasa_ingenieria`

Los integrantes de Ingeniería visualizan el DNI original. Los usuarios que no pertenecen al grupo reciben el valor:

```text
********
```

La política se aplicó sobre la materialized view de empleados y se validó utilizando un usuario sin pertenencia al grupo de Ingeniería.

### Restricción administrativa del entorno

Los grupos fueron creados a nivel de cuenta y utilizados para asignar privilegios en Unity Catalog.

La cuenta empleada en el laboratorio no posee permisos de Account Admin para modificar directamente determinadas membresías. Por esta razón, la validación del masking se realizó con el usuario disponible como usuario externo al grupo `electrocasa_ingenieria`.

Esta restricción corresponde a la administración de identidades del entorno y no afecta la definición ni la aplicación de la política de masking.

---

## 9. Pipeline de procesamiento

El procesamiento Bronze → Silver → Gold está implementado mediante Lakeflow Declarative Pipelines.

El pipeline principal es:

`electrocasa_pipeline_dev`

El pipeline genera y administra las transformaciones correspondientes a las diferentes fuentes y materializa los resultados dentro de Unity Catalog.

El flujo general es:

```text
Landing / Federation
        |
        v
      Bronze
        |
        v
 Validación y limpieza
        |
        +------> Cuarentena
        |
        v
      Silver
        |
        v
 Transformación analítica
        |
        v
       Gold
```

La ejecución completa del pipeline finalizó correctamente.

---

## 10. Orquestación con Databricks Jobs

La ejecución end-to-end se encuentra orquestada mediante:

`electrocasa_job_dev`

El Job contiene dos tareas con dependencia real:

```text
ejecutar_pipeline_lakehouse
             |
             | ALL_SUCCESS
             v
     validar_lakehouse
```

### `ejecutar_pipeline_lakehouse`

Ejecuta `electrocasa_pipeline_dev` y procesa las capas Bronze, Silver y Gold.

La tarea cuenta con configuración de reintento para manejar fallos transitorios.

### `validar_lakehouse`

Se ejecuta después de la finalización satisfactoria del pipeline.

El notebook realiza validaciones sobre los principales resultados generados y confirma la consistencia del procesamiento end-to-end.

La ejecución completa del Job finalizó con estado:

`SUCCESS`

---

## 11. Programación y alertas

El Job DEV utiliza la siguiente configuración:

- frecuencia: diaria;
- hora: 08:00;
- zona horaria: `America/Lima`;
- máximo de ejecuciones concurrentes: 1;
- reintento configurado para la tarea principal;
- notificación por correo ante fallos;
- cola de ejecución habilitada.

Esta configuración evita ejecuciones simultáneas sobre el mismo flujo y mantiene un historial centralizado de las corridas.

---

## 12. Databricks Asset Bundles

Los recursos del proyecto se definen y despliegan mediante Databricks Asset Bundles.

El archivo raíz es:

`databricks.yml`

Los recursos se encuentran separados en:

```text
resources/
├── electrocasa_job_dev.job.yml
└── electrocasa_pipeline_dev.pipeline.yml
```

El Bundle contiene dos targets:

- `dev`
- `prod`

### Target DEV

Comandos utilizados durante la validación y despliegue:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev electrocasa_job_dev
```

La validación del Bundle finalizó correctamente y la ejecución del Job DEV terminó con estado `SUCCESS`.

### Target PROD

Comandos utilizados:

```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

Los recursos desplegados son:

- `electrocasa_pipeline_prod`
- `electrocasa_job_prod`

El Job PROD permanece con el schedule pausado.

DEV y PROD utilizan el mismo workspace debido al alcance académico del proyecto. Los recursos de cada target mantienen nombres y rutas independientes. La ejecución PROD permanece detenida porque ambos targets comparten el catálogo físico `electrocasa`, evitando que dos pipelines administren simultáneamente los mismos objetos.

---

## 13. Monitoreo

El monitoreo se realizó mediante el historial de ejecuciones de Databricks Jobs.

Durante la ejecución se verificaron:

- estado general del Job;
- estado individual de cada tarea;
- dependencia entre tareas;
- duración de ejecución;
- ejecución del pipeline;
- ejecución de las validaciones finales;
- resultado de la corrida.

La ejecución utilizada como validación final terminó con estado:

`SUCCESS`

El historial de Databricks Jobs conserva la evidencia de las corridas realizadas durante las pruebas.

---

## 14. Estrategia de cómputo y costos

El proyecto utiliza principalmente **Databricks Serverless**.

La carga de trabajo está compuesta por procesos batch de corta duración y una ejecución programada diaria. No existe necesidad de mantener un cluster activo de forma permanente.

Serverless aporta al proyecto:

- cómputo iniciado bajo demanda;
- eliminación de tiempos ociosos de clusters permanentes;
- escalamiento administrado por Databricks;
- menor carga de administración de infraestructura;
- adecuación al patrón de ejecución periódico del pipeline.

La programación diaria limita las ejecuciones innecesarias y concentra el consumo de recursos en los periodos de procesamiento.

---

## 15. Configuración inicial

El notebook:

`notebooks/00_setup.ipynb`

contiene el aprovisionamiento base del proyecto.

Incluye:

- creación del catálogo `electrocasa`;
- creación de los esquemas Bronze, Silver y Gold;
- creación del Volume de landing;
- configuración de rutas;
- recuperación de credenciales desde Secret Scope;
- configuración de la conexión con Azure SQL Database;
- creación del Foreign Catalog;
- creación de grupos;
- asignación de privilegios;
- aplicación de políticas de seguridad.

La conexión utiliza los secretos almacenados en `electrocasa-sql`, por lo que las credenciales no se encuentran expuestas en el repositorio.

---

## 16. Estructura del repositorio

```text
electrocasa-lakehouse/
│
├── README.md
├── databricks.yml
│
├── notebooks/
│   └── 00_setup.ipynb
│
├── resources/
│   ├── electrocasa_job_dev.job.yml
│   └── electrocasa_pipeline_dev.pipeline.yml
│
└── src/
    ├── electrocasa_job_dev/
    │   └── validacion_lakehouse_job.ipynb
    │
    └── electrocasa_pipeline_dev/
        └── transformations/
```

El código del pipeline se mantiene separado de la configuración de los recursos del Bundle, facilitando su versionamiento y despliegue.

---

## 17. Flujo de ejecución

El proceso completo sigue la siguiente secuencia:

```text
1. Fuentes de datos
          |
          v
2. Landing / Lakehouse Federation
          |
          v
3. Bronze
          |
          v
4. Validación y limpieza
          |
          +----> Cuarentena
          |
          v
5. Silver
          |
          v
6. Transformaciones analíticas
          |
          v
7. Gold
          |
          v
8. Validación automática
```

---

## 18. Tecnologías utilizadas

- Azure Databricks
- Lakeflow Declarative Pipelines
- Databricks Jobs
- Databricks Asset Bundles
- Delta Lake
- Unity Catalog
- Databricks CLI
- Azure SQL Database
- Lakehouse Federation
- Secret Scope
- Python
- PySpark
- Spark SQL
- Git
- GitHub

---

## 19. Evidencia de ejecución

Las pruebas realizadas confirmaron el funcionamiento de los principales componentes de la solución:

| Componente | Resultado |
|---|---|
| Pipeline Bronze → Silver → Gold | Ejecutado correctamente |
| Validaciones de calidad | Correctas |
| Cuarentenas | Generadas |
| SCD Tipo 2 | Generado |
| Job end-to-end DEV | `SUCCESS` |
| Bundle DEV | Validado y desplegado |
| Bundle PROD | Validado y desplegado |
| Masking de DNI | Validado |
| Permisos de Unity Catalog | Aplicados |
| Versionamiento GitHub | Completado |

La ejecución del Job DEV verificó en una sola corrida el pipeline y el notebook de validación, confirmando la operación end-to-end del Lakehouse.

---

## 20. Estado final

El proyecto ElectroCasa quedó implementado y versionado con los siguientes componentes:

- integración de las seis fuentes de datos;
- arquitectura Medallion Bronze, Silver y Gold;
- controles de calidad;
- manejo de registros en cuarentena;
- deduplicación;
- historización SCD Tipo 2 para empleados;
- gobierno mediante Unity Catalog;
- grupos y permisos diferenciados;
- masking de DNI;
- integración con Azure SQL Database mediante Lakehouse Federation;
- protección de credenciales mediante Secret Scope;
- orquestación mediante Databricks Jobs;
- ejecución end-to-end satisfactoria;
- monitoreo mediante historial de ejecuciones;
- <img width="1705" height="977" alt="TrackingEnvios completo y validado de Bronze-Silver-Gold" src="https://github.com/user-attachments/assets/ee774a30-6d33-49b3-ada6-413bbfde429f" />

- cómputo Serverless;
- Databricks Asset Bundle con targets DEV y PROD;
- despliegue mediante Databricks CLI;
- código y configuración versionados en GitHub.
