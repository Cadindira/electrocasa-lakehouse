from pyspark import pipelines as dp
from pyspark.sql import functions as F


def normalizar_empleados():

    df = spark.read.table(
        "electrocasa.bronze.empleados"
    )

    return (
        df

        # 1. Limpiar campos de texto
        .withColumn(
            "id_empleado",
            F.trim(F.col("id_empleado"))
        )

        .withColumn(
            "nombre",
            F.trim(F.col("nombre"))
        )

        .withColumn(
            "dni",
            F.trim(F.col("dni"))
        )

        .withColumn(
            "email",
            F.lower(F.trim(F.col("email")))
        )

        .withColumn(
            "sucursal_id",
            F.upper(F.trim(F.col("sucursal_id")))
        )

        .withColumn(
            "cargo",
            F.trim(F.col("cargo"))
        )

        # 2. Preparar tipo_evento para normalización
        .withColumn(
            "_evento_normalizado",
            F.lower(
                F.regexp_replace(
                    F.trim(F.col("tipo_evento")),
                    " ",
                    "_"
                )
            )
        )

        # 3. Normalizar tipos de evento
        .withColumn(
            "tipo_evento",
            F.when(
                F.col("_evento_normalizado") == "alta",
                "alta"
            )
            .when(
                F.col("_evento_normalizado") == "baja",
                "baja"
            )
            .when(
                F.col("_evento_normalizado") == "cambio_salario",
                "cambio_salario"
            )
            .when(
                F.col("_evento_normalizado") == "transferencia",
                "transferencia"
            )
            .otherwise(F.col("_evento_normalizado"))
        )

        .drop("_evento_normalizado")

        # 4. Fecha técnica de transformación
        .withColumn(
            "_fecha_transformacion",
            F.current_timestamp()
        )
    )


def condicion_empleado_valido():

    return (
        F.col("id_empleado").isNotNull()
        & (F.col("id_empleado") != "")
        & F.col("nombre").isNotNull()
        & (F.col("nombre") != "")
        & F.col("sucursal_id").isNotNull()
        & (F.col("sucursal_id") != "")
        & F.col("cargo").isNotNull()
        & (F.col("cargo") != "")
        & F.col("fecha_evento").isNotNull()
        & F.col("salario").isNotNull()
        & (F.col("salario") > 0)
        & F.col("tipo_evento").isin(
            "alta",
            "baja",
            "cambio_salario",
            "transferencia"
        )
    )

@dp.materialized_view(
    name="electrocasa.silver.empleados",
    comment="Eventos de empleados validados y normalizados"
)
def silver_empleados():

    df = normalizar_empleados()

    return (
        df
        .filter(condicion_empleado_valido())
    )


@dp.materialized_view(
    name="electrocasa.silver.empleados_cuarentena",
    comment="Registros de empleados rechazados por reglas de calidad"
)
def empleados_cuarentena():

    df = normalizar_empleados()

    return (
        df
        .filter(~condicion_empleado_valido())

        .withColumn(
            "_motivo_cuarentena",
            F.when(
                F.col("id_empleado").isNull()
                | (F.col("id_empleado") == ""),
                "id_empleado_invalido"
            )
            .when(
                F.col("nombre").isNull()
                | (F.col("nombre") == ""),
                "nombre_invalido"
            )
            .when(
                F.col("fecha_evento").isNull(),
                "fecha_evento_nula"
            )
            .when(
                F.col("salario").isNull()
                | (F.col("salario") <= 0),
                "salario_invalido"
            )
            .when(
                F.col("sucursal_id").isNull()
                | (F.col("sucursal_id") == ""),
                "sucursal_invalida"
            )
            .when(
                F.col("cargo").isNull()
                | (F.col("cargo") == ""),
                "cargo_invalido"
            )
            .when(
                ~F.col("tipo_evento").isin(
                    "alta",
                    "baja",
                    "cambio_salario",
                    "transferencia"
                ),
                "tipo_evento_invalido"
            )
            .otherwise("otro_error_calidad")
        )
    )
