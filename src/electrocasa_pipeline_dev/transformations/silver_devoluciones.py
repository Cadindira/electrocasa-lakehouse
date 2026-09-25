from pyspark import pipelines as dp
from pyspark.sql import functions as F


def normalizar_devoluciones():

    df = spark.read.table(
        "electrocasa.bronze.devoluciones"
    )

    # 1. Eliminar duplicados exactos de negocio
    df = df.dropDuplicates([
        "devolucion_id",
        "pedido_id",
        "sucursal_id",
        "producto_id",
        "motivo",
        "monto_reembolso",
        "fecha_devolucion"
    ])

    # 2. Normalizar campos de texto
    df = (
        df
        .withColumn(
            "devolucion_id",
            F.trim(F.col("devolucion_id"))
        )
        .withColumn(
            "pedido_id",
            F.trim(F.col("pedido_id"))
        )
        .withColumn(
            "sucursal_id",
            F.trim(F.col("sucursal_id"))
        )
        .withColumn(
            "producto_id",
            F.trim(F.col("producto_id"))
        )
    )

    # 3. Normalizar motivo
    df = (
        df
        .withColumn(
            "motivo",
            F.when(
                F.col("motivo").isNull()
                | (F.trim(F.col("motivo")) == ""),
                "sin_clasificar"
            )
            .otherwise(
                F.lower(F.trim(F.col("motivo")))
            )
        )
    )

    # 4. Fecha técnica
    df = (
        df
        .withColumn(
            "_fecha_transformacion",
            F.current_timestamp()
        )
    )

    return df


def condicion_devolucion_valida():

    return (
        F.col("devolucion_id").isNotNull()
        & (F.col("devolucion_id") != "")

        & F.col("pedido_id").isNotNull()
        & (F.col("pedido_id") != "")

        & F.col("sucursal_id").isNotNull()
        & (F.col("sucursal_id") != "")

        & F.col("producto_id").isNotNull()
        & (F.col("producto_id") != "")

        & F.col("monto_reembolso").isNotNull()
        & (F.col("monto_reembolso") > 0)

        & F.col("fecha_devolucion").isNotNull()
    )

@dp.materialized_view(
    name="electrocasa.silver.devoluciones",
    comment="Devoluciones validadas, deduplicadas y normalizadas"
)
def silver_devoluciones():

    df = normalizar_devoluciones()

    return (
        df
        .filter(condicion_devolucion_valida())
    )

@dp.materialized_view(
    name="electrocasa.silver.devoluciones_cuarentena",
    comment="Devoluciones rechazadas por reglas de calidad"
)
def devoluciones_cuarentena():

    df = normalizar_devoluciones()

    df_invalidos = (
        df
        .filter(
            ~condicion_devolucion_valida()
        )
    )

    return (
        df_invalidos
        .withColumn(
            "_motivo_cuarentena",

            F.when(
                F.col("devolucion_id").isNull()
                | (F.col("devolucion_id") == ""),
                "devolucion_id_invalido"
            )

            .when(
                F.col("pedido_id").isNull()
                | (F.col("pedido_id") == ""),
                "pedido_id_invalido"
            )

            .when(
                F.col("sucursal_id").isNull()
                | (F.col("sucursal_id") == ""),
                "sucursal_invalida"
            )

            .when(
                F.col("producto_id").isNull()
                | (F.col("producto_id") == ""),
                "producto_id_invalido"
            )

            .when(
                F.col("monto_reembolso").isNull()
                | (F.col("monto_reembolso") <= 0),
                "monto_reembolso_invalido"
            )

            .when(
                F.col("fecha_devolucion").isNull(),
                "fecha_devolucion_invalida"
            )

            .otherwise(
                "otro_error_calidad"
            )
        )
    )
