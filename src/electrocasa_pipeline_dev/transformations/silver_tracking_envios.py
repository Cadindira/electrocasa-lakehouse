from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.silver.tracking_envios",
    comment="TrackingEnvios depurado y estandarizado desde Bronze"
)
@dp.expect_or_drop(
    "tracking_id_no_nulo",
    "tracking_id IS NOT NULL"
)
@dp.expect_or_drop(
    "pedido_id_no_nulo",
    "pedido_id IS NOT NULL"
)
@dp.expect_or_drop(
    "estado_entrega_valido",
    "estado_entrega IN ('pendiente', 'en_transito', 'entregado', 'devuelto')"
)
def silver_tracking_envios():

    df = spark.read.table(
        "electrocasa.bronze.tracking_envios"
    )

    # Eliminar duplicados exactos de negocio
    df_sin_duplicados = df.dropDuplicates([
        "tracking_id",
        "pedido_id",
        "courier",
        "estado_entrega",
        "sucursal_origen",
        "fecha_actualizacion"
    ])

    # Normalizar estado_entrega
    df_normalizado = (
        df_sin_duplicados
        .withColumn(
            "estado_entrega",
            F.when(
                F.lower(F.trim(F.col("estado_entrega"))) == "devuelto",
                "devuelto"
            )
            .when(
                F.lower(F.trim(F.col("estado_entrega"))) == "entregado",
                "entregado"
            )
            .when(
                F.lower(F.trim(F.col("estado_entrega"))).isin(
                    "en_camino",
                    "en camino",
                    "en_transito"
                ),
                "en_transito"
            )
            .when(
                F.lower(F.trim(F.col("estado_entrega"))) == "pendiente",
                "pendiente"
            )
            .otherwise(
                F.lower(F.trim(F.col("estado_entrega")))
            )
        )
        .withColumn(
            "_fecha_transformacion",
            F.current_timestamp()
        )
    )

    return df_normalizado
