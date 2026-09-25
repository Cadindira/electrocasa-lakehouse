from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.tracking_por_estado",
    comment="Resumen Gold de envíos agrupados por estado de entrega"
)
def gold_tracking_por_estado():

    df = spark.read.table(
        "electrocasa.silver.tracking_envios"
    )

    return (
        df
        .groupBy("estado_entrega")
        .agg(
            F.count("*").alias("cantidad_envios")
        )
        .orderBy("estado_entrega")
    )
