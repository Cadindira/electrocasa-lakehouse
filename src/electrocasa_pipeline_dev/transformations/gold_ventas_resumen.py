from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.ventas_resumen",
    comment="Resumen Gold de ventas por canal y método de pago"
)
def gold_ventas_resumen():

    df = spark.read.table(
        "electrocasa.silver.ventas"
    )

    return (
        df
        .groupBy(
            "canal",
            "metodo_pago"
        )
        .agg(
            F.countDistinct("venta_id").alias("cantidad_ventas"),

            F.sum("cantidad").alias("cantidad_unidades"),

            F.round(
                F.sum("monto_total"),
                2
            ).alias("monto_total_ventas"),

            F.round(
                F.avg("monto_total"),
                2
            ).alias("ticket_promedio")
        )
        .orderBy(
            "canal",
            "metodo_pago"
        )
    )
