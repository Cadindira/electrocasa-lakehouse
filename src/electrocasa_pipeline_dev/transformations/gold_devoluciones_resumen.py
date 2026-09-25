from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.devoluciones_resumen",
    comment="Resumen Gold de devoluciones por motivo"
)
def gold_devoluciones_resumen():

    df = spark.read.table(
        "electrocasa.silver.devoluciones"
    )

    return (
        df
        .groupBy("motivo")
        .agg(

            F.countDistinct("devolucion_id")
            .alias("cantidad_devoluciones"),

            F.round(
                F.sum("monto_reembolso"),
                2
            ).alias("monto_total_reembolsado"),

            F.round(
                F.avg("monto_reembolso"),
                2
            ).alias("reembolso_promedio"),

            F.round(
                F.min("monto_reembolso"),
                2
            ).alias("reembolso_minimo"),

            F.round(
                F.max("monto_reembolso"),
                2
            ).alias("reembolso_maximo")
        )
        .orderBy("motivo")
    )
