from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.ventas_por_categoria",
    comment="Resumen Gold de ventas enriquecidas con categoría de producto"
)
def gold_ventas_por_categoria():

    ventas = spark.read.table(
        "electrocasa.silver.ventas"
    ).alias("v")

    productos = spark.read.table(
        "electrocasa.silver.productos"
    ).alias("p")

    df = (
        ventas
        .join(
            productos,
            F.col("v.producto_id") == F.col("p.producto_id"),
            "left"
        )
        .select(
            F.col("v.venta_id").alias("venta_id"),
            F.col("v.cantidad").alias("cantidad"),
            F.col("v.monto_total").alias("monto_total"),

            F.coalesce(
                F.col("p.categoria"),
                F.lit("sin_clasificar")
            ).alias("categoria")
        )
    )

    return (
        df
        .groupBy("categoria")
        .agg(
            F.countDistinct("venta_id")
                .alias("cantidad_ventas"),

            F.sum("cantidad")
                .alias("unidades_vendidas"),

            F.round(
                F.sum("monto_total"),
                2
            ).alias("monto_total_ventas"),

            F.round(
                F.avg("monto_total"),
                2
            ).alias("ticket_promedio")
        )
        .orderBy("categoria")
    )
