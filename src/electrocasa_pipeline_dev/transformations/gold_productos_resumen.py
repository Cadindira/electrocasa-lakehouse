from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.productos_resumen",
    comment="Resumen Gold de productos por categoría"
)
def gold_productos_resumen():

    df = spark.read.table(
        "electrocasa.silver.productos"
    )

    return (
        df
        .groupBy("categoria")
        .agg(
            F.countDistinct("producto_id")
                .alias("cantidad_productos"),

            F.round(
                F.avg("precio_lista"),
                2
            ).alias("precio_promedio"),

            F.round(
                F.min("precio_lista"),
                2
            ).alias("precio_minimo"),

            F.round(
                F.max("precio_lista"),
                2
            ).alias("precio_maximo")
        )
        .orderBy("categoria")
    )
