from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.resenas_por_tag",
    comment="Resumen de reseñas por tag a partir del arreglo semiestructurado tags"
)
def gold_resenas_por_tag():

    df = spark.read.table(
        "electrocasa.silver.resenas"
    )

    df_tags = (
        df
        .select(
            "resena_id",
            "calificacion",
            F.explode("tags").alias("tag")
        )
    )

    return (
        df_tags
        .groupBy("tag")
        .agg(

            F.countDistinct("resena_id")
            .alias("cantidad_resenas"),

            F.round(
                F.avg("calificacion"),
                2
            ).alias("calificacion_promedio")
        )
        .orderBy(
            F.desc("cantidad_resenas")
        )
    )
