from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.resenas_resumen",
    comment="Resumen de reseñas por calificación"
)
def gold_resenas_resumen():

    df = spark.read.table(
        "electrocasa.silver.resenas"
    )

    return (
        df
        .groupBy("calificacion")
        .agg(

            F.countDistinct("resena_id")
            .alias("cantidad_resenas"),

            F.sum(
                F.when(
                    F.col("comentario") == "sin_comentario",
                    1
                ).otherwise(0)
            ).alias("sin_comentario"),

            F.sum(
                F.when(
                    F.col("comentario") != "sin_comentario",
                    1
                ).otherwise(0)
            ).alias("con_comentario"),

            F.sum(
                F.when(
                    F.size("respuestas") > 0,
                    1
                ).otherwise(0)
            ).alias("con_respuestas"),

            F.sum(
                F.when(
                    F.col("respuestas").isNull()
                    | (F.size("respuestas") == 0),
                    1
                ).otherwise(0)
            ).alias("sin_respuestas")
        )
        .orderBy("calificacion")
    )
