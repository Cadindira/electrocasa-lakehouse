from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="electrocasa.bronze.resenas",
    comment="Reseñas de clientes ingeridas desde JSON mediante Auto Loader"
)
def bronze_resenas():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("/Volumes/electrocasa/bronze/landing/resenas/")
        .select(
            "resena_id",
            "producto_id",
            "cliente_id",
            "calificacion",
            "comentario",
            "fecha_resena",
            "tags",
            "respuestas",
            "_rescued_data",
            F.current_timestamp().alias("_fecha_ingesta"),
            F.lit("resenas_clientes_json").alias("_sistema_origen")
        )
    )
