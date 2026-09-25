from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="electrocasa.bronze.productos",
    comment="Bronze de productos ingeridos desde JSON mediante Auto Loader"
)
def bronze_productos():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(
            "/Volumes/electrocasa/bronze/landing/productos/"
        )
        .select(
            "producto_id",
            "nombre_producto",
            "categoria",
            "marca",
            "precio_lista",
            "_rescued_data",
            F.current_timestamp().alias("_fecha_ingesta"),
            F.lit(
                "volume:electrocasa/bronze/landing/productos"
            ).alias("_sistema_origen")
        )
    )
