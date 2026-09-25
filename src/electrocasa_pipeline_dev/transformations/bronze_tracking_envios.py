from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="tracking_envios",
    comment="Bronze raw de TrackingEnvios proveniente de Azure SQL mediante Lakehouse Federation"
)
def tracking_envios():
    return (
        spark.read
        .table("electrocasa_tracking.dbo.trackingenvios")
        .select(
            "tracking_id",
            "pedido_id",
            "courier",
            "estado_entrega",
            "sucursal_origen",
            "fecha_actualizacion"
        )
        .withColumn("_fecha_ingesta", F.current_timestamp())
        .withColumn(
            "_sistema_origen",
            F.lit("azure_sql:electrocasadb.dbo.TrackingEnvios")
        )
        .withColumn(
            "_id_lote",
            F.concat(
                F.lit("tracking_"),
                F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
            )
        )
    )
