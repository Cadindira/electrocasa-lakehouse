from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="electrocasa.silver.empleados_scd2_conflictos",
    comment="Eventos de empleados cuya secuencia no puede determinarse para construir el SCD Tipo 2"
)
def empleados_scd2_conflictos():

    df = spark.read.table(
        "electrocasa.silver.empleados"
    )

    ventana = Window.partitionBy(
        "id_empleado",
        "fecha_evento",
        "tipo_evento"
    )

    return (
        df
        .withColumn(
            "_cantidad_mismo_evento",
            F.count("*").over(ventana)
        )
        .filter(
            F.col("_cantidad_mismo_evento") > 1
        )
        .withColumn(
            "_motivo_conflicto",
            F.lit("multiples_eventos_mismo_tipo_misma_fecha")
        )
    )
