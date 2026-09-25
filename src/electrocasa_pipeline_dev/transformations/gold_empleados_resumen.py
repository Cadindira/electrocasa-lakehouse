from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="electrocasa.gold.empleados_resumen",
    comment="Resumen Gold de movimientos de empleados por tipo de evento"
)
def gold_empleados_resumen():

    df = spark.read.table(
        "electrocasa.silver.empleados"
    )

    return (
        df
        .groupBy("tipo_evento")
        .agg(
            F.count("*").alias("cantidad_eventos"),

            F.countDistinct("id_empleado")
             .alias("cantidad_empleados"),

            F.round(
                F.avg("salario"),
                2
            ).alias("salario_promedio"),

            F.round(
                F.min("salario"),
                2
            ).alias("salario_minimo"),

            F.round(
                F.max("salario"),
                2
            ).alias("salario_maximo")
        )
        .orderBy("tipo_evento")
    )
