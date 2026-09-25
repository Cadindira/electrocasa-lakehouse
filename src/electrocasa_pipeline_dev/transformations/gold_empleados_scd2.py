from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="electrocasa.gold.empleados_scd2",
    comment="Historial dimensional SCD Tipo 2 de empleados"
)
def gold_empleados_scd2():

    df = spark.read.table(
        "electrocasa.silver.empleados"
    )

    # -------------------------------------------------
    # 1. Detectar conflictos no determinísticos
    # -------------------------------------------------

    ventana_conflicto = Window.partitionBy(
        "id_empleado",
        "fecha_evento",
        "tipo_evento"
    )

    df = (
        df
        .withColumn(
            "_cantidad_mismo_evento",
            F.count("*").over(ventana_conflicto)
        )
    )

    # Excluir los eventos ambiguos del SCD2
    df = df.filter(
        F.col("_cantidad_mismo_evento") == 1
    )

    # -------------------------------------------------
    # 2. Prioridad para eventos del mismo día
    # -------------------------------------------------

    df = (
        df
        .withColumn(
            "_prioridad_evento",
            F.when(
                F.col("tipo_evento") == "alta", 1
            )
            .when(
                F.col("tipo_evento") == "cambio_salario", 2
            )
            .when(
                F.col("tipo_evento") == "transferencia", 3
            )
            .when(
                F.col("tipo_evento") == "baja", 4
            )
            .otherwise(0)
        )
    )

    # -------------------------------------------------
    # 3. Obtener estado final de cada empleado por día
    # -------------------------------------------------

    ventana_dia = (
        Window
        .partitionBy(
            "id_empleado",
            "fecha_evento"
        )
        .orderBy(
            F.col("_prioridad_evento").desc()
        )
    )

    df_diario = (
        df
        .withColumn(
            "_rn",
            F.row_number().over(ventana_dia)
        )
        .filter(
            F.col("_rn") == 1
        )
    )

    # -------------------------------------------------
    # 4. Construir historial SCD Tipo 2
    # -------------------------------------------------

    ventana_historial = (
        Window
        .partitionBy("id_empleado")
        .orderBy("fecha_evento")
    )

    df_scd = (
        df_diario
        .withColumn(
            "version_scd",
            F.row_number().over(ventana_historial)
        )
        .withColumn(
            "_siguiente_fecha",
            F.lead("fecha_evento").over(ventana_historial)
        )
    )

    # -------------------------------------------------
    # 5. Fecha inicio / fecha fin / estado actual
    # -------------------------------------------------

    df_scd = (
        df_scd
        .withColumn(
            "fecha_inicio",
            F.col("fecha_evento")
        )

        .withColumn(
            "fecha_fin",
            F.when(
                F.col("_siguiente_fecha").isNotNull(),
                F.date_sub(
                    F.col("_siguiente_fecha"),
                    1
                )
            )
            .otherwise(
                F.lit("9999-12-31").cast("date")
            )
        )

        .withColumn(
            "estado_empleado",
            F.when(
                F.col("tipo_evento") == "baja",
                F.lit("inactivo")
            )
            .otherwise(
                F.lit("activo")
            )
        )

        .withColumn(
            "es_actual",
            F.when(
                F.col("_siguiente_fecha").isNull(),
                F.lit(True)
            )
            .otherwise(
                F.lit(False)
            )
        )
    )

    return (
        df_scd
        .select(
            "id_empleado",
            "version_scd",
            "nombre",
            "dni",
            "email",
            "sucursal_id",
            "cargo",
            "salario",
            "tipo_evento",
            "estado_empleado",
            "fecha_inicio",
            "fecha_fin",
            "es_actual"
        )
    )
