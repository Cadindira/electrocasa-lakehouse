from pyspark import pipelines as dp
from pyspark.sql import functions as F


def normalizar_resenas():

    df = spark.read.table(
        "electrocasa.bronze.resenas"
    )

    # 1. Eliminar duplicados exactos de negocio
    df = df.dropDuplicates([
        "resena_id",
        "producto_id",
        "cliente_id",
        "calificacion",
        "comentario",
        "fecha_resena",
        "tags",
        "respuestas"
    ])

    # 2. Normalizar identificadores
    df = (
        df
        .withColumn(
            "resena_id",
            F.trim(F.col("resena_id"))
        )
        .withColumn(
            "producto_id",
            F.trim(F.col("producto_id"))
        )
        .withColumn(
            "cliente_id",
            F.trim(F.col("cliente_id"))
        )
    )

    # 3. Normalizar comentario
    df = (
        df
        .withColumn(
            "comentario",
            F.when(
                F.col("comentario").isNull()
                | (F.trim(F.col("comentario")) == ""),
                F.lit("sin_comentario")
            )
            .otherwise(
                F.trim(F.col("comentario"))
            )
        )
    )

    # 4. Convertir fecha STRING -> DATE
    df = (
        df
        .withColumn(
            "fecha_resena",
            F.to_date(
                F.col("fecha_resena"),
                "yyyy-MM-dd"
            )
        )
    )

    # 5. Fecha técnica de transformación
    df = (
        df
        .withColumn(
            "_fecha_transformacion",
            F.current_timestamp()
        )
    )

    return df

def condicion_resena_valida():

    return (
        F.col("resena_id").isNotNull()
        & (F.col("resena_id") != "")

        & F.col("producto_id").isNotNull()
        & (F.col("producto_id") != "")

        & F.col("cliente_id").isNotNull()
        & (F.col("cliente_id") != "")

        & F.col("calificacion").isNotNull()
        & F.col("calificacion").between(1, 5)

        & F.col("fecha_resena").isNotNull()
    )


@dp.materialized_view(
    name="electrocasa.silver.resenas",
    comment="Reseñas de clientes deduplicadas, normalizadas y validadas"
)
def silver_resenas():

    df = normalizar_resenas()

    return (
        df
        .filter(
            condicion_resena_valida()
        )
    )

@dp.materialized_view(
    name="electrocasa.silver.resenas_cuarentena",
    comment="Reseñas rechazadas por reglas de calidad"
)
def resenas_cuarentena():

    df = normalizar_resenas()

    df_invalidos = (
        df
        .filter(
            ~condicion_resena_valida()
        )
    )

    return (
        df_invalidos
        .withColumn(
            "_motivo_cuarentena",

            F.when(
                F.col("resena_id").isNull()
                | (F.col("resena_id") == ""),
                "resena_id_invalido"
            )

            .when(
                F.col("producto_id").isNull()
                | (F.col("producto_id") == ""),
                "producto_id_invalido"
            )

            .when(
                F.col("cliente_id").isNull()
                | (F.col("cliente_id") == ""),
                "cliente_id_invalido"
            )

            .when(
                F.col("calificacion").isNull()
                | (F.col("calificacion") < 1)
                | (F.col("calificacion") > 5),
                "calificacion_invalida"
            )

            .when(
                F.col("fecha_resena").isNull(),
                "fecha_resena_invalida"
            )

            .otherwise(
                "otro_error_calidad"
            )
        )
    )
