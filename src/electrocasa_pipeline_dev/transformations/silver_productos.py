from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# PREPARACIÓN Y NORMALIZACIÓN COMÚN
# ============================================================

def normalizar_productos():
    """
    Prepara los productos provenientes de Bronze.

    - Detecta producto_id con precios conflictivos.
    - Limpia identificadores y campos descriptivos.
    - Reemplaza marca vacía por 'desconocida'.
    - Convierte precio_lista a DOUBLE mediante TRY_CAST.
    - Normaliza las categorías.
    - Conserva indicadores auxiliares para decidir posteriormente
      si el registro pertenece a Silver o a cuarentena.
    """

    df = spark.read.table(
        "electrocasa.bronze.productos"
    )

    # --------------------------------------------------------
    # 1. Detectar IDs con más de un precio diferente
    # --------------------------------------------------------

    ids_conflictivos = (
        df
        .groupBy("producto_id")
        .agg(
            F.countDistinct("precio_lista")
            .alias("precios_distintos")
        )
        .filter(
            F.col("precios_distintos") > 1
        )
        .select("producto_id")
        .withColumn(
            "_conflicto_precio",
            F.lit(True)
        )
    )

    # --------------------------------------------------------
    # 2. Agregar indicador de conflicto
    # --------------------------------------------------------

    df = (
        df
        .join(
            ids_conflictivos,
            on="producto_id",
            how="left"
        )
        .withColumn(
            "_conflicto_precio",
            F.coalesce(
                F.col("_conflicto_precio"),
                F.lit(False)
            )
        )
    )

    # Guardamos el valor original para poder explicar
    # posteriormente los errores en cuarentena.
    df = (
        df
        .withColumn(
            "_precio_lista_original",
            F.col("precio_lista")
        )
    )

    # --------------------------------------------------------
    # 3. Limpiar y tipificar
    # --------------------------------------------------------

    df = (
        df

        .withColumn(
            "producto_id",
            F.trim(F.col("producto_id"))
        )

        .withColumn(
            "nombre_producto",
            F.trim(F.col("nombre_producto"))
        )

        .withColumn(
            "marca",
            F.when(
                F.col("marca").isNull()
                | (F.trim(F.col("marca")) == ""),
                F.lit("desconocida")
            )
            .otherwise(
                F.trim(F.col("marca"))
            )
        )

        .withColumn(
            "precio_lista",
            F.expr(
                "TRY_CAST(precio_lista AS DOUBLE)"
            )
        )
    )

    # --------------------------------------------------------
    # 4. Crear categoría auxiliar normalizada
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "_categoria_normalizada",
            F.lower(
                F.trim(
                    F.translate(
                        F.col("categoria"),
                        "áéíóúÁÉÍÓÚ",
                        "aeiouAEIOU"
                    )
                )
            )
        )
        .withColumn(
            "_categoria_normalizada",
            F.regexp_replace(
                F.col("_categoria_normalizada"),
                "_",
                " "
            )
        )
        .withColumn(
            "_categoria_normalizada",
            F.regexp_replace(
                F.col("_categoria_normalizada"),
                r"\s+",
                " "
            )
        )
    )

    # --------------------------------------------------------
    # 5. Estandarizar categorías
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "categoria",
            F.when(
                F.col("_categoria_normalizada") == "climatizacion",
                "climatizacion"
            )
            .when(
                F.col("_categoria_normalizada") == "cocina",
                "cocina"
            )
            .when(
                F.col("_categoria_normalizada") == "electronica",
                "electronica"
            )
            .when(
                F.col("_categoria_normalizada") == "entretenimiento",
                "entretenimiento"
            )
            .when(
                F.col("_categoria_normalizada") == "linea blanca",
                "linea_blanca"
            )
            .otherwise(
                F.col("_categoria_normalizada")
            )
        )
        .drop("_categoria_normalizada")

        .withColumn(
            "_fecha_transformacion",
            F.current_timestamp()
        )
    )

    return df


# ============================================================
# CONDICIÓN DE PRODUCTO VÁLIDO
# ============================================================

def condicion_producto_valido():

    return (
        (~F.col("_conflicto_precio"))

        & F.col("producto_id").isNotNull()
        & (F.col("producto_id") != "")

        & F.col("nombre_producto").isNotNull()
        & (F.col("nombre_producto") != "")

        & F.col("categoria").isNotNull()
        & (F.col("categoria") != "")

        & F.col("precio_lista").isNotNull()
        & (F.col("precio_lista") > 0)
    )


# ============================================================
# SILVER
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.productos",
    comment="Productos depurados, tipificados y estandarizados desde Bronze"
)
@dp.expect_or_drop(
    "producto_id_valido",
    "producto_id IS NOT NULL AND TRIM(producto_id) <> ''"
)
@dp.expect_or_drop(
    "nombre_producto_valido",
    "nombre_producto IS NOT NULL AND TRIM(nombre_producto) <> ''"
)
@dp.expect_or_drop(
    "categoria_valida",
    "categoria IS NOT NULL AND TRIM(categoria) <> ''"
)
@dp.expect_or_drop(
    "precio_lista_valido",
    "precio_lista IS NOT NULL AND precio_lista > 0"
)
def silver_productos():

    df = normalizar_productos()

    return (
        df
        .filter(
            condicion_producto_valido()
        )
        .drop(
            "_conflicto_precio",
            "_precio_lista_original"
        )
    )


# ============================================================
# CUARENTENA
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.productos_cuarentena",
    comment="Productos rechazados por conflictos o problemas de calidad en el precio"
)
def productos_cuarentena():

    df = normalizar_productos()

    df_invalidos = (
        df
        .filter(
            ~condicion_producto_valido()
        )
    )

    return (
        df_invalidos
        .withColumn(
            "_motivo_cuarentena",

            # Se prioriza el conflicto de precio porque no podemos
            # seleccionar arbitrariamente cuál de los precios es correcto.
            F.when(
                F.col("_conflicto_precio"),
                "conflicto_precio"
            )

            .when(
                F.col("producto_id").isNull()
                | (F.col("producto_id") == ""),
                "producto_id_invalido"
            )

            .when(
                F.col("nombre_producto").isNull()
                | (F.col("nombre_producto") == ""),
                "nombre_producto_invalido"
            )

            .when(
                F.col("categoria").isNull()
                | (F.col("categoria") == ""),
                "categoria_invalida"
            )

            .when(
                F.col("_precio_lista_original").isNull()
                | (F.trim(F.col("_precio_lista_original")) == ""),
                "precio_nulo_vacio"
            )

            .when(
                F.col("precio_lista").isNull(),
                "precio_no_convertible"
            )

            .when(
                F.col("precio_lista") <= 0,
                "precio_no_positivo"
            )

            .otherwise(
                "otro_error_calidad"
            )
        )
    )
