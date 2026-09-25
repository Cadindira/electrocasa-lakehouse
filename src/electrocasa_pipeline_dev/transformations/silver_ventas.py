from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# NORMALIZACIÓN COMÚN
# ============================================================

def normalizar_ventas():
    """
    Prepara los registros de Bronze antes de aplicar reglas de calidad.

    - Elimina duplicados exactos de negocio.
    - Estandariza método de pago.
    - Estandariza canal.
    - Convierte fecha_venta a DATE.
    - Agrega fecha técnica de transformación.

    Esta función es reutilizada tanto por Silver como por cuarentena.
    """

    df = spark.read.table(
        "electrocasa.bronze.ventas"
    )

    # 1. Eliminar los 225 duplicados exactos de negocio
    df = df.dropDuplicates([
        "venta_id",
        "sucursal_id",
        "producto_id",
        "cantidad",
        "monto_total",
        "metodo_pago",
        "fecha_venta",
        "canal"
    ])

    # 2. Normalizar método de pago
    metodo = F.lower(F.trim(F.col("metodo_pago")))

    df = (
        df
        .withColumn(
            "metodo_pago",
            F.when(
                metodo.isin("efectivo", "efv"),
                "efectivo"
            )
            .when(
                metodo == "plin",
                "plin"
            )
            .when(
                metodo.isin(
                    "tarjeta",
                    "tc",
                    "tarjeta de credito",
                    "tarjeta_credito"
                ),
                "tarjeta_credito"
            )
            .when(
                metodo.isin(
                    "transferencia",
                    "transferencia bancaria"
                ),
                "transferencia_bancaria"
            )
            .when(
                metodo == "yape",
                "yape"
            )
            .otherwise(metodo)
        )

        # 3. Normalizar canal
        .withColumn(
            "canal",
            F.lower(F.trim(F.col("canal")))
        )

        # 4. Convertir los formatos detectados de fecha
        .withColumn(
            "fecha_venta",
            F.expr("""
                CASE
                    WHEN TRY_CAST(fecha_venta AS DATE) IS NOT NULL
                        THEN TRY_CAST(fecha_venta AS DATE)
                    ELSE TO_DATE(fecha_venta, 'dd/MM/yyyy')
                END
            """)
        )

        # 5. Fecha técnica
        .withColumn(
            "_fecha_transformacion",
            F.current_timestamp()
        )
    )

    return df


# ============================================================
# SILVER - REGISTROS VÁLIDOS
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.ventas",
    comment="Ventas depuradas, estandarizadas y validadas desde Bronze"
)
@dp.expect_or_drop(
    "venta_id_valido",
    "venta_id IS NOT NULL AND TRIM(venta_id) <> ''"
)
@dp.expect_or_drop(
    "producto_id_valido",
    "producto_id IS NOT NULL AND TRIM(producto_id) <> ''"
)
@dp.expect_or_drop(
    "cantidad_positiva",
    "cantidad IS NOT NULL AND cantidad > 0"
)
@dp.expect_or_drop(
    "monto_total_valido",
    "monto_total IS NOT NULL AND monto_total > 0"
)
@dp.expect_or_drop(
    "fecha_venta_valida",
    "fecha_venta IS NOT NULL"
)
@dp.expect_or_drop(
    "sucursal_contextual_valida",
    """
    canal <> 'tienda_fisica'
    OR (
        sucursal_id IS NOT NULL
        AND TRIM(sucursal_id) <> ''
    )
    """
)
def silver_ventas():

    return normalizar_ventas()


# ============================================================
# CUARENTENA
# ============================================================

@dp.materialized_view(
    name="electrocasa.silver.ventas_cuarentena",
    comment="Ventas rechazadas por reglas críticas de calidad de datos"
)
def ventas_cuarentena():

    df = normalizar_ventas()

    condicion_invalida = (

        # Identificadores obligatorios
        F.col("venta_id").isNull()
        | (F.trim(F.col("venta_id")) == "")

        | F.col("producto_id").isNull()
        | (F.trim(F.col("producto_id")) == "")

        # Cantidad
        | F.col("cantidad").isNull()
        | (F.col("cantidad") <= 0)

        # Monto
        | F.col("monto_total").isNull()
        | (F.col("monto_total") <= 0)

        # Fecha
        | F.col("fecha_venta").isNull()

        # La sucursal solamente es obligatoria para tienda física
        | (
            (F.col("canal") == "tienda_fisica")
            & (
                F.col("sucursal_id").isNull()
                | (F.trim(F.col("sucursal_id")) == "")
            )
        )
    )

    df_invalidos = df.filter(condicion_invalida)

    return (
        df_invalidos
        .withColumn(
            "_motivo_cuarentena",

            F.when(
                F.col("venta_id").isNull()
                | (F.trim(F.col("venta_id")) == ""),
                "venta_id_invalido"
            )

            .when(
                F.col("producto_id").isNull()
                | (F.trim(F.col("producto_id")) == ""),
                "producto_id_invalido"
            )

            .when(
                F.col("cantidad").isNull(),
                "cantidad_nula"
            )

            .when(
                F.col("cantidad") <= 0,
                "cantidad_invalida"
            )

            .when(
                F.col("monto_total").isNull(),
                "monto_total_nulo"
            )

            .when(
                F.col("monto_total") < 0,
                "monto_total_negativo"
            )

            .when(
                F.col("monto_total") == 0,
                "monto_total_cero"
            )

            .when(
                F.col("fecha_venta").isNull(),
                "fecha_venta_invalida"
            )

            .when(
                (F.col("canal") == "tienda_fisica")
                & (
                    F.col("sucursal_id").isNull()
                    | (F.trim(F.col("sucursal_id")) == "")
                ),
                "sucursal_fisica_invalida"
            )

            .otherwise(
                "otro_error_calidad"
            )
        )
    )
