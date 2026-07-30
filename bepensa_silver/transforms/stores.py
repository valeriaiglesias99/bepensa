from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import BRONZE_APP

# Mapa de renombres bronze -> silver (antes eran 13 .withColumnRenamed).
_RENAMES = {
    "cliente": "client_id",
    "nomcli": "client_name",
    "nomcolonia": "neighborhood",
    "nomgiro": "business_type",
    "nomjefe": "supervisor_name",
    "cedi_name": "cedi",
    "tamanio_1": "size_1",
    "tamanio_2": "size_2",
    "puertas": "doors",
    "gerencia_comercial": "commercial_management",
    "gerencia_ventas": "sales_management",
    "lat": "latitude",
    "lon": "longitude",
}

# Columnas de texto que se normalizan a Title Case.
_TITLE_CASE = ["client_name", "neighborhood", "business_type", "supervisor_name", "cedi"]


def build(spark: SparkSession) -> DataFrame:
    """silver.stores: tiendas activas, columnas en inglés y textos en
    Title Case."""
    df = (
        spark.table(f"{BRONZE_APP}.stores")
        .filter(F.col("is_active") == True)
        .select(
            "store_id", "cliente", "nomcli", "nomcolonia", "nomgiro",
            "nomjefe", "cedi_name", "tamanio_1", "tamanio_2", "puertas",
            "gerencia_comercial", "gerencia_ventas", "lat", "lon",
        )
    )

    for old, new in _RENAMES.items():
        df = df.withColumnRenamed(old, new)

    for col in _TITLE_CASE:
        df = df.withColumn(col, F.initcap(F.col(col)))

    return df
