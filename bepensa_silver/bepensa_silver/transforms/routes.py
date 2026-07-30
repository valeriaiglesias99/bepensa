from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import BRONZE_APP
from . import users


def build(spark: SparkSession) -> DataFrame:
    """silver.routes: rutas activas restringidas a preventistas."""
    preventistas = users.preventistas(spark).select("user_id")
    return (
        spark.table(f"{BRONZE_APP}.routes")
        .filter(F.col("is_active") == True)
        .select("route_id", "store_id", "user_id")
        .join(preventistas, on="user_id", how="inner")
    )
