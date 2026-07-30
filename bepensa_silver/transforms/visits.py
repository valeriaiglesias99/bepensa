from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import BRONZE_APP, TZ_OFFSET_HOURS
from . import users


def build(spark: SparkSession) -> DataFrame:
    """silver.visits: visitas activas de preventistas, con timestamps en
    hora local y duración en minutos.

    Nota: el notebook original creaba columnas de fecha y copias de las
    marcas de tiempo que nunca llegaban al `select` final; aquí se omiten.
    """
    preventistas = users.preventistas(spark).select("user_id")

    return (
        spark.table(f"{BRONZE_APP}.visits")
        .filter(F.col("is_active") == True)
        .join(preventistas, on="user_id", how="inner")
        .withColumn(
            "created_at_ts",
            F.col("created_at") - F.expr(f"INTERVAL {TZ_OFFSET_HOURS} HOURS"),
        )
        .withColumn(
            "completed_at_ts",
            F.col("completed_at") - F.expr(f"INTERVAL {TZ_OFFSET_HOURS} HOURS"),
        )
        .withColumn(
            "duration_min",
            F.when(
                F.col("created_at_ts").isNull() | F.col("completed_at_ts").isNull(),
                None,
            )
            .otherwise(
                (F.unix_timestamp("completed_at_ts") - F.unix_timestamp("created_at_ts"))
                / 60.0
            )
            .cast("double"),
        )
        .select(
            "visit_id",
            "store_id",
            "user_id",
            "created_at_ts",
            "completed_at_ts",
            "duration_min",
            "visit_completed",
        )
    )
