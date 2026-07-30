from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import BRONZE_APP, ROLE_PREVENTISTA


def preventistas(spark: SparkSession) -> DataFrame:
    """Usuarios bronze con rol de preventista (role == 2).

    Se usa como filtro de universo en `visits` y `routes`, por eso vive
    aquí y se importa desde esas transformaciones (antes era un df_users
    que se colaba de una celda a otra).
    """
    return spark.table(f"{BRONZE_APP}.users").filter(F.col("role") == ROLE_PREVENTISTA)


def build(spark: SparkSession) -> DataFrame:
    """silver.users: preventistas activos con el nombre normalizado."""
    return (
        spark.table(f"{BRONZE_APP}.users")
        .filter((F.col("role") == ROLE_PREVENTISTA) & (F.col("is_active") == True))
        .select("user_id", "identification", "fullname")
        .withColumn("fullname", F.initcap(F.col("fullname")))
    )
