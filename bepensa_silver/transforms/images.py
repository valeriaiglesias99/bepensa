from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import BRONZE_APP, BRONZE_REC


def _recognitions(spark: SparkSession) -> DataFrame:
    return spark.table(f"{BRONZE_REC}.recognitions").select(
        F.col("event_id").alias("event_id_rec"),
        F.col("labeled_image_path").alias("labeled_url"),
        "is_approved",
    )


def build(spark: SparkSession, visits_silver: DataFrame) -> DataFrame:
    """silver.images: imágenes activas de visitas del universo silver,
    con nivel/categoría de escena y datos de reconocimiento."""
    return (
        spark.table(f"{BRONZE_APP}.images")
        .filter(F.col("is_active") == True)
        .join(visits_silver.select("visit_id"), on="visit_id", how="inner")
        .select(
            "img_id",
            "visit_id",
            "original_url",
            "subscene_type",
            F.split("schema", ",").getItem(0).alias("scene_level"),
            F.split("schema", ",").getItem(1).alias("scene_category"),
            F.col("recognition_id").alias("event_id"),
        )
        .join(_recognitions(spark), F.col("event_id") == F.col("event_id_rec"), how="left")
        .drop("event_id_rec")
    )
