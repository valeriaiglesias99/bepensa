from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import BRONZE_APP, BRONZE_REC


def build(spark: SparkSession, questions_silver: DataFrame) -> DataFrame:
    """silver.products: productos con su library_object, la pregunta
    asociada y metadatos de la pregunta.

    Recibe `questions_silver` como parámetro en vez de leer la tabla ya
    persistida `silver.questions`; así desaparece la dependencia de orden
    de escritura que tenía el notebook. `presentation` vacío -> "Mixto".
    """
    products_questions = spark.table(f"{BRONZE_APP}.products_questions").select(
        "prod_id", "question_id"
    )
    library = spark.table(f"{BRONZE_REC}.library_objects").select(
        "library_object_id", F.col("name").alias("train_name_key")
    )
    questions = questions_silver.select(
        "question_id", "question_level", "task_name", "presentation"
    )

    return (
        spark.table(f"{BRONZE_APP}.products")
        .select("prod_id", "display_name", "train_name")
        .join(library, F.col("train_name") == F.col("train_name_key"), how="left")
        .drop("train_name_key")
        .join(products_questions, on="prod_id", how="left")
        .join(questions, on="question_id", how="inner")
        .withColumn(
            "presentation",
            F.when(
                F.col("presentation").isNull() | (F.trim(F.col("presentation")) == ""),
                F.lit("Mixto"),
            ).otherwise(F.col("presentation")),
        )
        .select(
            "prod_id",
            "question_id",
            "library_object_id",
            "display_name",
            "question_level",
            "task_name",
            "presentation",
        )
    )
