from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import BRONZE_APP


def read_tasks(spark: SparkSession) -> DataFrame:
    """Catálogo de tareas (task_id, task_name). Lo usan `questions` y
    `responses`; se lee una vez y se pasa a ambas."""
    return spark.table(f"{BRONZE_APP}.tasks").select("task_id", "task_name")


def build(spark: SparkSession, tasks: DataFrame) -> DataFrame:
    """silver.questions: preguntas activas con su tarea y una columna
    `presentation` derivada del texto (Familiares / Personales)."""
    return (
        spark.table(f"{BRONZE_APP}.questions")
        .filter(F.col("is_active") == True)
        .join(tasks, on="task_id", how="left")
        .select(
            "question_id",
            "question_text",
            "question_category",
            "task_name",
            "question_level",
        )
        .withColumn(
            "presentation",
            F.when(F.lower(F.col("question_text")).contains("familiar"), "Familiares")
            .when(F.lower(F.col("question_text")).contains("personal"), "Personales")
            .otherwise(None),
        )
    )
