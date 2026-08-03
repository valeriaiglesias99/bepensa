from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ..config import BRONZE_APP


def _auto_responses(spark: SparkSession, visits_silver: DataFrame) -> DataFrame:
    """Respuestas automáticas de preguntas activas (excluye puertas/rack/
    carriles/góndola), acotadas al universo de visitas silver."""
    questions = spark.table(f"{BRONZE_APP}.questions")
    return (
        spark.table(f"{BRONZE_APP}.automatic_responses").alias("a")
        .join(questions.alias("q"), F.col("q.question_id") == F.col("a.question_id"), "inner")
        .filter(
            ~(
                F.lower(F.col("q.question_text")).like("%puertas%")
                | F.lower(F.col("q.question_text")).like("%rack%")
                | F.lower(F.col("q.question_text")).like("%carriles%")
                # NORMALIZE(LOWER(x), NFD) LIKE '%gondola%' es equivalente a
                # lower(x) like '%gondola%': con un patrón sin diacríticos, NFD
                # no altera el match (ver nota abajo sobre "góndola" con tilde).
                | F.lower(F.col("q.question_text")).like("%gondola%")
            )
        )
        .filter(F.col("q.is_active") == True)
        .join(visits_silver.select("visit_id", "completed_at_ts"), on="visit_id", how="inner")
        .select(
            "visit_id",
            F.col("a.question_id").alias("question_id"),
            F.col("a.answer").alias("answer"),
            F.col("completed_at_ts").alias("completed_at"),
            F.col("q.task_id").alias("task_id"),
            F.col("q.question_text").alias("question_text"),
            F.col("q.question_type").alias("question_type"),
            F.col("a.aut_resp_id").alias("resp_id"),
        )
    )


def _user_responses(spark: SparkSession) -> DataFrame:
    """Última respuesta de usuario por (visit_id, question_id)."""
    questions = spark.table(f"{BRONZE_APP}.questions")
    w_latest = Window.partitionBy("visit_id", "question_id").orderBy(F.col("created_at").desc())
    return (
        spark.table(f"{BRONZE_APP}.responses").alias("r")
        .withColumn("rn", F.row_number().over(w_latest))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .join(questions.alias("q"), F.col("q.question_id") == F.col("r.question_id"), "inner")
        .select(
            F.col("r.visit_id").alias("visit_id"),
            F.col("r.question_id").alias("question_id"),
            F.col("r.created_at").alias("created_at"),
            F.col("r.value").alias("value"),
            F.col("q.task_id").alias("task_id"),
            F.col("q.question_text").alias("question_text"),
            F.col("q.question_type").alias("question_type"),
            F.col("r.resp_id").alias("resp_id"),
        )
    )


def build(spark: SparkSession, visits_silver: DataFrame, tasks: DataFrame) -> DataFrame:
    """silver.responses: respuesta final por (visit_id, question_id),
    combinando la respuesta automática con la del usuario.

    Regla de negocio: la respuesta automática solo gana cuando la pregunta
    es de tipo 'Number' y no hubo respuesta de usuario; en cualquier otro
    caso manda la del usuario.
    """
    a = _auto_responses(spark, visits_silver).alias("a")
    u = _user_responses(spark).alias("u")

    qtype = F.coalesce(F.col("a.question_type"), F.col("u.question_type"))
    auto_wins = F.col("u.created_at").isNull() & (qtype == "Number")

    merged = (
        a.join(
            u,
            (F.col("a.visit_id") == F.col("u.visit_id"))
            & (F.col("a.question_id") == F.col("u.question_id")),
            how="full_outer",
        )
        # Un solo select con coalesce evita el "visit_id ambiguo" del full outer.
        .select(
            F.coalesce(F.col("a.visit_id"), F.col("u.visit_id")).alias("visit_id"),
            F.coalesce(F.col("a.question_id"), F.col("u.question_id")).alias("question_id"),
            F.coalesce(F.col("a.resp_id"), F.col("u.resp_id")).alias("resp_id"),
            F.coalesce(F.col("a.task_id"), F.col("u.task_id")).alias("task_id_key"),
            qtype.alias("question_type"),
            F.coalesce(F.col("a.question_text"), F.col("u.question_text")).alias("question_text"),
            F.when(auto_wins, F.col("a.answer")).otherwise(F.col("u.value")).alias("ultima_respuesta"),
            F.when(auto_wins, F.lit("auto")).otherwise(F.lit("user")).alias("fuente"),
            F.when(F.col("a.completed_at").isNull(), F.col("u.created_at"))
            .when(F.col("u.created_at").isNull(), F.col("a.completed_at"))
            .otherwise(F.greatest(F.col("a.completed_at"), F.col("u.created_at")))
            .alias("fecha_respuesta"),
        )
    )

    return (
        merged
        .join(visits_silver.select("visit_id", "user_id", "store_id"), on="visit_id", how="inner")
        .join(tasks.alias("t"), F.col("task_id_key") == F.col("t.task_id"), "inner")
        .select(
            "visit_id",
            "user_id",
            "store_id",
            "resp_id",
            F.col("t.task_id").alias("task_id"),
            F.col("t.task_name").alias("task_name"),
            "question_id",
            "question_type",
            "question_text",
            "ultima_respuesta",
            "fuente",
            "fecha_respuesta",
        )
        .distinct()
    )
