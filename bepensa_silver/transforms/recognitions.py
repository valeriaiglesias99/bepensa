from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType

from ..config import BRONZE_REC

_BBOX_SCHEMA = StructType([
    StructField("height", DoubleType()),
    StructField("width", DoubleType()),
    StructField("x_max", DoubleType()),
    StructField("x_min", DoubleType()),
    StructField("y_max", DoubleType()),
    StructField("y_min", DoubleType()),
])


def build(spark: SparkSession) -> DataFrame:
    """silver.recognitions: inferencias con bounding box parseado y
    asignación de fila (`shelf_row`) y posición (`shelf_position`) dentro
    de la rejilla del anaquel.

    La fila se detecta por el salto vertical entre cajas consecutivas,
    normalizado por la altura promedio; un salto por encima de un umbral
    dinámico (2x el p75 de la foto, con piso de 1.2) inicia una fila nueva.
    """
    w_rows_asc = Window.partitionBy("recognition_id").orderBy("y_center")
    w_cumulative = (
        Window.partitionBy("recognition_id")
        .orderBy("y_center")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_photo = Window.partitionBy("recognition_id")

    df = (
        spark.table(f"{BRONZE_REC}.inferences")
        .select("inference_id", "recognition_id", "library_object_id", "score", "bounding_box")
        .withColumn("bbox", F.from_json(F.col("bounding_box"), _BBOX_SCHEMA))
        .withColumn("bbox_x_min", F.col("bbox.x_min"))
        .withColumn("bbox_x_max", F.col("bbox.x_max"))
        .withColumn("bbox_y_min", F.col("bbox.y_min"))
        .withColumn("bbox_y_max", F.col("bbox.y_max"))
        .withColumn("bbox_width", F.col("bbox.width"))
        .withColumn("bbox_height", F.col("bbox.height"))
        .drop("bbox", "bounding_box")
        # Centro vertical y salto respecto a la caja anterior.
        .withColumn("y_center", (F.col("bbox_y_min") + F.col("bbox_y_max")) / 2)
        .withColumn("y_center_prev", F.lag("y_center", 1).over(w_rows_asc))
        .withColumn("height_prev", F.lag("bbox_height", 1).over(w_rows_asc))
        .withColumn("jump", F.col("y_center") - F.col("y_center_prev"))
        .withColumn("avg_height", (F.col("bbox_height") + F.col("height_prev")) / 2)
        .withColumn("jump_norm", F.col("jump") / F.col("avg_height"))
        # Umbral dinámico por foto.
        .withColumn(
            "p75_jump_norm",
            F.expr("percentile_approx(jump_norm, 0.75)").over(w_photo),
        )
        .withColumn("threshold", F.greatest(F.col("p75_jump_norm") * 2, F.lit(1.2)))
        # Salto sobre el umbral (o primera caja) -> nueva fila.
        .withColumn(
            "new_row",
            F.when(
                F.col("y_center_prev").isNull()
                | (F.col("jump_norm") > F.col("threshold")),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "shelf_row",
            F.concat(F.lit("r"), (F.sum("new_row").over(w_cumulative) - 1).cast("int")),
        )
        .drop(
            "y_center_prev", "height_prev", "jump", "avg_height",
            "jump_norm", "p75_jump_norm", "threshold", "new_row",
        )
    )

    # Posición horizontal (izquierda a derecha) dentro de cada fila.
    w_position = Window.partitionBy("recognition_id", "shelf_row").orderBy("bbox_x_min")
    df = df.withColumn("shelf_position", F.row_number().over(w_position))

    # event_id desde recognitions.
    rec_event = spark.table(f"{BRONZE_REC}.recognitions").select(
        F.col("recognition_id").alias("rec_id_key"), "event_id"
    )

    return (
        df.join(rec_event, F.col("recognition_id") == F.col("rec_id_key"), how="left")
        .drop("rec_id_key")
        .select(
            "inference_id",
            "recognition_id",
            "event_id",
            "library_object_id",
            "score",
            "shelf_row",
            "shelf_position",
        )
    )
