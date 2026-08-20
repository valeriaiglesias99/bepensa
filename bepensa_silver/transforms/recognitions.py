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
    """silver.recognitions.

    Parsea las bounding boxes y asigna:
      - shelf_row: fila del anaquel
      - shelf_position: posición horizontal dentro de la fila

    recognition_id representa una foto.

    La fila se determina agrupando detecciones cuyos centros verticales
    están suficientemente cerca. La tolerancia se calcula a partir de
    la altura de las bounding boxes.
    """

    # ------------------------------------------------------------------
    # 1. Leer inferencias y parsear bounding_box
    # ------------------------------------------------------------------

    df = (
        spark.table(f"{BRONZE_REC}.inferences")
        .select(
            "inference_id",
            "recognition_id",
            "library_object_id",
            "score",
            "bounding_box",
        )
        .withColumn(
            "bbox",
            F.from_json(F.col("bounding_box"), _BBOX_SCHEMA),
        )
        .withColumn("bbox_x_min", F.col("bbox.x_min"))
        .withColumn("bbox_x_max", F.col("bbox.x_max"))
        .withColumn("bbox_y_min", F.col("bbox.y_min"))
        .withColumn("bbox_y_max", F.col("bbox.y_max"))
        .withColumn("bbox_width", F.col("bbox.width"))
        .withColumn("bbox_height", F.col("bbox.height"))
        .drop("bbox", "bounding_box")
    )

    # ------------------------------------------------------------------
    # 2. Centro vertical y horizontal
    # ------------------------------------------------------------------

    df = (
        df
        .withColumn(
            "y_center",
            (F.col("bbox_y_min") + F.col("bbox_y_max")) / 2,
        )
        .withColumn(
            "x_center",
            (F.col("bbox_x_min") + F.col("bbox_x_max")) / 2,
        )
    )

    # ------------------------------------------------------------------
    # 3. Altura típica de los productos de la foto
    #
    #    Usamos mediana aproximada para evitar que una caja muy grande
    #    distorsione la tolerancia.
    # ------------------------------------------------------------------

    w_photo = Window.partitionBy("recognition_id")

    df = df.withColumn(
        "median_height",
        F.expr(
            "percentile_approx(bbox_height, 0.5)"
        ).over(w_photo),
    )

    # Tolerancia vertical.
    #
    # Si los centros verticales están dentro de esta distancia,
    # consideramos que pertenecen a la misma fila.
    #
    # Puedes ajustar este valor.
    df = df.withColumn(
        "row_tolerance",
        F.greatest(
            F.col("median_height") * F.lit(0.60),
            F.lit(10.0),
        ),
    )

    # ------------------------------------------------------------------
    # 4. Ordenar verticalmente
    # ------------------------------------------------------------------

    w_y = (
        Window
        .partitionBy("recognition_id")
        .orderBy("y_center", "x_center", "inference_id")
    )

    df = df.withColumn(
        "y_center_prev",
        F.lag("y_center").over(w_y),
    )

    # ------------------------------------------------------------------
    # 5. Detectar inicio de una nueva fila
    # ------------------------------------------------------------------

    df = df.withColumn(
        "y_gap",
        F.col("y_center") - F.col("y_center_prev"),
    )

    df = df.withColumn(
        "new_row",
        F.when(
            F.col("y_center_prev").isNull()
            | (F.col("y_gap") > F.col("row_tolerance")),
            1,
        ).otherwise(0),
    )

    # ------------------------------------------------------------------
    # 6. Crear número de fila
    # ------------------------------------------------------------------

    w_rows = (
        Window
        .partitionBy("recognition_id")
        .orderBy("y_center", "x_center", "inference_id")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow,
        )
    )

    df = df.withColumn(
        "row_number_raw",
        F.sum("new_row").over(w_rows) - 1,
    )

    df = df.withColumn(
        "shelf_row",
        F.concat(
            F.lit("r"),
            F.col("row_number_raw").cast("int"),
        ),
    )

    # ------------------------------------------------------------------
    # 7. Posición horizontal dentro de cada fila
    # ------------------------------------------------------------------

    w_position = (
        Window
        .partitionBy("recognition_id", "shelf_row")
        .orderBy(
            "bbox_x_min",
            "x_center",
            "inference_id",
        )
    )

    df = df.withColumn(
        "shelf_position",
        F.row_number().over(w_position),
    )

    # ------------------------------------------------------------------
    # 8. Obtener event_id desde recognitions
    # ------------------------------------------------------------------

    rec_event = (
        spark.table(f"{BRONZE_REC}.recognitions")
        .select(
            F.col("recognition_id").alias("rec_id_key"),
            "event_id",
        )
    )

    # ------------------------------------------------------------------
    # 9. Resultado final
    # ------------------------------------------------------------------

    return (
        df
        .join(
            rec_event,
            F.col("recognition_id") == F.col("rec_id_key"),
            how="left",
        )
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
