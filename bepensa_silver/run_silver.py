"""Orquestador del pipeline Silver de Bepensa.

Ejecutable como Databricks Job (entry point) o llamado desde un notebook
delgado:

    from bepensa_silver.run_silver import run
    run(spark)
"""
from pyspark.sql import SparkSession

from .io import write_silver
from .transforms import (
    users,
    visits,
    stores,
    routes,
    questions,
    images,
    products,
    recognitions,
    responses,
)


def run(spark: SparkSession) -> None:
    # 1. Transformaciones base (sin dependencias entre sí).
    users_silver = users.build(spark)
    stores_silver = stores.build(spark)
    visits_silver = visits.build(spark)
    routes_silver = routes.build(spark)

    # 2. Transformaciones que dependen de las anteriores.
    tasks = questions.read_tasks(spark)
    questions_silver = questions.build(spark, tasks)
    products_silver = products.build(spark, questions_silver)
    images_silver = images.build(spark, visits_silver)
    recognitions_silver = recognitions.build(spark)
    responses_silver = responses.build(spark, visits_silver, tasks)

    # 3. Escritura. Como las dependencias se resolvieron pasando
    #    DataFrames (no leyendo tablas persistidas), el orden de escritura
    #    ya no afecta la lógica.
    outputs = {
        "visits": visits_silver,
        "users": users_silver,
        "stores": stores_silver,
        "routes": routes_silver,
        "questions": questions_silver,
        "products": products_silver,
        "images": images_silver,
        "recognitions": recognitions_silver,
        "responses": responses_silver,
    }
    for name, df in outputs.items():
        write_silver(df, name)


if __name__ == "__main__":
    run(SparkSession.builder.getOrCreate())
