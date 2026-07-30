"""Helpers de lectura/escritura.

Centraliza la escritura para que todas las tablas silver se guarden con
las mismas opciones (evita el olvido de `overwriteSchema` que teníamos
en el notebook).
"""
from pyspark.sql import DataFrame

from .config import SILVER


def write_silver(df: DataFrame, name: str, *, overwrite_schema: bool = True,
                 verbose: bool = True) -> str:
    """Escribe `df` como `bepensa.silver.<name>` sobrescribiendo.

    Devuelve el nombre completo de la tabla destino.
    """
    target = f"{SILVER}.{name}"
    writer = df.write.mode("overwrite")
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(target)
    if verbose:
        print(f"\u2713 {target}")
    return target
