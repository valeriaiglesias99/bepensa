"""Configuración central del pipeline.

Un solo lugar para nombres de catálogo/esquema y parámetros de negocio.
Si mañana cambia el offset horario, el rol de preventista o el catálogo,
se toca aquí y no en cada transformación.
"""

CATALOG = "bepensa"

BRONZE_APP = f"{CATALOG}.bronze_app"
BRONZE_REC = f"{CATALOG}.bronze_recognition"
SILVER = f"{CATALOG}.silver"

# Bronze guarda las marcas de tiempo en UTC; restamos este offset para
# obtener la hora local.
TZ_OFFSET_HOURS = 5

# Rol que identifica a un preventista en bronze_app.users.
ROLE_PREVENTISTA = 2
