# bepensa_silver

Pipeline de la capa Silver de Bepensa, migrado del notebook a un paquete
de Python para versionarlo en Git y poder testearlo.

## Estructura

```
bepensa_silver/
├── config.py            constantes de catálogo/esquema y parámetros de negocio
├── io.py                write_silver(): escritura estandarizada
├── run_silver.py        orquestador (entry point del job)
└── transforms/
    ├── users.py         silver.users  + preventistas() compartido
    ├── visits.py        silver.visits
    ├── stores.py        silver.stores
    ├── routes.py        silver.routes
    ├── questions.py     silver.questions + read_tasks()
    ├── images.py        silver.images
    ├── products.py      silver.products
    ├── recognitions.py  silver.recognitions
    └── responses.py     silver.responses
```

Cada transformación expone `build(spark, ...) -> DataFrame` y recibe como
parámetros los DataFrames de los que depende (no lee tablas silver ya
persistidas). El orquestador arma el grafo de dependencias.

## Cómo usarlo en Databricks

1. Sube la carpeta `bepensa_silver/` a una **Git folder** o **Workspace
   File**, al lado de tu notebook de orquestación.

2. En un notebook delgado:

   ```python
   %load_ext autoreload
   %autoreload 2

   from bepensa_silver.run_silver import run
   run(spark)
   ```

   `autoreload` hace que los cambios en los `.py` se tomen sin reiniciar.

3. Para explorar una sola tabla mientras iteras:

   ```python
   from bepensa_silver.transforms import recognitions
   recognitions.build(spark).display()
   ```

## Notas sobre la migración

- Se reintegraron las CTEs de `responses` (`_auto_responses` /
  `_user_responses`) que faltaban en la versión del notebook.
- `products` ahora recibe `questions_silver` como parámetro en vez de leer
  `silver.questions`: se elimina la dependencia de orden de escritura.
- `visits` se simplificó: se quitó el doble filtro `is_active` y las
  columnas de fecha/copia que no llegaban al resultado final.
- Todas las escrituras pasan por `write_silver()` con `overwriteSchema`
  consistente (antes `routes` no lo tenía).
- Los nombres de columna de salida se conservaron idénticos al notebook
  (incluidos los que están en español, como `ultima_respuesta`), para no
  romper los consumidores actuales. Si más adelante quieres unificar todo
  a inglés, es un cambio aparte y coordinado con Power BI / la web app.
```
