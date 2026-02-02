from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# 1. Iniciar Spark (Optimizado para tu M4)
spark = SparkSession.builder \
    .appName("ExploracionOlist") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. Cargar los datasets principales
# Asegúrate de que los nombres coincidan con tus archivos en /data
df_orders = spark.read.csv("../data/olist_orders_dataset.csv", header=True, inferSchema=True)
df_items = spark.read.csv("../data/olist_order_items_dataset.csv", header=True, inferSchema=True)

# 3. Ver las primeras filas y el esquema (Tipos de datos)
print("--- Esquema de Pedidos ---")
df_orders.printSchema()
df_orders.show(5)

# 4. Contar nulos (Esto es clave para el CV: "Data Cleaning")
print("--- Conteo de Nulos en Pedidos ---")
df_orders.select([count(when(col(c).isNull(), c)).alias(c) for c in df_orders.columns]).show()

spark.stop()