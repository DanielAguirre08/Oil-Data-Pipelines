from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit

# 1. Iniciar Spark con esteroides para tu M4
spark = SparkSession.builder \
    .appName("OlistSilverTransform") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. Cargar la data desde tu carpeta local (Capa Bronze local)
# Nota: En un entorno pro leeríamos de Azure, pero para tu proyecto
# usaremos la carpeta /data como nuestro "espejo" de la nube.
df_orders = spark.read.csv("../data/olist_orders_dataset.csv", header=True, inferSchema=True)

print("--- Limpiando Tabla de Pedidos ---")

# 3. TRANSFORMACIONES SILVER
# A) Convertir strings a Timestamps (Fechas reales)
# B) Crear una columna de 'estado_entrega' simplificada
df_silver = df_orders.withColumn("fecha_compra", to_timestamp(col("order_purchase_timestamp"))) \
    .withColumn("fecha_aprobado", to_timestamp(col("order_approved_at"))) \
    .withColumn("fecha_entrega_real", to_timestamp(col("order_delivered_customer_date"))) \
    .withColumn("fecha_entrega_estimada", to_timestamp(col("order_estimated_delivery_date")))

# 4. Manejo de Nulos (Data Quality)
# Si no tiene fecha de entrega, le ponemos un estado "Pendiente"
df_silver = df_silver.withColumn("status_entrega",
                                 when(col("fecha_entrega_real").isNull(), "En Camino/Cancelado")
                                 .otherwise("Entregado"))

# 5. Selección de columnas finales
df_final_silver = df_silver.select(
    "order_id", "customer_id", "order_status",
    "fecha_compra", "fecha_entrega_real", "fecha_entrega_estimada", "status_entrega"
)

df_final_silver.show(10)
print(f"Total de registros procesados: {df_final_silver.count()}")

# 6. Guardar progreso (Opcional: puedes guardarlo como .parquet para que sea más rápido)
# df_final_silver.write.mode("overwrite").parquet("../data/silver_orders.parquet")

spark.stop()