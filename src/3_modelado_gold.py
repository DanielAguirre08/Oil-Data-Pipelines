from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

spark = SparkSession.builder.appName("OlistGoldModel").getOrCreate()

# 1. CARGA DE CAPAS (Usamos los archivos que ya tienes en /data)
# Cargamos la de Orders (la que limpiamos antes) y las nuevas
orders = spark.read.csv("../data/olist_orders_dataset.csv", header=True, inferSchema=True)
items = spark.read.csv("../data/olist_order_items_dataset.csv", header=True, inferSchema=True)
products = spark.read.csv("../data/olist_products_dataset.csv", header=True, inferSchema=True)
translation = spark.read.csv("../data/product_category_name_translation.csv", header=True, inferSchema=True)

# 2. PROCESO DE UNIÓN (El "Cerebro" del Pipeline)
# Unimos Pedidos con Ítems (para tener precios)
df_gold = items.join(orders, "order_id", "inner")

# Unimos con Productos (para tener categorías en portugués)
df_gold = df_gold.join(products, "product_id", "left")

# Unimos con la Traducción (para tener categorías en inglés)
df_gold = df_gold.join(translation, "product_category_name", "left")

# 3. SELECCIÓN Y CÁLCULOS FINALES
# Aquí creamos la "Tabla Maestra" que irá a Power BI
final_fact_table = df_gold.select(
    col("order_id"),
    col("customer_id"),
    col("product_id"),
    col("price").alias("monto_venta"),
    col("freight_value").alias("costo_envio"),
    # Calculamos el Total usando LaTeX: $Total = Precio + Flete$
    round(col("price") + col("freight_value"), 2).alias("total_pedido"),
    col("product_category_name_english").alias("categoria_producto"),
    col("order_status").alias("estado")
)

print("--- Vista Previa de la Capa Gold (Lista para Power BI) ---")
final_fact_table.show(10)
print(f"Registros finales: {final_fact_table.count()}")

# 4. OPCIONAL: Guardar localmente para respaldo
final_fact_table.write.mode("overwrite").csv("../data/gold_ventas_olist.csv", header=True)

spark.stop()