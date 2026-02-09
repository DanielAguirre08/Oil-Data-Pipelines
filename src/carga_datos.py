import os
from pyspark.sql import SparkSession

# 1. Iniciamos Spark con el conector de Azure
spark = SparkSession.builder \
    .appName("CargaOlistAzure") \
    .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11") \
    .getOrCreate()

# 2. Configuración de Azure SQL
server_name = "olistdb.database.windows.net"
database_name = "db_olist_final"
url = f"jdbc:sqlserver://{server_name}:1433;database={database_name};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

properties = {
    "user": "sqladmin",
    "password": os.getenv('AZURE_KEY'),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# 3. RUTA AL ARCHIVO CSV (Nota el .cvs al final como me indicaste)
ruta_data_procesada = "/Users/danielfranciscoaguirreespinoza/Documents/java_intelliJ/Olist-Data-Engineering-Pipeline/data/gold_ventas_olist.csv"

print("--- 🔍 LEYENDO ARCHIVO CSV ---")
try:
    # Leemos como CSV, con cabecera y detectando tipos de datos automáticamente
    df_final_olist = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(ruta_data_procesada)

    print(f"✅ ¡Archivo leído! Total de filas a subir: {df_final_olist.count()}")

    # 4. SUBIDA A AZURE
    print("🚀 Subiendo data a Azure... Esto puede tardar un par de minutos.")
    df_final_olist.write.jdbc(url=url, table="FactVentas", mode="overwrite", properties=properties)
    print("⭐ ¡TODO LISTO! La data ya está en la base de datos de Azure.")

except Exception as e:
    print(f"❌ Ocurrió un error: {e}")

finally:
    spark.stop()