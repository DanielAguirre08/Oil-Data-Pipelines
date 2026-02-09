import os
from azure.storage.blob import BlobServiceClient

# --- CONFIGURACIÓN ---
# PEGA TU CADENA AQUÍ ABAJO ENTRE LAS COMILLAS
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=datalakeolistdaniel;AccountKey=DxFpgjO10mlHGy3MMOo2kT427PPglzT3/y68tJ3J0DCgm+5y0/Qke7H95xEKva4kw+6BA0nl4QTI+AStg+JrLg==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "bronze"
# Usamos ../data porque el script está en /src y la data en la raíz
LOCAL_PATH = "../data"

def subir_archivos_a_azure():
    try:
        # 1. Conectar con Azure
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

        # 2. Listar archivos .csv en la carpeta /data
        archivos = [f for f in os.listdir(LOCAL_PATH) if f.endswith('.csv')]

        if not archivos:
            print(f"❌ No se encontraron archivos CSV en {os.path.abspath(LOCAL_PATH)}")
            return

        print(f"🚀 Iniciando subida de {len(archivos)} archivos a la capa Bronze...")

        for nombre_archivo in archivos:
            # Crear el cliente del archivo (blob)
            blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=nombre_archivo)

            ruta_completa = os.path.join(LOCAL_PATH, nombre_archivo)

            # Subir el archivo
            with open(ruta_completa, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                print(f"✅ {nombre_archivo} subido exitosamente.")

        print("\n✨ ¡PROCESO TERMINADO! Revisa tu portal de Azure.")

    except Exception as e:
        print(f"❌ Error crítico: {e}")

if __name__ == "__main__":
    subir_archivos_a_azure()