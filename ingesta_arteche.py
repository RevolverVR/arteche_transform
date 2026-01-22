import dlt
import pandas as pd
import os

def ejecutar_ingesta():
    # 1. El nombre de tu archivo de datos (ASEGÚRATE QUE EL NOMBRE SEA EXACTO)
    archivo_fuente = "KPI(Base Tickets) (version 1) Arteche.xlsx" 
    
    if not os.path.exists(archivo_fuente):
        print(f"❌ Error: No se encuentra el archivo {archivo_fuente}")
        return

    print(f"🚀 Iniciando ingesta de datos desde {archivo_fuente}...")
    df = pd.read_excel(archivo_fuente)

    # 2. Configurar el pipeline hacia DuckDB
    # El dataset_name debe ser 'datos_crudos' porque así lo busca dbt
    pipeline = dlt.pipeline(
        pipeline_name='ingesta_arteche',
        destination='duckdb',
        dataset_name='datos_crudos'
    )

    # 3. Cargar los datos en la tabla 'tickets'
    load_info = pipeline.run(df, table_name="tickets", write_disposition="replace")
    
    print("✅ Ingesta completada exitosamente.")
    print(load_info)

if __name__ == "__main__":
    ejecutar_ingesta()
