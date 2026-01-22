import dlt
import pandas as pd
import os

def ejecutar_ingesta():
    archivo_fuente = "KPI(Base Tickets) (version 1) Arteche.xlsx" 
    
    if not os.path.exists(archivo_fuente):
        print(f"❌ Error: No se encuentra el archivo {archivo_fuente}")
        return

    print(f"🚀 Iniciando ingesta de datos desde {archivo_fuente}...")
    
    # 1. Cargar el DataFrame
    df = pd.read_excel(archivo_fuente)

    # 2. LIMPIEZA PROACTIVA: Identificamos todas las columnas de tiempo conflictivas
    # Agregamos 'Resolved Time' y cualquier otra que pueda tener datos mixtos
    columnas_tiempo = [
        'Responded Date', 
        'Resolved Time', 
        'Created Time', 
        'Due Date'
    ]
    
    for col in columnas_tiempo:
        if col in df.columns:
            print(f"🧹 Limpiando y normalizando columna: {col}")
            # errors='coerce' convierte textos inválidos en NaT (Not a Time), que DuckDB acepta
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # 3. Configurar el pipeline hacia DuckDB
    pipeline = dlt.pipeline(
        pipeline_name='ingesta_arteche',
        destination='duckdb',
        dataset_name='datos_crudos'
    )

    # 4. Cargar los datos en la tabla 'tickets'
    load_info = pipeline.run(df, table_name="tickets", write_disposition="replace")
    
    print("✅ ¡Ingesta completada exitosamente!")
    print(load_info)

if __name__ == "__main__":
    ejecutar_ingesta()
