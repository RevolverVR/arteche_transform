import dlt
import pandas as pd
import os

def ejecutar_ingesta():
    archivo_fuente = "KPI(Base Tickets) (version 1) Arteche.xlsx" 
    
    if not os.path.exists(archivo_fuente):
        print(f"Error: No se encuentra el archivo {archivo_fuente}")
        return

    print(f"Iniciando ingesta de datos desde {archivo_fuente}...")
    
    # 1. Cargar el DataFrame
    df = pd.read_excel(archivo_fuente)

    # 2. Limpieza automatizada
    # Busca cualquier columna que contenga 'Date' o 'Time' en su nombre
    columnas_a_limpiar = [col for col in df.columns if 'Date' in col or 'Time' in col]
    
    for col in columnas_a_limpiar:
        print(f"Normalizando columna detectada: {col}")
        # errors='coerce' convierte textos invalidos en NaT (Not a Time)
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 3. Configurar el pipeline hacia DuckDB
    pipeline = dlt.pipeline(
        pipeline_name='ingesta_arteche',
        destination='duckdb',
        dataset_name='datos_crudos'
    )

    # 4. Cargar los datos en la tabla 'tickets'
    load_info = pipeline.run(df, table_name="tickets", write_disposition="replace")
    
    print("Ingesta completada exitosamente.")
    print(load_info)

if __name__ == "__main__":
    ejecutar_ingesta()
