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

    # 2. Limpieza de Tipos de Datos
    for col in df.columns:
        # Normalizar nombres para la busqueda
        col_lower = col.lower()
        
        # Convertir columnas de fecha a formato datetime
        if 'date' in col_lower or ('time' in col_lower and 'elapsed' not in col_lower and 'onhold' not in col_lower):
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convertir duraciones (timedelta) a segundos para compatibilidad con pyarrow
        elif 'time' in col_lower or 'elapsed' in col_lower:
            temp_timedelta = pd.to_timedelta(df[col], errors='coerce')
            df[col] = temp_timedelta.dt.total_seconds()

    # 3. Configurar el pipeline hacia DuckDB
    pipeline = dlt.pipeline(
        pipeline_name='ingesta_arteche',
        destination='duckdb',
        dataset_name='datos_crudos'
    )

    # 4. Cargar los datos
    load_info = pipeline.run(df, table_name="tickets", write_disposition="replace")
    
    print("Ingesta completada exitosamente.")
    print(load_info)

if __name__ == "__main__":
    ejecutar_ingesta()
