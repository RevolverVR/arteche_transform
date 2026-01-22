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

    # 2. Limpieza de columnas de Fecha y Tiempo
    columnas_tiempo = [col for col in df.columns if 'Date' in col or 'Time' in col]
    
    for col in columnas_tiempo:
        print(f"Normalizando columna de tiempo: {col}")
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 3. Limpieza de columnas de Duración (Timedelta)
    # Este paso soluciona el error de PyArrow con 'Onhold time'
    for col in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[col]):
            print(f"Convirtiendo duración (timedelta) a texto: {col}")
            df[col] = df[col].astype(str)
        # Verificación adicional por si pandas lo detecta como objeto genérico
        elif df[col].dtype == 'object':
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if hasattr(sample, 'days') and hasattr(sample, 'seconds'):
                print(f"Detectado timedelta en columna de objeto, convirtiendo: {col}")
                df[col] = df[col].astype(str)

    # 4. Configurar el pipeline hacia DuckDB
    pipeline = dlt.pipeline(
        pipeline_name='ingesta_arteche',
        destination='duckdb',
        dataset_name='datos_crudos'
    )

    # 5. Ejecutar la carga
    load_info = pipeline.run(df, table_name="tickets", write_disposition="replace")
    
    print("Ingesta completada exitosamente.")
    print(load_info)

if __name__ == "__main__":
    ejecutar_ingesta()
