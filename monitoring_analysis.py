import pandas as pd
import os

def extract_monitoring_data(file_path):
    """Extrae los datos del archivo Monitoring.csv usando el delimitador adecuado."""
    try:
        data = pd.read_csv(file_path, delimiter=';')
        return data
    except Exception as e:
        print(f"Error al extraer datos del archivo Monitoring.csv: {e}")
        return None

def transform_monitoring_data(data, threshold=0.3):
    """
    Transforma y limpia los datos del archivo Monitoring.csv.
    Solo incluye columnas con datos significativos (más del threshold de valores no nulos).
    """
    # Eliminar filas y columnas donde más del threshold de valores sean nulos
    column_validity = data.notnull().mean() > threshold
    valid_columns = data.columns[column_validity]
    
    if len(valid_columns) == 0:
        print("Advertencia: No se encontraron columnas con datos significativos.")
        return pd.DataFrame()  # Retornamos un DataFrame vacío si no hay datos útiles

    data_cleaned = data[valid_columns].dropna()

    # Convertir a los tipos de datos apropiados (si es necesario)
    data_cleaned = data_cleaned.apply(pd.to_numeric, errors='coerce').fillna(0)

    # Renombrar las columnas para facilitar el análisis
    data_cleaned.columns = [col.replace("/", "_").replace(":", "") for col in data_cleaned.columns]
    
    return data_cleaned

def analyze_monitoring_data(data):
    """Realiza análisis básico sobre los datos de monitoreo."""
    if data.empty:
        print("No hay datos suficientes para analizar.")
        return None
    
    try:
        # Ejemplo de análisis: calcular estadísticas descriptivas
        stats = data.describe()
        return stats
    except Exception as e:
        print(f"Error al analizar los datos: {e}")
        return None

def monitoring_etl(file_path, output_file, threshold=0.3):
    """Ejecuta el proceso ETL para el archivo Monitoring.csv."""
    # Extract
    data = extract_monitoring_data(file_path)
    if data is not None:
        # Transform
        transformed_data = transform_monitoring_data(data, threshold=threshold)
        if transformed_data.empty:
            print(f"Advertencia: El archivo {file_path} no tiene columnas con suficientes datos.")
        else:
            # Guardar el archivo transformado
            transformed_data.to_csv(output_file, index=False)
            print(f"Archivo transformado guardado en {output_file}")
            # Analyze
            stats = analyze_monitoring_data(transformed_data)
            return stats
    else:
        print(f"No se pudo procesar el archivo {file_path} correctamente.")
        return None
