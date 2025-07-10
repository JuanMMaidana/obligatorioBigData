#!/usr/bin/env python3
"""
Pipeline 1: Ingesta de datos crudos al Data Lake
===============================================

Este script implementa la primera etapa del pipeline ETL, donde se realiza la ingesta
de datos desde las fuentes originales (archivos CSV) hacia la zona RAW del Data Lake.

Zona RAW:
- Es una copia fiel y optimizada de la fuente original
- Sirve como backup permanente de los datos históricos
- Formato optimizado (Parquet) para consultas posteriores más eficientes
- Compresión snappy para reducir el espacio de almacenamiento
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *
import os

def create_spark_session():
    """
    Crea una sesión de Spark configurada para el procesamiento de datos.
    
    Returns:
        SparkSession: Sesión de Spark configurada
    """
    return SparkSession.builder \
        .appName("MovieDataLake-RawIngestion") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def ingest_movies_metadata(spark):
    """
    Ingesta los metadatos de películas desde CSV hacia la zona RAW.
    
    Args:
        spark (SparkSession): Sesión de Spark
    """
    print("📽️  Procesando metadatos de películas...")
    
    # Leer archivo CSV de metadatos de películas
    movies_df = spark.read.csv(
        "movies_metadata.csv",
        header=True,
        inferSchema=True,
        escape='"',
        multiLine=True
    )
    
    # Añadir columna de fecha de ingesta para trazabilidad
    movies_df_with_timestamp = movies_df.withColumn(
        "ingestion_date", 
        current_timestamp()
    )
    
    # Guardar en zona RAW como Parquet con compresión snappy
    output_path = "datalake/raw/movies_metadata/"
    movies_df_with_timestamp.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)
    
    print(f"✅ Metadatos de películas guardados en: {output_path}")
    print(f"📊 Registros procesados: {movies_df_with_timestamp.count()}")
    
    return movies_df_with_timestamp

def ingest_credits(spark):
    """
    Ingesta los créditos de películas desde CSV hacia la zona RAW.
    
    Args:
        spark (SparkSession): Sesión de Spark
    """
    print("🎬 Procesando créditos de películas...")
    
    # Leer archivo CSV de créditos
    credits_df = spark.read.csv(
        "credits.csv",
        header=True,
        inferSchema=True,
        escape='"',
        multiLine=True
    )
    
    # Añadir columna de fecha de ingesta para trazabilidad
    credits_df_with_timestamp = credits_df.withColumn(
        "ingestion_date", 
        current_timestamp()
    )
    
    # Guardar en zona RAW como Parquet con compresión snappy
    output_path = "datalake/raw/credits/"
    credits_df_with_timestamp.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)
    
    print(f"✅ Créditos guardados en: {output_path}")
    print(f"📊 Registros procesados: {credits_df_with_timestamp.count()}")
    
    return credits_df_with_timestamp

def main():
    """
    Función principal que ejecuta el pipeline de ingesta a la zona RAW.
    """
    print("🚀 Iniciando Pipeline 1: Ingesta a zona RAW")
    print("=" * 50)
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    try:
        # Crear directorios de salida si no existen
        os.makedirs("datalake/raw", exist_ok=True)
        
        # Ingestar metadatos de películas
        movies_df = ingest_movies_metadata(spark)
        
        # Ingestar créditos
        credits_df = ingest_credits(spark)
        
        print("\n🎉 Pipeline completado exitosamente!")
        print("📁 Datos guardados en zona RAW del Data Lake")
        print("💾 Formato: Parquet con compresión snappy")
        print("🔄 Listos para ser procesados en la siguiente etapa (Refined)")
        
    except Exception as e:
        print(f"❌ Error durante la ingesta: {str(e)}")
        raise
    
    finally:
        # Cerrar sesión de Spark
        spark.stop()

if __name__ == "__main__":
    main() 