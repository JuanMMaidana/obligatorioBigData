#!/usr/bin/env python3
"""
Pipeline 2: Transformación Raw a Refined
========================================

Este script implementa la segunda etapa del pipeline ETL, donde se procesan los datos
de la zona RAW hacia la zona REFINED del Data Lake.

Zona REFINED:
- Datos limpios, validados y enriquecidos
- Fuente única de la verdad (Single Source of Truth)
- Datos preparados y listos para análisis
- Esquema consistente y tipos de datos correctos
- Información de calidad empresarial
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import os

def create_spark_session():
    """
    Crea una sesión de Spark configurada para el procesamiento de datos.
    
    Returns:
        SparkSession: Sesión de Spark configurada
    """
    return SparkSession.builder \
        .appName("MovieDataLake-RefinedTransformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def clean_movies_data(spark):
    """
    Limpia y transforma los datos de películas desde la zona RAW.
    
    Args:
        spark (SparkSession): Sesión de Spark
        
    Returns:
        DataFrame: DataFrame limpio de películas
    """
    print("🧹 Limpiando datos de películas...")
    
    # Leer datos de películas desde zona RAW
    movies_df = spark.read.parquet("datalake/raw/movies_metadata/")
    
    print(f"📊 Registros iniciales: {movies_df.count()}")
    
    # Filtrar filas donde el id sea un número válido
    movies_cleaned = movies_df.filter(
        col("id").isNotNull() & 
        col("id").rlike("^[0-9]+$")
    )
    
    print(f"📊 Después de filtrar IDs válidos: {movies_cleaned.count()}")
    
    # Convertir tipos de datos
    movies_cleaned = movies_cleaned.withColumn("id", col("id").cast(IntegerType())) \
                                   .withColumn("budget", col("budget").cast(DoubleType())) \
                                   .withColumn("revenue", col("revenue").cast(DoubleType())) \
                                   .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))
    
    # Filtrar datos de baja calidad: mantener solo películas con budget > 1000 Y revenue > 1000
    movies_quality = movies_cleaned.filter(
        (col("budget") > 1000) & 
        (col("revenue") > 1000)
    )
    
    print(f"📊 Después de filtrar calidad (budget > 1000 y revenue > 1000): {movies_quality.count()}")
    
    return movies_quality

def extract_director_info(spark):
    """
    Extrae información de directores desde los datos de créditos.
    
    Args:
        spark (SparkSession): Sesión de Spark
        
    Returns:
        DataFrame: DataFrame con información de directores
    """
    print("🎭 Extrayendo información de directores...")
    
    # Leer datos de créditos desde zona RAW
    credits_df = spark.read.parquet("datalake/raw/credits/")
    
    # Convertir id a Integer
    credits_df = credits_df.withColumn("id", col("id").cast(IntegerType()))
    
    # Extraer el nombre del director usando expresión regular
    # Patrón: 'job': 'Director' ... 'name': 'Director Name'
    director_pattern = r"'job':\s*'Director'[^}]*'name':\s*'([^']+)'"
    
    credits_with_director = credits_df.withColumn(
        "director",
        regexp_extract(col("crew"), director_pattern, 1)
    )
    
    # Seleccionar solo id y director, filtrar filas donde el director esté vacío
    directors_df = credits_with_director.select("id", "director") \
                                       .filter(col("director") != "")
    
    print(f"📊 Directores extraídos: {directors_df.count()}")
    
    return directors_df

def create_master_dataset(movies_df, directors_df):
    """
    Crea el dataset maestro unificado con enriquecimientos.
    
    Args:
        movies_df (DataFrame): DataFrame limpio de películas
        directors_df (DataFrame): DataFrame de directores
        
    Returns:
        DataFrame: Dataset maestro enriquecido
    """
    print("🔗 Creando dataset maestro...")
    
    # Unir películas con directores usando inner join
    master_df = movies_df.join(directors_df, on="id", how="inner")
    
    print(f"📊 Registros después del join: {master_df.count()}")
    
    # Enriquecimiento: Crear nuevas columnas calculadas
    master_enriched = master_df.withColumn(
        "profit", 
        col("revenue") - col("budget")
    ).withColumn(
        "roi", 
        when(col("budget") > 0, col("profit") / col("budget"))
        .otherwise(0)  # Manejo de división por cero
    ).withColumn(
        "release_year", 
        year(col("release_date"))
    )
    
    # Filtrar registros con años válidos
    master_enriched = master_enriched.filter(col("release_year").isNotNull())
    
    print(f"📊 Registros finales en dataset maestro: {master_enriched.count()}")
    
    return master_enriched

def save_refined_data(master_df):
    """
    Guarda el dataset maestro en la zona REFINED.
    
    Args:
        master_df (DataFrame): Dataset maestro enriquecido
    """
    print("💾 Guardando dataset maestro en zona REFINED...")
    
    output_path = "datalake/refined/movies_master.parquet"
    
    master_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)
    
    print(f"✅ Dataset maestro guardado en: {output_path}")
    
    # Mostrar estadísticas del dataset
    print("\n📈 Estadísticas del dataset maestro:")
    print(f"   • Total de películas: {master_df.count()}")
    print(f"   • Directores únicos: {master_df.select('director').distinct().count()}")
    
    # Obtener rango de años de forma segura
    year_range = master_df.agg(min('release_year'), max('release_year')).collect()[0]
    print(f"   • Rango de años: {year_range}")
    
    # Obtener ROI promedio de forma segura
    roi_avg = master_df.agg(avg('roi')).collect()[0][0]
    if roi_avg is not None:
        print(f"   • ROI promedio: {roi_avg:.2f}")
    else:
        print(f"   • ROI promedio: No disponible")

def main():
    """
    Función principal que ejecuta el pipeline de transformación RAW -> REFINED.
    """
    print("🚀 Iniciando Pipeline 2: Raw → Refined")
    print("=" * 50)
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    try:
        # Crear directorios de salida si no existen
        os.makedirs("datalake/refined", exist_ok=True)
        
        # Paso 1: Limpiar datos de películas
        movies_clean = clean_movies_data(spark)
        
        # Paso 2: Extraer información de directores
        directors_df = extract_director_info(spark)
        
        # Paso 3: Crear dataset maestro unificado
        master_df = create_master_dataset(movies_clean, directors_df)
        
        # Paso 4: Guardar en zona REFINED
        save_refined_data(master_df)
        
        print("\n🎉 Pipeline completado exitosamente!")
        print("📁 Dataset maestro creado en zona REFINED")
        print("🔍 Datos limpios, validados y enriquecidos")
        print("📊 Fuente única de la verdad lista para análisis")
        print("🔄 Listos para ser agregados en la siguiente etapa (Curated)")
        
    except Exception as e:
        print(f"❌ Error durante la transformación: {str(e)}")
        raise
    
    finally:
        # Cerrar sesión de Spark
        spark.stop()

if __name__ == "__main__":
    main() 