#!/usr/bin/env python3
"""
Pipeline 4: Análisis de Géneros Cinematográficos
================================================

Este script implementa un análisis adicional de géneros cinematográficos
para complementar los KPIs existentes del Data Lake.

Análisis Incluidos:
- ROI por género cinematográfico
- Evolución de géneros por década
- Relación género vs presupuesto
- Géneros más rentables por período
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os

def create_spark_session():
    """
    Crea una sesión de Spark configurada para el procesamiento de datos.
    
    Returns:
        SparkSession: Sesión de Spark configurada
    """
    return SparkSession.builder \
        .appName("MovieDataLake-GenreAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def extract_genres_from_master(spark):
    """
    Extrae y procesa géneros desde el dataset maestro.
    
    Args:
        spark (SparkSession): Sesión de Spark
        
    Returns:
        DataFrame: DataFrame con géneros extraídos
    """
    print("🎭 Extrayendo géneros cinematográficos...")
    
    # Leer dataset maestro
    master_df = spark.read.parquet("datalake/refined/movies_master.parquet")
    
    # Función UDF para extraer géneros del JSON
    def extract_genres(genres_str):
        try:
            if genres_str and genres_str != '[]':
                genres_data = json.loads(genres_str.replace("'", '"'))
                if isinstance(genres_data, list):
                    return [genre.get('name', '') for genre in genres_data if genre.get('name')]
                return []
            return []
        except:
            return []
    
    extract_genres_udf = udf(extract_genres, ArrayType(StringType()))
    
    # Extraer géneros y crear filas separadas para cada género
    movies_with_genres = master_df.withColumn("genres", extract_genres_udf(col("genres")))
    
    # Explotar géneros (crear una fila por cada género)
    exploded_genres = movies_with_genres.select(
        "id", "title", "director", "budget", "revenue", "profit", "roi", 
        "release_year", "release_date", explode("genres").alias("genre")
    ).filter(col("genre") != "")
    
    print(f"📊 Registros con géneros extraídos: {exploded_genres.count()}")
    print(f"🎬 Géneros únicos encontrados: {exploded_genres.select('genre').distinct().count()}")
    
    return exploded_genres

def create_genre_performance_kpis(genres_df):
    """
    Crea KPIs de rendimiento por género.
    
    Args:
        genres_df (DataFrame): DataFrame con géneros extraídos
        
    Returns:
        DataFrame: KPIs de rendimiento por género
    """
    print("📊 Creando KPIs de rendimiento por género...")
    
    # Agrupar por género y calcular métricas
    genre_performance = genres_df.groupBy("genre") \
        .agg(
            count("id").alias("numero_peliculas"),
            avg("roi").alias("roi_promedio"),
            sum("profit").alias("ganancia_total"),
            avg("budget").alias("presupuesto_promedio"),
            avg("revenue").alias("recaudacion_promedio"),
            sum("revenue").alias("recaudacion_total"),
            avg("profit").alias("ganancia_promedio")
        )
    
    # Filtrar géneros con relevancia estadística (5 o más películas)
    genre_performance_filtered = genre_performance.filter(
        col("numero_peliculas") >= 5
    )
    
    # Redondear valores
    genre_performance_final = genre_performance_filtered.withColumn(
        "roi_promedio", round(col("roi_promedio"), 3)
    ).withColumn(
        "ganancia_total", round(col("ganancia_total"), 2)
    ).withColumn(
        "presupuesto_promedio", round(col("presupuesto_promedio"), 2)
    ).withColumn(
        "recaudacion_promedio", round(col("recaudacion_promedio"), 2)
    ).withColumn(
        "recaudacion_total", round(col("recaudacion_total"), 2)
    ).withColumn(
        "ganancia_promedio", round(col("ganancia_promedio"), 2)
    )
    
    # Ordenar por ROI promedio descendente
    genre_performance_final = genre_performance_final.orderBy(desc("roi_promedio"))
    
    print(f"📊 Géneros con relevancia estadística: {genre_performance_final.count()}")
    
    return genre_performance_final

def create_genre_decade_analysis(genres_df):
    """
    Crea análisis de géneros por década.
    
    Args:
        genres_df (DataFrame): DataFrame con géneros extraídos
        
    Returns:
        DataFrame: Análisis de géneros por década
    """
    print("📅 Creando análisis de géneros por década...")
    
    # Crear columna de década
    genres_with_decade = genres_df.withColumn(
        "decade", 
        concat(
            floor(col("release_year") / 10) * 10,
            lit("-"),
            floor(col("release_year") / 10) * 10 + 9
        )
    )
    
    # Agrupar por género y década
    genre_decade_analysis = genres_with_decade.groupBy("genre", "decade") \
        .agg(
            count("id").alias("numero_peliculas"),
            avg("roi").alias("roi_promedio"),
            sum("profit").alias("ganancia_total"),
            avg("budget").alias("presupuesto_promedio"),
            sum("revenue").alias("recaudacion_total")
        )
    
    # Redondear valores
    genre_decade_final = genre_decade_analysis.withColumn(
        "roi_promedio", round(col("roi_promedio"), 3)
    ).withColumn(
        "ganancia_total", round(col("ganancia_total"), 2)
    ).withColumn(
        "presupuesto_promedio", round(col("presupuesto_promedio"), 2)
    ).withColumn(
        "recaudacion_total", round(col("recaudacion_total"), 2)
    )
    
    # Ordenar por década y género
    genre_decade_final = genre_decade_final.orderBy("decade", desc("roi_promedio"))
    
    print(f"📊 Combinaciones género-década: {genre_decade_final.count()}")
    
    return genre_decade_final

def create_genre_budget_analysis(genres_df):
    """
    Crea análisis de relación género vs presupuesto.
    
    Args:
        genres_df (DataFrame): DataFrame con géneros extraídos
        
    Returns:
        DataFrame: Análisis de género vs presupuesto
    """
    print("💰 Creando análisis de género vs presupuesto...")
    
    # Crear rangos de presupuesto
    genres_with_budget_range = genres_df.withColumn(
        "budget_range",
        when(col("budget") < 10000000, "Bajo (< $10M")
        .when(col("budget") < 50000000, "Medio ($10M-$50M")
        .when(col("budget") < 100000000, "Alto ($50M-$100M")
        .otherwise("Muy Alto (> $100M")
    )
    
    # Agrupar por género y rango de presupuesto
    genre_budget_analysis = genres_with_budget_range.groupBy("genre", "budget_range") \
        .agg(
            count("id").alias("numero_peliculas"),
            avg("roi").alias("roi_promedio"),
            avg("budget").alias("presupuesto_promedio"),
            sum("profit").alias("ganancia_total")
        )
    
    # Redondear valores
    genre_budget_final = genre_budget_analysis.withColumn(
        "roi_promedio", round(col("roi_promedio"), 3)
    ).withColumn(
        "presupuesto_promedio", round(col("presupuesto_promedio"), 2)
    ).withColumn(
        "ganancia_total", round(col("ganancia_total"), 2)
    )
    
    # Ordenar por género y rango de presupuesto
    genre_budget_final = genre_budget_final.orderBy("genre", "budget_range")
    
    print(f"📊 Combinaciones género-presupuesto: {genre_budget_final.count()}")
    
    return genre_budget_final

def save_genre_analysis(genre_performance_df, genre_decade_df, genre_budget_df):
    """
    Guarda los análisis de géneros en la zona CURATED.
    
    Args:
        genre_performance_df (DataFrame): KPIs de rendimiento por género
        genre_decade_df (DataFrame): Análisis de géneros por década
        genre_budget_df (DataFrame): Análisis de género vs presupuesto
    """
    print("💾 Guardando análisis de géneros en zona CURATED...")
    
    # Guardar rendimiento por género
    genre_perf_output = "datalake/curated/genre_performance.parquet"
    genre_performance_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(genre_perf_output)
    
    print(f"✅ Rendimiento por género guardado en: {genre_perf_output}")
    
    # Guardar análisis por década
    genre_decade_output = "datalake/curated/genre_decade_analysis.parquet"
    genre_decade_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(genre_decade_output)
    
    print(f"✅ Análisis por década guardado en: {genre_decade_output}")
    
    # Guardar análisis de presupuesto
    genre_budget_output = "datalake/curated/genre_budget_analysis.parquet"
    genre_budget_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(genre_budget_output)
    
    print(f"✅ Análisis de presupuesto guardado en: {genre_budget_output}")

def display_genre_insights(genre_performance_df, genre_decade_df, genre_budget_df):
    """
    Muestra insights clave sobre géneros cinematográficos.
    
    Args:
        genre_performance_df (DataFrame): KPIs de rendimiento por género
        genre_decade_df (DataFrame): Análisis de géneros por década
        genre_budget_df (DataFrame): Análisis de género vs presupuesto
    """
    print("\n🎭 INSIGHTS CLAVE DE GÉNEROS CINEMATOGRÁFICOS")
    print("=" * 60)
    
    # Top 5 géneros por ROI
    print("\n🏆 TOP 5 GÉNEROS POR ROI:")
    top_genres = genre_performance_df.limit(5).collect()
    for i, row in enumerate(top_genres, 1):
        print(f"   {i}. {row['genre']} - ROI: {row['roi_promedio']:.1%} ({row['numero_peliculas']} películas)")
    
    # Género más prolífico
    most_prolific_genre = genre_performance_df.orderBy(desc("numero_peliculas")).first()
    print(f"\n📽️  GÉNERO MÁS PROLÍFICO: {most_prolific_genre['genre']} ({most_prolific_genre['numero_peliculas']} películas)")
    
    # Género con mayor ganancia total
    highest_profit_genre = genre_performance_df.orderBy(desc("ganancia_total")).first()
    print(f"💰 GÉNERO CON MAYOR GANANCIA TOTAL: {highest_profit_genre['genre']} (${highest_profit_genre['ganancia_total']:,.0f})")
    
    # Género con mayor presupuesto promedio
    highest_budget_genre = genre_performance_df.orderBy(desc("presupuesto_promedio")).first()
    print(f"💸 GÉNERO CON MAYOR PRESUPUESTO: {highest_budget_genre['genre']} (${highest_budget_genre['presupuesto_promedio']:,.0f} promedio)")

def main():
    """
    Función principal que ejecuta el análisis de géneros.
    """
    print("🚀 Iniciando Pipeline 4: Análisis de Géneros Cinematográficos")
    print("=" * 70)
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    try:
        # Crear directorios de salida si no existen
        os.makedirs("datalake/curated", exist_ok=True)
        
        # Paso 1: Extraer géneros desde el dataset maestro
        genres_df = extract_genres_from_master(spark)
        
        # Paso 2: Crear KPIs de rendimiento por género
        genre_performance_df = create_genre_performance_kpis(genres_df)
        
        # Paso 3: Crear análisis de géneros por década
        genre_decade_df = create_genre_decade_analysis(genres_df)
        
        # Paso 4: Crear análisis de género vs presupuesto
        genre_budget_df = create_genre_budget_analysis(genres_df)
        
        # Paso 5: Guardar análisis de géneros
        save_genre_analysis(genre_performance_df, genre_decade_df, genre_budget_df)
        
        # Paso 6: Mostrar insights de géneros
        display_genre_insights(genre_performance_df, genre_decade_df, genre_budget_df)
        
        print("\n🎉 Análisis de géneros completado exitosamente!")
        print("📁 Productos de datos creados en zona CURATED:")
        print("   • genre_performance.parquet - Rendimiento por género")
        print("   • genre_decade_analysis.parquet - Análisis por década")
        print("   • genre_budget_analysis.parquet - Análisis de presupuesto")
        print("🎭 Insights listos para visualización y análisis")
        
    except Exception as e:
        print(f"❌ Error durante el análisis de géneros: {str(e)}")
        raise
    
    finally:
        # Cerrar sesión de Spark
        spark.stop()

if __name__ == "__main__":
    main() 