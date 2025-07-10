#!/usr/bin/env python3
"""
Pipeline 3: Transformación Refined a Curated
============================================

Este script implementa la tercera etapa del pipeline ETL, donde se procesan los datos
de la zona REFINED hacia la zona CURATED del Data Lake.

Zona CURATED:
- Productos de datos agregados y listos para el negocio
- KPIs que responden directamente a preguntas de negocio
- Datos optimizados para dashboards y reportes
- Métricas calculadas con relevancia estadística
- Información estratégica para la toma de decisiones
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    """
    Crea una sesión de Spark configurada para el procesamiento de datos.
    
    Returns:
        SparkSession: Sesión de Spark configurada
    """
    return SparkSession.builder \
        .appName("MovieDataLake-CuratedAggregation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_master_data(spark):
    """
    Carga el dataset maestro desde la zona REFINED.
    
    Args:
        spark (SparkSession): Sesión de Spark
        
    Returns:
        DataFrame: Dataset maestro de películas
    """
    print("📖 Cargando dataset maestro desde zona REFINED...")
    
    master_df = spark.read.parquet("datalake/refined/movies_master.parquet")
    
    print(f"📊 Registros cargados: {master_df.count()}")
    
    return master_df

def create_director_performance_kpis(master_df):
    """
    Crea el producto de datos de rendimiento de directores.
    
    Este KPI responde preguntas clave del negocio como:
    - ¿Qué directores tienen el mejor ROI consistente?
    - ¿Cuáles son los directores más prolíficos?
    - ¿En qué directores deberíamos invertir?
    
    Args:
        master_df (DataFrame): Dataset maestro de películas
        
    Returns:
        DataFrame: KPIs de rendimiento de directores
    """
    print("🎬 Creando KPIs de rendimiento de directores...")
    
    # Agrupar por director y calcular métricas de rendimiento
    director_performance = master_df.groupBy("director") \
        .agg(
            count("id").alias("numero_peliculas"),
            avg("roi").alias("roi_promedio"),
            expr("percentile_approx(roi, 0.5)").alias("roi_mediano"),
            stddev("roi").alias("roi_stddev"),
            sum("profit").alias("ganancia_total"),
            avg("budget").alias("presupuesto_promedio"),
            avg("revenue").alias("recaudacion_promedio")
        )
    
    # Filtrar directores con relevancia estadística (3 o más películas)
    director_performance_filtered = director_performance.filter(
        col("numero_peliculas") >= 3
    )
    
    # Redondear valores decimales para mejor presentación
    director_performance_final = director_performance_filtered.withColumn(
        "roi_promedio", round(col("roi_promedio"), 3)
    ).withColumn(
        "roi_mediano", round(col("roi_mediano"), 3)
    ).withColumn(
        "roi_stddev", round(col("roi_stddev"), 3)
    ).withColumn(
        "ganancia_total", round(col("ganancia_total"), 2)
    ).withColumn(
        "presupuesto_promedio", round(col("presupuesto_promedio"), 2)
    ).withColumn(
        "recaudacion_promedio", round(col("recaudacion_promedio"), 2)
    )
    
    # Ordenar por ROI mediano descendente (priorizar mediana sobre promedio)
    director_performance_final = director_performance_final.orderBy(
        desc("roi_mediano")
    )
    
    print(f"📊 Directores con relevancia estadística: {director_performance_final.count()}")
    
    return director_performance_final

def create_genre_performance_kpis(master_df):
    """
    Crea el producto de datos de rendimiento de géneros.
    
    Este KPI responde preguntas clave del negocio como:
    - ¿Qué géneros tienen el mejor ROI consistente?
    - ¿Cuáles son los géneros más prolíficos?
    - ¿En qué géneros deberíamos invertir?
    
    Args:
        master_df (DataFrame): Dataset maestro de películas
        
    Returns:
        DataFrame: KPIs de rendimiento de géneros
    """
    print("🎭 Creando KPIs de rendimiento de géneros...")
    
    # Crear un DataFrame expandido con múltiples filas por película (una por género)
    # Usar split para dividir por las comas y luego extraer cada género
    genre_df = master_df.select("*") \
        .withColumn("genre_1", regexp_extract(col("genres"), r"'name':\s*'([^']+)'", 1)) \
        .withColumn("genre_2", regexp_extract(col("genres"), r"'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'([^']+)'", 1)) \
        .withColumn("genre_3", regexp_extract(col("genres"), r"'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'([^']+)'", 1))
    
    # Crear DataFrame con una fila por género
    genre_1_df = genre_df.select("*", col("genre_1").alias("genre")).filter(col("genre_1") != "")
    genre_2_df = genre_df.select("*", col("genre_2").alias("genre")).filter(col("genre_2") != "")
    genre_3_df = genre_df.select("*", col("genre_3").alias("genre")).filter(col("genre_3") != "")
    
    # Unir todos los géneros
    all_genres_df = genre_1_df.union(genre_2_df).union(genre_3_df)
    
    # Agrupar por género y calcular métricas de rendimiento
    genre_performance = all_genres_df.groupBy("genre") \
        .agg(
            count("id").alias("numero_peliculas"),
            avg("roi").alias("roi_promedio"),
            expr("percentile_approx(roi, 0.5)").alias("roi_mediano"),
            stddev("roi").alias("roi_stddev"),
            sum("profit").alias("ganancia_total"),
            avg("budget").alias("presupuesto_promedio"),
            avg("revenue").alias("recaudacion_promedio")
        )
    
    # Filtrar géneros con relevancia estadística (10 o más películas)
    genre_performance_filtered = genre_performance.filter(
        col("numero_peliculas") >= 10
    )
    
    # Redondear valores decimales para mejor presentación
    genre_performance_final = genre_performance_filtered.withColumn(
        "roi_promedio", round(col("roi_promedio"), 3)
    ).withColumn(
        "roi_mediano", round(col("roi_mediano"), 3)
    ).withColumn(
        "roi_stddev", round(col("roi_stddev"), 3)
    ).withColumn(
        "ganancia_total", round(col("ganancia_total"), 2)
    ).withColumn(
        "presupuesto_promedio", round(col("presupuesto_promedio"), 2)
    ).withColumn(
        "recaudacion_promedio", round(col("recaudacion_promedio"), 2)
    )
    
    # Ordenar por ROI mediano descendente
    genre_performance_final = genre_performance_final.orderBy(
        desc("roi_mediano")
    )
    
    print(f"📊 Géneros con relevancia estadística: {genre_performance_final.count()}")
    
    return genre_performance_final

def create_genre_roi_distribution(master_df):
    """
    Crea el producto de datos desagregado para Box Plot de géneros.
    
    Este dataset contiene los datos sin agregar para crear visualizaciones
    de distribución detalladas como Box Plots.
    
    Args:
        master_df (DataFrame): Dataset maestro de películas
        
    Returns:
        DataFrame: Datos desagregados de género y ROI
    """
    print("📦 Creando distribución desagregada de géneros y ROI...")
    
    # Extraer géneros del string JSON usando expresión regular
    genre_df = master_df.select("id", "roi", "genres") \
        .withColumn("genre_1", regexp_extract(col("genres"), r"'name':\s*'([^']+)'", 1)) \
        .withColumn("genre_2", regexp_extract(col("genres"), r"'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'([^']+)'", 1)) \
        .withColumn("genre_3", regexp_extract(col("genres"), r"'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'([^']+)'", 1))
    
    # Crear distribución desagregada
    genre_1_dist = genre_df.select("id", "roi", col("genre_1").alias("genre")).filter(col("genre_1") != "")
    genre_2_dist = genre_df.select("id", "roi", col("genre_2").alias("genre")).filter(col("genre_2") != "")
    genre_3_dist = genre_df.select("id", "roi", col("genre_3").alias("genre")).filter(col("genre_3") != "")
    
    # Unir todas las distribuciones
    genre_distribution = genre_1_dist.union(genre_2_dist).union(genre_3_dist) \
        .filter(col("roi").isNotNull() & (col("genre") != ""))
    
    print(f"📊 Registros de distribución: {genre_distribution.count()}")
    
    return genre_distribution

def create_yearly_trends_kpis(master_df):
    """
    Crea el producto de datos de tendencias anuales del mercado.
    
    Este KPI responde preguntas clave del negocio como:
    - ¿Cómo ha evolucionado el mercado cinematográfico?
    - ¿En qué años fue más rentable la industria?
    - ¿Cuáles son las tendencias de presupuesto y recaudación?
    
    Args:
        master_df (DataFrame): Dataset maestro de películas
        
    Returns:
        DataFrame: KPIs de tendencias anuales
    """
    print("📈 Creando KPIs de tendencias anuales del mercado...")
    
    # Agrupar por año y calcular métricas de mercado
    yearly_trends = master_df.groupBy("release_year") \
        .agg(
            count("id").alias("numero_peliculas"),
            avg("budget").alias("presupuesto_promedio"),
            sum("revenue").alias("recaudacion_total"),
            avg("roi").alias("roi_promedio_anual"),
            expr("percentile_approx(roi, 0.5)").alias("roi_mediano_anual"),
            stddev("roi").alias("roi_stddev_anual"),
            sum("profit").alias("ganancia_total_anual"),
            avg("profit").alias("ganancia_promedio")
        )
    
    # Redondear valores decimales para mejor presentación
    yearly_trends_final = yearly_trends.withColumn(
        "presupuesto_promedio", round(col("presupuesto_promedio"), 2)
    ).withColumn(
        "recaudacion_total", round(col("recaudacion_total"), 2)
    ).withColumn(
        "roi_promedio_anual", round(col("roi_promedio_anual"), 3)
    ).withColumn(
        "roi_mediano_anual", round(col("roi_mediano_anual"), 3)
    ).withColumn(
        "roi_stddev_anual", round(col("roi_stddev_anual"), 3)
    ).withColumn(
        "ganancia_total_anual", round(col("ganancia_total_anual"), 2)
    ).withColumn(
        "ganancia_promedio", round(col("ganancia_promedio"), 2)
    )
    
    # Ordenar por año
    yearly_trends_final = yearly_trends_final.orderBy("release_year")
    
    print(f"📊 Años con datos: {yearly_trends_final.count()}")
    
    return yearly_trends_final

def create_genre_success_rate(master_df):
    """
    Crea el producto de datos de tasa de éxito por género.
    
    Esta métrica mide el riesgo de cada género calculando el porcentaje
    de películas que son rentables (ROI > 0).
    
    Args:
        master_df (DataFrame): Dataset maestro de películas
        
    Returns:
        DataFrame: Tasa de éxito por género
    """
    print("📊 Creando tasa de éxito por género...")
    
    # Extraer géneros del string JSON usando expresión regular
    genre_df = master_df.select("*") \
        .withColumn("genre_1", regexp_extract(col("genres"), r"'name':\s*'([^']+)'", 1)) \
        .withColumn("genre_2", regexp_extract(col("genres"), r"'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'([^']+)'", 1)) \
        .withColumn("genre_3", regexp_extract(col("genres"), r"'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'[^']+',\s*[^}]+},\s*\{[^}]*'name':\s*'([^']+)'", 1))
    
    # Crear columna de rentabilidad
    genre_with_profit = genre_df.withColumn("is_profitable", when(col("roi") > 0, 1).otherwise(0))
    
    # Crear DataFrame con una fila por género
    genre_1_df = genre_with_profit.select("*", col("genre_1").alias("genre")).filter(col("genre_1") != "")
    genre_2_df = genre_with_profit.select("*", col("genre_2").alias("genre")).filter(col("genre_2") != "")
    genre_3_df = genre_with_profit.select("*", col("genre_3").alias("genre")).filter(col("genre_3") != "")
    
    # Unir todos los géneros
    all_genres_df = genre_1_df.union(genre_2_df).union(genre_3_df)
    
    # Calcular tasa de éxito por género
    success_rate_df = all_genres_df.groupBy("genre") \
        .agg(
            count("id").alias("total_movies"),
            sum("is_profitable").alias("profitable_movies")
        ) \
        .withColumn("success_rate", col("profitable_movies") / col("total_movies")) \
        .withColumn("success_rate", round(col("success_rate"), 3)) \
        .orderBy(desc("success_rate"))
    
    # Filtrar géneros con relevancia estadística (10 o más películas)
    success_rate_filtered = success_rate_df.filter(col("total_movies") >= 10)
    
    print(f"📊 Géneros con tasa de éxito calculada: {success_rate_filtered.count()}")
    
    return success_rate_filtered

def create_budget_range_analysis(master_df):
    """
    Crea el producto de datos de análisis por rango de presupuesto.
    
    Esta métrica analiza el rendimiento por categorías de presupuesto
    para estrategias de inversión.
    
    Args:
        master_df (DataFrame): Dataset maestro de películas
        
    Returns:
        DataFrame: Análisis por rango de presupuesto
    """
    print("💰 Creando análisis por rango de presupuesto...")
    
    # Crear categorías de presupuesto
    budget_analysis_df = master_df.withColumn(
        "budget_range",
        when(col("budget") < 1000000, "Bajo: <$1M")
        .when(col("budget") < 10000000, "Medio: $1M-$10M")
        .when(col("budget") < 50000000, "Alto: $10M-$50M")
        .otherwise("Blockbuster: >$50M")
    ).withColumn("is_profitable", when(col("roi") > 0, 1).otherwise(0))
    
    # Agrupar por rango de presupuesto y calcular métricas
    budget_range_analysis = budget_analysis_df.groupBy("budget_range") \
        .agg(
            expr("percentile_approx(roi, 0.5)").alias("roi_mediano"),
            count("id").alias("numero_peliculas"),
            sum("is_profitable").alias("profitable_movies")
        ) \
        .withColumn("success_rate", col("profitable_movies") / col("numero_peliculas")) \
        .withColumn("roi_mediano", round(col("roi_mediano"), 3)) \
        .withColumn("success_rate", round(col("success_rate"), 3)) \
        .orderBy("numero_peliculas")
    
    print(f"📊 Rangos de presupuesto analizados: {budget_range_analysis.count()}")
    
    return budget_range_analysis

def save_curated_products(director_performance_df, genre_performance_df, genre_roi_distribution_df, genre_success_rate_df, budget_range_analysis_df, yearly_trends_df):
    """
    Guarda los productos de datos en la zona CURATED.
    
    Args:
        director_performance_df (DataFrame): KPIs de rendimiento de directores
        genre_performance_df (DataFrame): KPIs de rendimiento de géneros
        genre_roi_distribution_df (DataFrame): Distribución desagregada de géneros y ROI
        genre_success_rate_df (DataFrame): Tasa de éxito por género
        budget_range_analysis_df (DataFrame): Análisis por rango de presupuesto
        yearly_trends_df (DataFrame): KPIs de tendencias anuales
    """
    print("💾 Guardando productos de datos en zona CURATED...")
    
    # Guardar rendimiento de directores
    director_output_path = "datalake/curated/director_performance.parquet"
    director_performance_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(director_output_path)
    
    print(f"✅ Rendimiento de directores guardado en: {director_output_path}")
    
    # Guardar rendimiento de géneros
    genre_output_path = "datalake/curated/genre_performance.parquet"
    genre_performance_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(genre_output_path)
    
    print(f"✅ Rendimiento de géneros guardado en: {genre_output_path}")
    
    # Guardar distribución desagregada de géneros y ROI
    genre_dist_output_path = "datalake/curated/genre_roi_distribution.parquet"
    genre_roi_distribution_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(genre_dist_output_path)
    
    print(f"✅ Distribución de géneros y ROI guardada en: {genre_dist_output_path}")
    
    # Guardar tasa de éxito por género
    success_rate_output_path = "datalake/curated/genre_success_rate.parquet"
    genre_success_rate_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(success_rate_output_path)
    
    print(f"✅ Tasa de éxito por género guardada en: {success_rate_output_path}")
    
    # Guardar análisis por rango de presupuesto
    budget_range_output_path = "datalake/curated/budget_range_analysis.parquet"
    budget_range_analysis_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(budget_range_output_path)
    
    print(f"✅ Análisis por rango de presupuesto guardado en: {budget_range_output_path}")
    
    # Guardar tendencias anuales
    yearly_output_path = "datalake/curated/yearly_trends.parquet"
    yearly_trends_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(yearly_output_path)
    
    print(f"✅ Tendencias anuales guardadas en: {yearly_output_path}")

def display_business_insights(director_performance_df, genre_performance_df, yearly_trends_df):
    """
    Muestra insights clave del negocio basados en los KPIs generados.
    
    Args:
        director_performance_df (DataFrame): KPIs de rendimiento de directores
        genre_performance_df (DataFrame): KPIs de rendimiento de géneros
        yearly_trends_df (DataFrame): KPIs de tendencias anuales
    """
    print("\n🔍 INSIGHTS CLAVE DEL NEGOCIO")
    print("=" * 50)
    
    # Top 5 directores por ROI Mediano (métricas más robustas)
    print("\n🏆 TOP 5 DIRECTORES POR ROI MEDIANO:")
    top_directors = director_performance_df.limit(5).collect()
    if top_directors:
        for i, row in enumerate(top_directors, 1):
            print(f"   {i}. {row['director']} - ROI Mediano: {row['roi_mediano']:.1%} | StdDev: {row['roi_stddev']:.1%} ({row['numero_peliculas']} películas)")
    else:
        print("   No hay datos disponibles")
    
    # Top 5 géneros por ROI Mediano
    print("\n🎭 TOP 5 GÉNEROS POR ROI MEDIANO:")
    top_genres = genre_performance_df.limit(5).collect()
    if top_genres:
        for i, row in enumerate(top_genres, 1):
            print(f"   {i}. {row['genre']} - ROI Mediano: {row['roi_mediano']:.1%} | StdDev: {row['roi_stddev']:.1%} ({row['numero_peliculas']} películas)")
    else:
        print("   No hay datos disponibles")
    
    # Director más prolífico
    most_prolific = director_performance_df.orderBy(desc("numero_peliculas")).first()
    if most_prolific:
        print(f"\n📽️  DIRECTOR MÁS PROLÍFICO: {most_prolific['director']} ({most_prolific['numero_peliculas']} películas)")
    else:
        print(f"\n📽️  DIRECTOR MÁS PROLÍFICO: No disponible")
    
    # Director con mayor ganancia total
    highest_profit = director_performance_df.orderBy(desc("ganancia_total")).first()
    if highest_profit:
        print(f"💰 DIRECTOR CON MAYOR GANANCIA TOTAL: {highest_profit['director']} (${highest_profit['ganancia_total']:,.0f})")
    else:
        print(f"💰 DIRECTOR CON MAYOR GANANCIA TOTAL: No disponible")
    
    # Año más rentable por ROI Mediano
    best_year = yearly_trends_df.orderBy(desc("roi_mediano_anual")).first()
    if best_year:
        print(f"\n📅 AÑO MÁS RENTABLE: {best_year['release_year']} (ROI Mediano: {best_year['roi_mediano_anual']:.1%})")
    else:
        print(f"\n📅 AÑO MÁS RENTABLE: No disponible")
    
    # Año con mayor recaudación
    highest_revenue_year = yearly_trends_df.orderBy(desc("recaudacion_total")).first()
    if highest_revenue_year:
        print(f"🎯 AÑO CON MAYOR RECAUDACIÓN: {highest_revenue_year['release_year']} (${highest_revenue_year['recaudacion_total']:,.0f})")
    else:
        print(f"🎯 AÑO CON MAYOR RECAUDACIÓN: No disponible")

def main():
    """
    Función principal que ejecuta el pipeline de agregación REFINED -> CURATED.
    """
    print("🚀 Iniciando Pipeline 3: Refined → Curated")
    print("=" * 50)
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    try:
        # Crear directorios de salida si no existen
        os.makedirs("datalake/curated", exist_ok=True)
        
        # Paso 1: Cargar dataset maestro
        master_df = load_master_data(spark)
        
        # Paso 2: Crear KPIs de rendimiento de directores
        director_performance_df = create_director_performance_kpis(master_df)
        
        # Paso 3: Crear KPIs de rendimiento de géneros
        genre_performance_df = create_genre_performance_kpis(master_df)
        
        # Paso 4: Crear distribución desagregada de géneros y ROI
        genre_roi_distribution_df = create_genre_roi_distribution(master_df)
        
        # Paso 5: Crear tasa de éxito por género
        genre_success_rate_df = create_genre_success_rate(master_df)
        
        # Paso 6: Crear análisis por rango de presupuesto
        budget_range_analysis_df = create_budget_range_analysis(master_df)
        
        # Paso 7: Crear KPIs de tendencias anuales
        yearly_trends_df = create_yearly_trends_kpis(master_df)
        
        # Paso 8: Guardar productos de datos
        save_curated_products(director_performance_df, genre_performance_df, genre_roi_distribution_df, genre_success_rate_df, budget_range_analysis_df, yearly_trends_df)
        
        # Paso 9: Mostrar insights del negocio
        display_business_insights(director_performance_df, genre_performance_df, yearly_trends_df)
        
        print("\n🎉 Pipeline completado exitosamente!")
        print("📊 Productos de datos creados en zona CURATED:")
        print("   • director_performance.parquet (KPIs de directores con ROI mediano)")
        print("   • genre_performance.parquet (KPIs de géneros con ROI mediano)")
        print("   • genre_roi_distribution.parquet (Datos desagregados para Box Plot)")
        print("   • genre_success_rate.parquet (Tasa de éxito por género)")
        print("   • budget_range_analysis.parquet (Análisis por rango de presupuesto)")
        print("   • yearly_trends.parquet (Tendencias anuales con ROI mediano)")
        print("🎯 KPIs listos para dashboards y reportes")
        print("💼 Insights accionables para la toma de decisiones")
        print("🔚 Pipeline ETL completo finalizado")
        
    except Exception as e:
        print(f"❌ Error durante la agregación: {str(e)}")
        raise
    
    finally:
        # Cerrar sesión de Spark
        spark.stop()

if __name__ == "__main__":
    main() 