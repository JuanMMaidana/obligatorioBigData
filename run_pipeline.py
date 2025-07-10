#!/usr/bin/env python3
"""
Ejecutor Principal del Pipeline ETL Data Lake
============================================

Este script ejecuta secuencialmente todo el pipeline ETL del Data Lake:
1. Ingesta de datos crudos (Raw)
2. Transformación y limpieza (Refined)
3. Agregación y KPIs (Curated)

Autor: Data Engineering Team
Proyecto: Movie Data Lake Pipeline
"""

import subprocess
import sys
import os
import time
from datetime import datetime

def print_banner():
    """Imprime el banner del pipeline"""
    print("=" * 60)
    print("🎬 MOVIE DATA LAKE ETL PIPELINE")
    print("=" * 60)
    print("📊 Arquitectura Multi-Etapa: Raw → Refined → Curated")
    print("🚀 Iniciando pipeline completo...")
    print("=" * 60)

def run_pipeline_stage(stage_number, script_path, stage_name):
    """
    Ejecuta una etapa específica del pipeline.
    
    Args:
        stage_number (int): Número de la etapa
        script_path (str): Ruta al script a ejecutar
        stage_name (str): Nombre descriptivo de la etapa
        
    Returns:
        bool: True si la etapa se ejecutó correctamente, False si falló
    """
    print(f"\n🔄 ETAPA {stage_number}: {stage_name}")
    print("-" * 50)
    
    start_time = time.time()
    
    try:
        # Ejecutar el script
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Mostrar la salida del script
        print(result.stdout)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ ETAPA {stage_number} COMPLETADA en {duration:.2f}s")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ ERROR EN ETAPA {stage_number}:")
        print(f"Código de salida: {e.returncode}")
        print(f"Error: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ ERROR INESPERADO EN ETAPA {stage_number}: {str(e)}")
        return False

def check_prerequisites():
    """
    Verifica que los prerrequisitos estén cumplidos.
    
    Returns:
        bool: True si todo está listo, False si falta algo
    """
    print("\n🔍 Verificando prerrequisitos...")
    
    # Verificar que los archivos CSV existan
    required_files = ["movies_metadata.csv", "credits.csv"]
    
    for file in required_files:
        if not os.path.exists(file):
            print(f"❌ Archivo faltante: {file}")
            return False
        print(f"✅ Archivo encontrado: {file}")
    
    # Verificar que los scripts de pipeline existan
    pipeline_scripts = [
        "pipelines/1_ingest_to_raw.py",
        "pipelines/2_raw_to_refined.py",
        "pipelines/3_refined_to_curated.py"
    ]
    
    for script in pipeline_scripts:
        if not os.path.exists(script):
            print(f"❌ Script faltante: {script}")
            return False
        print(f"✅ Script encontrado: {script}")
    
    print("✅ Todos los prerrequisitos están cumplidos")
    return True

def create_directory_structure():
    """Crea la estructura de directorios necesaria"""
    print("\n📁 Creando estructura de directorios...")
    
    directories = [
        "datalake/raw",
        "datalake/refined", 
        "datalake/curated"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Directorio creado/verificado: {directory}")

def display_final_summary():
    """Muestra un resumen final del pipeline ejecutado"""
    print("\n" + "=" * 60)
    print("🎉 PIPELINE COMPLETADO EXITOSAMENTE")
    print("=" * 60)
    print("\n📊 ESTRUCTURA FINAL DEL DATA LAKE:")
    print("├── datalake/")
    print("│   ├── raw/")
    print("│   │   ├── movies_metadata/     # Datos crudos de películas")
    print("│   │   └── credits/             # Datos crudos de créditos")
    print("│   ├── refined/")
    print("│   │   └── movies_master.parquet # Dataset maestro unificado")
    print("│   └── curated/")
    print("│       ├── director_performance.parquet # KPIs de directores")
    print("│       └── yearly_trends.parquet        # Tendencias anuales")
    print("\n🎯 PRODUCTOS DE DATOS GENERADOS:")
    print("   • Director Performance: Rendimiento de directores")
    print("   • Yearly Trends: Tendencias anuales del mercado")
    print("\n💼 CASOS DE USO:")
    print("   • Selección de directores para proyectos")
    print("   • Análisis de tendencias del mercado")
    print("   • Optimización de presupuestos")
    print("   • Dashboards ejecutivos")
    print("\n🔚 Pipeline ETL finalizado correctamente")

def main():
    """Función principal que ejecuta todo el pipeline"""
    start_time = time.time()
    
    print_banner()
    
    # Paso 1: Verificar prerrequisitos
    if not check_prerequisites():
        print("❌ Los prerrequisitos no están cumplidos. Pipeline abortado.")
        sys.exit(1)
    
    # Paso 2: Crear estructura de directorios
    create_directory_structure()
    
    # Paso 3: Ejecutar pipeline etapa por etapa
    pipeline_stages = [
        (1, "pipelines/1_ingest_to_raw.py", "Ingesta de datos crudos (Raw)"),
        (2, "pipelines/2_raw_to_refined.py", "Transformación y limpieza (Refined)"),
        (3, "pipelines/3_refined_to_curated.py", "Agregación y KPIs (Curated)")
    ]
    
    failed_stages = []
    
    for stage_num, script_path, stage_name in pipeline_stages:
        success = run_pipeline_stage(stage_num, script_path, stage_name)
        if not success:
            failed_stages.append((stage_num, stage_name))
    
    # Paso 4: Mostrar resultados finales
    total_time = time.time() - start_time
    
    if failed_stages:
        print(f"\n❌ PIPELINE FALLIDO después de {total_time:.2f}s")
        print("Etapas fallidas:")
        for stage_num, stage_name in failed_stages:
            print(f"   • Etapa {stage_num}: {stage_name}")
        sys.exit(1)
    else:
        display_final_summary()
        print(f"\n⏱️  Tiempo total de ejecución: {total_time:.2f}s")
        print(f"📅 Finalizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 