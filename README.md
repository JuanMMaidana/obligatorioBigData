# 🎬 Movie Data Lake ETL Pipeline

## 📋 Descripción del Proyecto

Este proyecto implementa un pipeline de datos ETL (Extract, Transform, Load) multi-etapa con **PySpark** para una productora cinematográfica. El pipeline procesa datos de películas a través de tres zonas de un Data Lake para generar KPIs accionables que apoyen la toma de decisiones estratégicas.

## 🏗️ Arquitectura del Data Lake

El pipeline implementa una arquitectura de Data Lake con tres zonas bien definidas:

```
datalake/
├── raw/          # Zona RAW - Datos crudos optimizados
├── refined/      # Zona REFINED - Datos limpios y unificados  
└── curated/      # Zona CURATED - KPIs y productos de datos
```

### 🔄 Flujo de Datos

1. **RAW** → Ingesta de datos crudos desde CSV
2. **REFINED** → Limpieza, transformación y unificación
3. **CURATED** → Agregación y generación de KPIs

## 📁 Estructura del Proyecto

```
obli2/
├── pipelines/
│   ├── 1_ingest_to_raw.py           # Ingesta Raw
│   ├── 2_raw_to_refined.py          # Transformación Refined
│   └── 3_refined_to_curated.py      # Agregación Curated
├── datalake/
│   ├── raw/
│   │   ├── movies_metadata/         # 45,466 registros
│   │   └── credits/                 # 45,476 registros
│   ├── refined/
│   │   └── movies_master.parquet    # 5,304 registros unificados
│   └── curated/
│       ├── director_performance.parquet # 585 directores
│       └── yearly_trends.parquet        # 99 años
├── charts/                          # Visualizaciones generadas
│   ├── top_10_directors_roi.png     # Top 10 directores
│   ├── roi_distribution.png         # Distribución de ROI
│   ├── prolificity_vs_roi.png       # Prolificidad vs ROI
│   ├── market_evolution.png         # Evolución del mercado
│   ├── movies_per_year.png          # Producción por año
│   └── interactive_dashboard.html   # Dashboard interactivo
├── movies_metadata.csv              # Datos fuente original
├── credits.csv                      # Datos fuente original
├── run_pipeline.py                  # Ejecutor principal
├── visualizations.py                # Generador de gráficas
├── requirements.txt                 # Dependencias
└── README.md                        # Este archivo
```

## 🚀 Instalación y Configuración

### Prerrequisitos

- Python 3.8+
- Java 8 o superior (requerido por Spark)
- Los archivos CSV: `movies_metadata.csv` y `credits.csv`

### Instalación

1. **Clonar o descargar el proyecto**
2. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verificar instalación de Java:**
   ```bash
   java -version
   ```

## 🎯 Ejecución del Pipeline

### Opción 1: Ejecutar Pipeline Completo (Recomendado)

```bash
python run_pipeline.py
```

Este comando ejecuta automáticamente las tres etapas del pipeline:

### Opción 2: Ejecutar Etapas Individuales

```bash
# Etapa 1: Ingesta a Raw
python pipelines/1_ingest_to_raw.py

# Etapa 2: Transformación a Refined  
python pipelines/2_raw_to_refined.py

# Etapa 3: Agregación a Curated
python pipelines/3_refined_to_curated.py
```

## 📊 Generación de Visualizaciones

### Instalar Dependencias de Visualización

```bash
pip install matplotlib seaborn plotly
```

### Generar Gráficas y Dashboards

```bash
python visualizations.py
```

Este script genera automáticamente:

**📈 Gráficas Estáticas (PNG):**
- `top_10_directors_roi.png` - Top 10 directores por ROI
- `roi_distribution.png` - Distribución de ROI por director
- `prolificity_vs_roi.png` - Prolificidad vs ROI
- `market_evolution.png` - Evolución del mercado
- `movies_per_year.png` - Producción por año

**🎯 Dashboard Interactivo (HTML):**
- `interactive_dashboard.html` - Dashboard completo con Plotly

## 📊 Productos de Datos Generados

### 1. Director Performance (`director_performance.parquet`)
- **Propósito**: Análisis de rendimiento de directores
- **Métricas**: 
  - Número de películas
  - ROI promedio
  - Ganancia total
  - Presupuesto promedio

### 2. Yearly Trends (`yearly_trends.parquet`)
- **Propósito**: Tendencias anuales del mercado cinematográfico
- **Métricas**:
  - Películas por año
  - Presupuesto promedio anual
  - Recaudación total anual
  - ROI promedio anual

## 🔍 Casos de Uso Empresariales

### 📈 Para Ejecutivos
- **Selección de Directores**: Identificar directores con mejor ROI
- **Planificación de Presupuestos**: Tendencias de inversión por año
- **Análisis de Mercado**: Evolución de la industria cinematográfica

### 📊 Para Analistas
- **Dashboards**: Datos listos para visualización
- **Reportes**: KPIs calculados y validados
- **Investigación**: Dataset maestro para análisis detallado

## 📊 Visualizaciones y Gráficas Sugeridas

### 🎬 Director Performance Dashboard

**1. Top 10 Directores por ROI**
- Gráfico de barras horizontal
- Mostrar ROI promedio vs número de películas
- Color por rango de ROI (verde: alto, amarillo: medio, rojo: bajo)

**2. Distribución de ROI por Director**
- Histograma de ROI promedio
- Mostrar distribución normal/anormal
- Identificar outliers (directores excepcionales)

**3. Director Prolificidad vs ROI**
- Scatter plot: número de películas vs ROI promedio
- Tamaño del punto por ganancia total
- Identificar patrones de éxito

**4. Evolución Temporal de Directores**
- Línea de tiempo por director
- ROI por año de lanzamiento
- Identificar tendencias de carrera

### 📈 Market Trends Dashboard

**5. Evolución del Mercado Cinematográfico**
- Gráfico de líneas múltiples
- Presupuesto promedio, recaudación total, ROI promedio por año
- Identificar épocas doradas y crisis

**6. Distribución de Presupuestos por Año**
- Box plot por década
- Mostrar evolución de inversiones
- Identificar inflación del presupuesto

**7. ROI vs Año de Lanzamiento**
- Scatter plot con línea de tendencia
- Mostrar si las películas antiguas son más rentables
- Identificar patrones históricos

**8. Heatmap de Rentabilidad por Década**
- Matriz de calor: década vs rango de presupuesto
- Color por ROI promedio
- Identificar combinaciones óptimas

### 💰 Financial Analysis Dashboard

**9. Distribución de Ganancias**
- Gráfico de violín (violin plot)
- Mostrar distribución de ganancias por director
- Identificar directores consistentes vs volátiles

**10. ROI vs Presupuesto**
- Scatter plot con regresión
- Mostrar relación entre inversión y retorno
- Identificar punto óptimo de inversión

**11. Tendencias de Ganancia Total**
- Gráfico de área apilada
- Ganancia total por año y director
- Mostrar contribución de cada director

**12. Análisis de Riesgo-Retorno**
- Scatter plot: desviación estándar vs ROI promedio
- Identificar directores de bajo riesgo y alto retorno
- Clasificar por perfil de inversión

### 🛠️ Herramientas de Visualización Recomendadas

- **Python**: Matplotlib, Seaborn, Plotly
- **BI Tools**: Tableau, Power BI, Looker
- **Web**: D3.js, Chart.js, Apache Superset
- **Notebooks**: Jupyter con widgets interactivos

## 🛠️ Detalles Técnicos

### Zona RAW
- **Formato**: Parquet con compresión Snappy
- **Función**: Backup permanente y optimizado de datos originales
- **Transformación**: Mínima (solo timestamp de ingesta)

### Zona REFINED
- **Formato**: Parquet con compresión Snappy
- **Función**: Fuente única de la verdad (Single Source of Truth)
- **Transformaciones**:
  - Limpieza de datos
  - Validación de tipos
  - Extracción de directores (RegEx)
  - Cálculo de métricas (profit, ROI, release_year)

### Zona CURATED
- **Formato**: Parquet con compresión Snappy
- **Función**: KPIs agregados para el negocio
- **Transformaciones**:
  - Agregaciones por director
  - Tendencias temporales
  - Filtros de relevancia estadística

## 🎨 Características del Código

- **Modular**: Cada etapa es independiente y reutilizable
- **Documentado**: Comentarios explicativos en cada función
- **Robusto**: Manejo de errores y validaciones
- **Escalable**: Configuración optimizada de Spark
- **Trazable**: Logs informativos del procesamiento

## 📋 Troubleshooting

### Error: "Java not found"
```bash
# Instalar Java (Ubuntu/Debian)
sudo apt update
sudo apt install default-jdk

# Verificar instalación
java -version
```

### Error: "CSV file not found"
Asegúrate de que los archivos `movies_metadata.csv` y `credits.csv` estén en la raíz del proyecto.

### Error: "Permission denied"
```bash
# Dar permisos de ejecución
chmod +x run_pipeline.py
chmod +x pipelines/*.py
```

## 📞 Soporte

Si encuentras algún problema:
1. Verifica que todas las dependencias estén instaladas
2. Confirma que Java esté configurado correctamente
3. Revisa los logs de error para diagnóstico detallado

## 🏆 Resultados Obtenidos

Al completar exitosamente el pipeline:
- ✅ **3 zonas del Data Lake pobladas** con datos optimizados
- ✅ **2 productos de datos** listos para análisis y dashboards
- ✅ **Insights accionables** para decisiones de negocio
- ✅ **Arquitectura escalable** para futuras expansiones

### 📊 Métricas de Procesamiento

**Etapa 1 (Raw):**
- 📽️ 45,466 registros de películas procesados
- 🎬 45,476 registros de créditos procesados
- ⏱️ Tiempo: ~8 segundos

**Etapa 2 (Refined):**
- 🧹 5,312 películas después de filtros de calidad
- 🎭 44,416 directores extraídos
- 🔗 5,304 películas en dataset maestro final
- 📈 ROI promedio: 807% (8.07)
- ⏱️ Tiempo: ~10 segundos

**Etapa 3 (Curated):**
- 🎬 585 directores con relevancia estadística
- 📅 99 años de tendencias del mercado
- 🏆 KPIs calculados y validados
- ⏱️ Tiempo: ~6 segundos

### 🎯 Insights Clave Generados

**Top 5 Directores por ROI:**
1. John Waters - 12,437% (4 películas)
2. Tobe Hooper - 9,314% (4 películas)
3. Hamilton Luske - 9,135% (3 películas)
4. David Lynch - 8,786% (8 películas)
5. John G. Avildsen - 6,244% (5 películas)

**🏆 Director Más Prolífico:** Steven Spielberg (30 películas)
**💰 Mayor Ganancia Total:** Steven Spielberg ($7.5 mil millones)
**📅 Año Más Rentable:** 1937 (ROI: 12,324%)
**🎯 Mayor Recaudación:** 2016 ($29.5 mil millones)

## 🎯 Resumen Ejecutivo

### 🏆 Logros del Proyecto

✅ **Pipeline ETL Completo**: Implementación exitosa de arquitectura Data Lake multi-etapa
✅ **Procesamiento de Datos**: 45K+ registros procesados en ~25 segundos
✅ **KPIs Generados**: 585 directores analizados, 99 años de tendencias
✅ **Visualizaciones**: 5 gráficas estáticas + 1 dashboard interactivo
✅ **Insights Accionables**: Identificación de directores top y tendencias del mercado

### 💼 Valor de Negocio

**Para Ejecutivos:**
- Identificación de directores con mejor ROI (John Waters: 12,437%)
- Análisis de tendencias históricas del mercado (1915-2017)
- Optimización de presupuestos basada en datos históricos

**Para Analistas:**
- Dataset maestro limpio y validado (5,304 registros)
- KPIs calculados automáticamente
- Visualizaciones listas para dashboards ejecutivos

### 🚀 Próximos Pasos Sugeridos

1. **Integración con BI Tools**: Conectar con Tableau/Power BI
2. **Análisis Predictivo**: Modelos de ML para predecir ROI
3. **Análisis de Géneros**: Incluir análisis por género cinematográfico
4. **Análisis de Actores**: Extender análisis al reparto
5. **Dashboard en Tiempo Real**: Implementar actualizaciones automáticas

---

**🎯 ¡Listo para generar insights que impulsen el éxito de tu productora cinematográfica!** 