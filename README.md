# Análisis Estratégico para una Productora Cinematográfica: Un Enfoque Basado en Datos

**Autor:** Juan Maidana, Agustín Lorenzo
**Materia:** Big Data
**Fecha:** Julio 2025

---

## 1. Resumen del Proyecto

Este proyecto aborda el desafío de una productora cinematográfica que busca transicionar de una toma de decisiones basada en la intuición a una estrategia fundamentada en evidencia empírica. El objetivo principal es desarrollar una plataforma de analítica de datos que permita optimizar el portafolio de inversiones, la selección de proyectos y la estrategia de producción a través del análisis de datos históricos del mercado.

Para lograrlo, se diseñó e implementó un Data Lake con una arquitectura de múltiples zonas (Raw, Refined, Curated) y se desarrolló un pipeline de datos robusto y escalable utilizando **Apache Spark**. Este pipeline se encarga de la ingesta, limpieza, transformación y agregación de datos de más de 45,000 películas, culminando en la generación de un conjunto de Indicadores Clave de Rendimiento (KPIs).

El análisis de estos KPIs reveló insights estratégicos, entre los que destacan la identificación de los géneros con mejor balance riesgo-recompensa, el descubrimiento de los rangos de presupuesto óptimos para maximizar la rentabilidad y la cuantificación de la tasa de éxito real para diferentes tipos de producciones.

---

## 2. Arquitectura de la Solución

La solución se fundamenta en una arquitectura de **Data Lake** moderna, diseñada para garantizar la calidad, gobernanza y escalabilidad de los datos. La estructura lógica se divide en tres zonas:

* **Zona Raw:** Punto de ingesta donde los datos se almacenan en su formato original. Actúa como una copia de seguridad persistente y auditable, desacoplando la ingesta de la transformación.
* **Zona Refined:** El área de procesamiento donde los datos son limpiados, validados, enriquecidos y unificados. Aquí reside la "fuente única de la verdad" del proyecto.
* **Zona Curated:** La capa final de consumo, donde se publican los productos de datos agregados (KPIs) en un formato optimizado (Parquet), listos para ser analizados y visualizados.

Este diseño multi-capa asegura la trazabilidad del dato y permite que los análisis de negocio se realicen sobre un conjunto de datos curado y de alta confianza.

---

## 3. Metodología y Pipeline de Datos

El procesamiento de datos se realiza a través de un pipeline automatizado y secuencial, orquestado en tres etapas principales:

1.  **Ingesta a la Zona Raw:** Los datos fuente (CSVs) son cargados y convertidos al formato columnar **Apache Parquet**, añadiendo metadatos de ingesta para su trazabilidad.
2.  **Procesamiento a la Zona Refined:** Se ejecutan las tareas intensivas de limpieza de datos, conversión de tipos, y el enriquecimiento a través de `joins`. En esta fase se calculan métricas a nivel de película individual, como el ROI. Se aplica un riguroso filtro de calidad para trabajar únicamente con registros que poseen datos financieros válidos.
3.  **Agregación a la Zona Curated:** Se calculan los KPIs finales y las agregaciones a nivel de negocio (ej. ROI mediano por género, tasa de éxito, etc.), que servirán como base para todo el análisis visual.

---

## 4. Hallazgos Clave y Resultados

El análisis de los datos curados permitió extraer los siguientes insights estratégicos, validados a través de un dashboard de visualización interactivo:

* **El Riesgo Varía Significativamente entre Géneros:** Aunque géneros como "Terror" y "Ciencia Ficción" presentan el potencial de ROI más alto (demostrado por outliers en los Box Plots), su variabilidad es también la mayor. En contraste, géneros como "Animación" y "Aventura" ofrecen un **ROI mediano más consistente y un menor riesgo**, perfilándose como inversiones más seguras.

* **La Tasa de Éxito es un Indicador de Riesgo Crucial:** Se determinó que la probabilidad de que una película sea rentable (ROI > 0) es un indicador más pragmático que el ROI promedio. Géneros con un ROI promedio alto pueden tener una tasa de éxito inferior al 50%, lo que implica un alto riesgo de pérdida. Este análisis permite a la productora construir un portafolio de inversiones más balanceado.

* **Identificación del "Sweet Spot" de Inversión:** El análisis por rangos de presupuesto reveló que las películas de **presupuesto medio (entre $1M y $10M)** ofrecen el balance óptimo entre rentabilidad y riesgo, presentando un ROI mediano robusto y una de las tasas de éxito más elevadas del estudio. Esto sugiere una estrategia de enfoque en este nicho en lugar de competir en el volátil mercado de los blockbusters.

* **Tendencias del Mercado a Largo Plazo:** El análisis histórico muestra un incremento constante en los presupuestos promedio a lo largo de las décadas, mientras que el ROI mediano no ha crecido al mismo ritmo. Esto indica una posible saturación del mercado y una creciente necesidad de estrategias de producción eficientes y basadas en datos para mantener la rentabilidad.

---

## 5. Instrucciones de Uso

Para replicar este análisis y explorar los resultados, siga los siguientes pasos:

1.  **Clonar el repositorio:**
    ```bash
    git clone [https://github.com/JuanMMaidana/obligatorioBigData.git](https://github.com/JuanMMaidana/obligatorioBigData.git)
    cd obligatorioBigData
    ```

2.  **Instalar dependencias:**
    (Asegúrese de tener un entorno de Python 3 y Java 8+ configurado)
    ```bash
    pip install -r requirements.txt
    ```

3.  **Ejecutar el pipeline completo:**
    Este comando ejecutará las tres etapas del ETL y generará los datos en la carpeta `datalake/`.
    ```bash
    python run_pipeline.py
    ```

4.  **Generar las visualizaciones y el reporte:**
    Este comando leerá los datos de la zona `curated` y creará el dashboard en `reports/charts/`.
    ```bash
    python visualize.py
    ```
    El dashboard principal se encontrará en `reports/charts/presentation.html`.

---

## 6. Tecnologías Utilizadas

* **Lenguaje de Programación:** Python
* **Motor de Procesamiento:** Apache Spark
* **Librerías Principales:** PySpark, Pandas, Matplotlib, Seaborn, Plotly
* **Formato de Almacenamiento:** Apache Parquet
* **Control de Versiones:** Git y GitHub