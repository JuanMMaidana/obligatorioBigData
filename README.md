# Análisis Estratégico para una Productora Cinematográfica: Un Enfoque Basado en Datos

**Autor:** [Tu Nombre Completo]
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
3.  **Ag