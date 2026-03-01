data engineer?
1. EL PROYECTO: "Atlantic-Ops Lakehouse"
Visión: Vamos a construir una plataforma de datos en tiempo real para la logística marítima. No es solo "ver barcos", es un sistema de ingestión y analítica capaz de detectar eventos operativos (como un barco entrando en zona de carga) y guardar histórico fiable.
Valor para tu CV: Demostrarás que sabes manejar el ciclo de vida completo del dato: desde que se genera (API/Sensor) hasta que aporta valor (Dashboard), pasando por un almacenamiento robusto (Lakehouse).
2. EL STACK TECNOLÓGICO "PRO" (Gratuito & Open Source)
Para que sea profesional, añadiremos capas de Calidad y Operaciones al stack original.
A. Infraestructura & Orquestación
* Docker & Docker Compose: Para encapsular todo el entorno. "Infrastructure as Code" local.
* GitHub: Control de versiones.
* GitHub Actions: (El toque PRO) Para CI/CD. Cada vez que hagas `push`, se ejecutarán tests automáticos y linter. Esto enamora a los reclutadores.
B. Capa de Ingestión (Streaming)
* Python 3.12: Con librerías modernas (`pydantic` para validación de datos estricta).
* Apache Kafka: El estándar de la industria para desacoplar productores de consumidores.
C. Capa de Procesamiento & Storage (Lakehouse)
* Apache Spark (Structured Streaming): El motor de procesamiento distribuido.
* Delta Lake: Formato de almacenamiento. Aporta transacciones ACID (fiabilidad) sobre archivos Parquet. Es lo que diferencia un Data Lake (pantano de archivos) de un Lakehouse (gestión seria).
* MinIO: Servidor de almacenamiento de objetos compatible con S3. Simulará la nube (AWS S3) en tu local.
D. Capa de Consumo (Serving Layer)
* DuckDB: Aquí mi recomendación de experto. Usar Spark para alimentar el dashboard en tiempo real es muy pesado. Usaremos DuckDB para que Streamlit lea los datos Delta desde MinIO de forma ultra-rápida. Es una tecnología muy "hot" ahora mismo.
* Streamlit: Para la visualización interactiva (Mapas y KPIs).
3. ROADMAP DE IMPLEMENTACIÓN (Fases del Proyecto)
No vamos a escribir código a lo loco. Vamos a trabajar por "Sprints".
🏁 FASE 0: Setup Profesional & DevOps (Cimientos)
* Configurar repositorio GitHub.
* Estructura de carpetas estándar (tipo `cookiecutter` para Data Science/Engineering).
* Configurar Pre-commit hooks (para que no puedas subir código mal formateado).
* Configurar el `docker-compose.yml` base (Infraestructura).
📡 FASE 1: Ingestión Robusta (Producer)
* Desarrollo del script Python que consulta la API de barcos.
* Implementación de Pydantic para validar que los datos vienen bien antes de enviarlos.
* Envío de mensajes JSON al topic de Kafka `vessel-positions`.
* Extra Pro: Gestión de errores y logs (si la API falla, que no explote el programa).
⚙️ FASE 2: Procesamiento Streaming (Spark & Delta)
* Configuración de Spark para conectarse a Kafka y MinIO.
* Lógica de ETL: Limpieza de datos y casteos de tipos.
* Lógica de Negocio: Geofencing. (¿Está el barco dentro del polígono del puerto? True/False).
* Escritura en Delta Lake (particionado por fecha).
📊 FASE 3: Serving & Visualización
* Conexión de DuckDB con MinIO/Delta Lake.
* Creación del Dashboard en Streamlit:
   * Mapa en tiempo real.
   * Gráficos de "Barcos por hora".
   * Alertas de entrada en puerto.
🚀 FASE 4: Automatización & CI/CD (El "Wow" factor)
* Crear tests unitarios con `pytest` (probar la lógica sin levantar todo el sistema).
* Crear pipeline en GitHub Actions: "Build & Test".
* Documentación final (`README.md` técnico impecable).

## Modelo de Datos: VesselPosition

### Fuente
Datos obtenidos de [API específica o AIS feeds]

### Campos obligatorios
- `mmsi`: Identificador único de 9 dígitos (estándar IMO)
- `ship_name`: Nombre de la embarcación
- `lat`, `lon`: Coordenadas WGS84
- `speed`: Velocidad en nudos
- `heading`: Rumbo verdadero en grados

### Reglas de negocio
1. Coordenadas válidas: lat [-90, 90], lon [-180, 180]
2. MMSI debe estar en rango [100000000, 999999999]
3. Speed máxima: 102.2 nudos (límite físico AIS)
4. Todos los timestamps en UTC

### Ejemplo JSON
```json
{
  "mmsi": 123456789,
  "ship_name": "ATLANTIC VOYAGER",
  "lat": 36.1234,
  "lon": -5.3567,
  "speed": 12.5,
  "heading": 45.0,
  "status": "Under way using engine",
  "timestamp": "2025-01-04T10:30:00Z"
}
```

# Atlantic-Ops Dashboard - Technical Documentation

## 🎯 Overview

Dashboard de visualización en tiempo real para el sistema Atlantic-Ops Maritime Intelligence Platform. Construido con arquitectura modular, siguiendo principios SOLID y mejores prácticas de ingeniería de software.

## 🏗️ Arquitectura del Dashboard

```
┌─────────────────────────────────────────────────────────────┐
│                    PRESENTATION LAYER                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Streamlit  │  │    PyDeck    │  │    Plotly    │      │
│  │      UI      │  │     Maps     │  │    Charts    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    BUSINESS LOGIC LAYER                      │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         VesselAnalyzer (Metrics & Analytics)         │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                       DATA LAYER                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │    DataManager (DuckDB + S3/MinIO Connection)        │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER                             │
│               Delta Lake on MinIO (S3-Compatible)            │
└─────────────────────────────────────────────────────────────┘
```

## 🔑 Características Clave

### 1. **Arquitectura en Capas**
- **Presentation Layer**: Componentes UI (Streamlit, PyDeck, Plotly)
- **Business Layer**: Lógica de análisis y procesamiento
- **Data Layer**: Gestión de conexiones y queries
- **Storage Layer**: Delta Lake como fuente de verdad

### 2. **Gestión de Estado**
- Actualización automática configurable (1-10 segundos)
- Placeholders estáticos para evitar re-renderizado completo
- Cache de conexiones con DuckDB

### 3. **Visualizaciones Avanzadas**

#### Mapa Interactivo (PyDeck)
- **Capas múltiples**: Geofence + Vessels
- **Tooltip HTML personalizado** con información detallada
- **Colores dinámicos**: Verde (en puerto), Rojo (navegando)
- **Tamaño variable**: Basado en velocidad del vessel

#### Gráficos Analíticos (Plotly)
- **Distribución de velocidades**: Histograma interactivo
- **Timeline de tráfico**: Evolución últimas 24h
- **Estado operacional**: Pie chart con estados AIS

### 4. **Sistema de Alertas**
- Velocidad alta configurable
- Detección de datos obsoletos (staleness)
- Visualización mediante boxes coloreados

### 5. **Filtros Avanzados**
- Por zona portuaria
- Por velocidad mínima
- Calidad de datos (freshness)

## 📊 Métricas y KPIs

| Métrica | Descripción | Cálculo |
|---------|-------------|---------|
| **Total Vessels** | Número único de embarcaciones | `COUNT(DISTINCT ship_name)` |
| **In Port** | Vessels en zona portuaria | `SUM(CASE WHEN in_port THEN 1)` |
| **Moving** | Vessels en movimiento | `COUNT(WHERE speed > 0.5)` |
| **Avg Speed** | Velocidad media de la flota | `AVG(speed)` |
| **Data Quality** | % datos frescos (<60s) | `(fresh_count / total) * 100` |

## 🛠️ Componentes Técnicos

### DataManager Class
```python
class DataManager:
    """
    Gestiona conexiones a DuckDB y queries al Lakehouse.
    
    Responsabilidades:
    - Lazy loading de conexión DuckDB
    - Configuración S3/MinIO
    - Queries optimizadas con Window Functions
    - Manejo de errores y logging
    """
```

**Métodos principales:**
- `load_latest_vessels()`: Query con `ROW_NUMBER()` para última posición
- `load_historical_traffic()`: Agregación temporal para timeline
- `_create_connection()`: Setup de DuckDB con extensión httpfs

### VesselAnalyzer Class
```python
class VesselAnalyzer:
    """
    Lógica de análisis y enriquecimiento de datos.
    
    Transformaciones:
    - Cálculo de distancia Haversine al puerto
    - Clasificación por velocidad
    - Asignación de colores para visualización
    - Data quality scoring
    """
```

**Métodos principales:**
- `enrich_dataframe()`: Añade columnas calculadas
- `get_metrics()`: Agrega KPIs
- `_calculate_distance()`: Fórmula Haversine vectorizada

### MapBuilder Class
```python
class MapBuilder:
    """
    Constructor de mapas PyDeck con múltiples capas.
    
    Capas:
    1. PolygonLayer: Geofence del puerto
    2. ScatterplotLayer: Posiciones de vessels
    """
```

### ChartsBuilder Class
```python
class ChartsBuilder:
    """
    Generador de gráficos Plotly.
    
    Gráficos:
    - Speed Distribution (Histogram)
    - Traffic Timeline (Line chart)
    - Status Pie (Donut chart)
    """
```

## 🎨 Sistema de Estilos

### Paleta de Colores
```css
Background: #0a0e27 → #1a1d35 (gradient)
Cards: #1e293b → #334155 (gradient)
Borders: #475569
Text Primary: #e2e8f0
Text Secondary: #94a3b8
Accent Blue: #3b82f6
Success Green: #22c55e
Warning Orange: #f59e0b
Error Red: #ef4444
```

### Alertas
- **Success**: Verde (#064e3b)
- **Warning**: Naranja (#78350f)
- **Error**: Rojo (#7f1d1d)
- **Info**: Azul (#1e3a8a)

## 🚀 Optimizaciones de Rendimiento

### 1. Query Optimization
```sql
-- Uso de Window Functions en lugar de subqueries
WITH ranked_vessels AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY ship_name 
        ORDER BY timestamp DESC
    ) as rn
)
SELECT * FROM ranked_vessels WHERE rn = 1
```

### 2. Incremental Updates
- Solo re-renderiza contenedores que cambiaron
- Placeholders estáticos evitan full page reload

### 3. Data Freshness
- Filtro de datos obsoletos antes de visualizar
- Indicador visual de calidad de datos

### 4. Connection Pooling
- Conexión DuckDB reutilizada (lazy loading)
- Cache de configuración S3

## 📝 Logging Strategy

```python
logger.info("📊 Cargados X vessels")    # Operaciones normales
logger.warning("⚠️ No hay datos...")    # Situaciones recuperables
logger.error("❌ Error cargando...")    # Errores con stack trace
```

Niveles:
- **INFO**: Flujo normal de la aplicación
- **WARNING**: Situaciones anómalas pero manejables
- **ERROR**: Errores que requieren atención

## 🔧 Configuración

Todas las constantes en clase `Config`:

```python
@dataclass
class Config:
    # Lakehouse
    S3_ENDPOINT: str = "localhost:9000"
    S3_BUCKET: str = "lakehouse"
    
    # Geofencing
    PORT_LAT_MIN: float = 28.10
    PORT_LAT_MAX: float = 28.18
    
    # Performance
    DATA_STALENESS_THRESHOLD: int = 60
    CACHE_TTL: int = 300
```

## 📦 Dependencias

```txt
streamlit>=1.28.0
duckdb>=0.9.0
pandas>=2.0.0
pydeck>=0.8.0
plotly>=5.17.0
```

## 🎯 Ejecución

```bash
# Desde el directorio raíz del proyecto
streamlit run src/dashboard/app.py

# Con configuración custom
streamlit run src/dashboard/app.py --server.port 8501 --server.address 0.0.0.0
```

## 🧪 Testing Recommendations

### Unit Tests
```python
def test_vessel_analyzer_enrichment():
    """Test de enriquecimiento de dataframe"""
    df = pd.DataFrame({...})
    enriched = VesselAnalyzer.enrich_dataframe(df, config)
    assert 'distance_to_port' in enriched.columns

def test_data_manager_query():
    """Test de query a DuckDB"""
    dm = DataManager(config)
    df = dm.load_latest_vessels()
    assert not df.empty
```

### Integration Tests
- Mock de conexión S3/MinIO
- Verificar renders sin errores
- Validar cálculos de métricas

## 🚨 Troubleshooting

### Problema: "No hay datos disponibles"
**Causa**: Pipeline no está generando datos
**Solución**:
1. Verificar producer: `docker logs producer`
2. Verificar Kafka: `kafka-console-consumer --topic vessel_positions`
3. Verificar Spark: `docker logs spark-streaming`
4. Verificar MinIO: Navegar a `localhost:9000` → bucket `lakehouse`

### Problema: Dashboard lento
**Causa**: Demasiados datos en query
**Solución**:
1. Reducir ventana temporal en query histórico
2. Aumentar intervalo de refresh
3. Limitar número de rows en tabla

### Problema: Conexión S3 falla
**Causa**: Credenciales o endpoint incorrectos
**Solución**:
1. Verificar variables en `Config`
2. Probar conexión manual: `mc ls minio/lakehouse`

## 📈 Métricas de Calidad del Código

- **Modularidad**: 5 clases especializadas
- **Separación de concerns**: 3 capas distintas
- **Testabilidad**: Inyección de dependencias via `Config`
- **Mantenibilidad**: Docstrings en todas las funciones
- **Escalabilidad**: Queries optimizadas, lazy loading

## 🎓 Conceptos Avanzados Utilizados

1. **Dataclasses**: Para configuración type-safe
2. **Property decorators**: Lazy loading de conexiones
3. **Context managers**: (implícito en Streamlit containers)
4. **Type hints**: En todos los métodos
5. **Window Functions SQL**: Para queries eficientes
6. **Vectorización Pandas**: En cálculos de distancia

## 🏆 Mejores Prácticas Aplicadas

✅ Configuración centralizada  
✅ Logging estructurado  
✅ Manejo de errores robusto  
✅ Separación de concerns  
✅ Código autodocumentado  
✅ Performance optimizado  
✅ UI responsive  

---

**Autor**: Raul Jimenez  
**Versión**: 2.0  
**Stack**: Streamlit + DuckDB + Delta Lake + PyDeck + Plotly  
**Licencia**: MIT


rauljimenez@MacBook-Pro-de-Raul Atlantic-Ops % docker exec -it atlantic-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 512M \
  --executor-memory 512M \
  --conf "spark.executor.memoryOverhead=256M" \
  --conf "spark.network.timeout=10000000" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  /opt/spark/jobs/streaming_etl.py
1322