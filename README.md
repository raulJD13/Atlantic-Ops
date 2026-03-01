# Atlantic-Ops Lakehouse

> Real-time Maritime Logistics Intelligence Platform
> Streaming ingestion → Lakehouse storage (Delta) → Fast serving (DuckDB) → Interactive dashboard (Streamlit)

![Real-Time](https://img.shields.io/badge/Real--Time-Streaming-blue) ![Kafka](https://img.shields.io/badge/Kafka-Event%20Bus-black) ![Spark](https://img.shields.io/badge/Spark-Structured%20Streaming-orange) ![Delta](https://img.shields.io/badge/Delta-Lakehouse-purple) ![MinIO](https://img.shields.io/badge/MinIO-S3%20Local-red) ![DuckDB](https://img.shields.io/badge/DuckDB-Serving-yellow) ![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-ff4b4b)

---

## What is this?

**Atlantic-Ops Lakehouse** is an end-to-end **data engineering** platform focused on **maritime operations**.

This is not only about “tracking vessels on a map”: the goal is to **ingest vessel positions in real time**, **validate them**, **detect operational events** (e.g., a vessel entering a port area via **geofencing**) and **store a reliable historical record** with ACID guarantees on top of a data lake.

What this demonstrates on a CV:

* Streaming architecture (Kafka + Spark Structured Streaming)
* Data contracts and strict validation (Pydantic)
* A proper Lakehouse (Delta Lake) over S3-like object storage (MinIO)
* Efficient serving for analytics/BI (DuckDB)
* Interactive dashboard (Streamlit)
* Professional DevOps: Docker, CI/CD, tests, linting

---

## Architecture

### Data flow (high level)

```
┌──────────────┐     ┌───────────┐     ┌──────────────────────┐
│  AIS / API    │ --> │  Producer  │ --> │   Kafka (vessel-*)   │
└──────────────┘     └───────────┘     └───────────┬──────────┘
                                                    │
                                                    ▼
                                           ┌──────────────────┐
                                           │ Spark Streaming    │
                                           │  + ETL + Geofence  │
                                           └─────────┬─────────┘
                                                     │
                                                     ▼
                                            ┌──────────────────┐
                                            │ Delta Lake (ACID) │
                                            │  on MinIO (S3)    │
                                            └─────────┬─────────┘
                                                     │
                                                     ▼
                                          ┌────────────────────┐
                                          │ DuckDB Serving Layer│
                                          └─────────┬──────────┘
                                                    │
                                                    ▼
                                          ┌────────────────────┐
                                          │ Streamlit Dashboard │
                                          └────────────────────┘
```

### Dashboard layering

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

---

## Tech stack

### Infra and DevOps

* Docker / Docker Compose (local Infrastructure as Code)
* GitHub + GitHub Actions (CI/CD: tests, lint, build)
* Pre-commit hooks (quality gates before commits)

### Ingestion (streaming)

* Python 3.12
* Pydantic (strict validation and data contracts)
* Apache Kafka (event bus)

### Processing and storage (lakehouse)

* Apache Spark 3.5+ (Structured Streaming)
* Delta Lake 3.x (ACID over Parquet)
* MinIO (local S3-compatible object storage)

### Serving and visualization

* DuckDB (fast queries over Delta/Parquet via S3)
* Streamlit + PyDeck + Plotly (UI, maps, charts)

---

## Key features

* End-to-end streaming: positions → Kafka → Spark → Delta
* Data quality by design: Pydantic validation + business rules
* Real lakehouse: schema enforcement, time travel (Delta), reliable history
* Geofencing: port-area entry/exit detection
* Lightweight serving: DuckDB powers the dashboard without overloading Spark
* Dashboard: real-time map, KPIs, alerts
* Basic observability: structured logging and robust error handling

---

## Repository structure (suggested)

This may differ slightly in your repo, but this is a solid “cookiecutter-style” reference.

```
.
├── docker-compose.yml
├── .env.example
├── .github/
│   └── workflows/
│       └── ci.yml
├── src/
│   ├── producer/
│   │   ├── main.py
│   │   ├── models.py          # Pydantic data contracts
│   │   └── settings.py
│   ├── streaming/
│   │   └── streaming_etl.py   # Spark Structured Streaming job
│   └── dashboard/
│       ├── app.py             # Streamlit entrypoint
│       ├── data_manager.py
│       ├── vessel_analyzer.py
│       ├── map_builder.py
│       └── charts_builder.py
├── tests/
│   ├── test_models.py
│   ├── test_geofence.py
│   └── test_queries.py
└── README.md
```

---

## Quickstart

### 1) Prerequisites

* Docker + Docker Compose
* (Optional) `make`
* Python 3.12 for local development (if running modules outside Docker)

### 2) Configure environment

Copy the example and adjust credentials/endpoints as needed.

```bash
cp .env.example .env
```

### 3) Start the platform

```bash
docker compose up -d --build
```

### 4) Verify services

```bash
docker compose ps

docker compose logs -f producer
```

Typical endpoints (may vary depending on `docker-compose.yml`):

* Streamlit: `http://localhost:8501`
* MinIO Console: `http://localhost:9001`
* Spark UI (master): `http://localhost:8080`

### 5) Run the Spark streaming job (example)

If you run the job manually from the Spark master container (as in your setup):

```bash
docker exec -it atlantic-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark/jobs/streaming_etl.py
```

### 6) Stop everything

```bash
docker compose down -v
```

---

## Data contract: `VesselPosition`

### Required fields

* `mmsi`: unique 9-digit identifier
* `ship_name`: vessel name
* `lat`, `lon`: WGS84 coordinates
* `speed`: knots
* `heading`: degrees

### Business rules

1. `lat` in [-90, 90] and `lon` in [-180, 180]
2. `mmsi` in [100000000, 999999999]
3. `speed` <= 102.2 (physical AIS limit)
4. `timestamp` always in UTC

### Example payload

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

---

## Streaming ETL (Spark + Delta)

### What the job does

* Consumes JSON events from Kafka (topic `vessel-positions`)
* Cleans/casts types and normalizes timestamps
* Enriches with business logic: geofencing (`in_port` flag)
* Writes to Delta Lake in MinIO
* Recommended partitioning: `date = to_date(timestamp)`

### Why Delta Lake

A data lake without transactions tends to become a messy collection of files. Delta provides:

* ACID guarantees and consistency
* Schema enforcement and schema evolution
* Time travel and auditing
* Safe upsert/merge operations (if needed)

---

## Serving layer (DuckDB)

Spark is excellent for processing, but it is not ideal to power an interactive dashboard directly.

DuckDB enables:

* Connecting to S3/MinIO (via `httpfs`)
* Reading Delta/Parquet tables and aggregating quickly
* Using SQL window functions for “latest position per vessel” queries

Example strategy (latest row per `ship_name`):

```sql
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ship_name ORDER BY timestamp DESC) AS rn
  FROM vessel_positions
)
SELECT *
FROM ranked
WHERE rn = 1;
```

---

## Dashboard (Streamlit)

### Capabilities

* Interactive real-time map (PyDeck): vessels + port geofence polygon
* KPIs: total vessels, in_port, moving, avg_speed, data_quality
* Alerts: high speed, stale data (staleness)
* Filters: by zone, speed, data quality

### KPI catalog

| Metric        | Description              | Computation                         |
| ------------- | ------------------------ | ----------------------------------- |
| Total Vessels | Unique vessels           | `COUNT(DISTINCT ship_name)`         |
| In Port       | Vessels inside port zone | `SUM(CASE WHEN in_port THEN 1 END)` |
| Moving        | Vessels moving           | `COUNT(WHERE speed > 0.5)`          |
| Avg Speed     | Fleet average speed      | `AVG(speed)`                        |
| Data Quality  | Fresh data percentage    | `(fresh / total) * 100`             |

---

## Testing and quality

### Unit tests (pytest)

* Model validation (Pydantic)
* Geofencing (inside/outside/edge cases)
* DuckDB queries (small fixtures)

```bash
pytest -q
```

### Lint and formatting (recommended)

* `ruff` + `black` + `mypy` (optional)
* Pre-commit to run checks automatically

---

## CI/CD (GitHub Actions)

Typical pipeline on `push`:

* Install dependencies
* Lint and format checks
* Run unit tests
* Build (if applicable)

This is a strong signal of engineering discipline when someone reviews the repository.

---

## Troubleshooting

### “No data available” on the dashboard

1. Producer is running:

   ```bash
   docker compose logs -f producer
   ```
2. Kafka receives messages (if you have a CLI): consume the topic
3. Spark job is running and healthy:

   ```bash
   docker compose logs -f spark-master
   ```
4. MinIO bucket contains objects (check via MinIO Console)

### Dashboard is slow

* Reduce historical window
* Increase refresh interval
* Limit row counts in tables

### S3/MinIO connection errors

* Check endpoint and credentials in `.env` / `Config`
* Ensure MinIO is reachable from DuckDB/Spark inside the Docker network

---

## Roadmap

* Phase 0: Professional setup and DevOps

  * repo structure, pre-commit, docker-compose base
* Phase 1: Robust ingestion

  * producer, Pydantic, logging
* Phase 2: Streaming ETL

  * Structured Streaming, geofencing, Delta partitioning
* Phase 3: Serving and visualization

  * DuckDB, Streamlit, metrics
* Phase 4: Automation and CI/CD

  * pytest, GitHub Actions, final documentation

---

## Contributing

1. Fork the repo
2. Create a branch: `git checkout -b feat/new-feature`
3. Run tests/lint before opening a PR
4. Open a Pull Request with a clear description and screenshots if applicable

---

## License

MIT.

---

## Author

Raul Jimenez
Contact: `rauljimenez@MacBook-Pro-de-Raul` (update with a real email before publishing)

---

## Note

With a single command you can spin up a complete, containerized data platform:

* Kafka (event bus)
* Spark (distributed processing)
* MinIO (S3-compatible lakehouse storage)
* Producer (real-time ingestion)
* Streaming ETL job
* Analytics dashboard
