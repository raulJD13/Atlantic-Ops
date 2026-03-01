# src/streamlit/app.py
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st


# -----------------------------
# 0) CONFIG
# -----------------------------
@dataclass(frozen=True)
class Config:
    APP_TITLE: str = "Atlantic-Ops: Maritime Control Tower"
    PAGE_ICON: str = "⚓"

    # Data source
    PARQUET_GLOB: str = "s3://lakehouse/vessel_data_v2/*.parquet"

    # MinIO/DuckDB httpfs
    S3_REGION: str = "us-east-1"
    S3_ENDPOINT: str = "minio:9000"  # docker network name (no localhost)
    S3_ACCESS_KEY: str = "atlantic_admin"
    S3_SECRET_KEY: str = "atlantic_password"
    S3_USE_SSL: bool = False
    S3_URL_STYLE: str = "path"

    # Port (Las Palmas - Puerto de La Luz)
    PORT_CENTER_LAT: float = 28.14
    PORT_CENTER_LON: float = -15.42
    # Simple geofence polygon (lon,lat)
    PORT_POLYGON: Tuple[Tuple[float, float], ...] = (
        (-15.45, 28.10),
        (-15.39, 28.10),
        (-15.39, 28.18),
        (-15.45, 28.18),
    )

    # Data freshness / quality
    STALENESS_WARN_SEC: int = 60
    STALENESS_BAD_SEC: int = 180

    # Cache
    CACHE_TTL_SEC: int = 10


CFG = Config()


# -----------------------------
# 1) UI / THEME
# -----------------------------
st.set_page_config(
    page_title=CFG.APP_TITLE,
    layout="wide",
    page_icon=CFG.PAGE_ICON,
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = """
<style>
/* Page background */
.stApp {
  background: radial-gradient(1200px 700px at 30% 10%, rgba(59,130,246,.15), transparent 55%),
              radial-gradient(900px 600px at 90% 0%, rgba(34,197,94,.12), transparent 50%),
              linear-gradient(180deg, #070a14 0%, #0b1220 45%, #070a14 100%);
  color: #e5e7eb;
}

/* Remove Streamlit header spacing a bit */
header { visibility: hidden; height: 0px; }
section.main > div { padding-top: 1.2rem; }

/* Sidebar */
[data-testid="stSidebar"] {
  background: linear-gradient(180deg, rgba(15,23,42,.92) 0%, rgba(2,6,23,.92) 100%);
  border-right: 1px solid rgba(148,163,184,.18);
}

/* Cards */
.card {
  background: linear-gradient(180deg, rgba(30,41,59,.70) 0%, rgba(15,23,42,.70) 100%);
  border: 1px solid rgba(148,163,184,.18);
  border-radius: 16px;
  padding: 14px 14px;
  box-shadow: 0 8px 30px rgba(0,0,0,.35);
}
.card h3 { margin: 0; font-size: 0.9rem; color: rgba(226,232,240,.85); font-weight: 600; }
.card .big { font-size: 1.8rem; font-weight: 800; margin-top: 6px; letter-spacing: .2px; }
.card .sub { margin-top: 6px; font-size: 0.85rem; color: rgba(148,163,184,.95); }

/* Pills */
.pill {
  display: inline-block;
  padding: 3px 10px;
  border-radius: 999px;
  font-size: .78rem;
  border: 1px solid rgba(148,163,184,.20);
  background: rgba(2,6,23,.45);
  margin-right: 6px;
}

/* Alerts */
.alert {
  border-radius: 14px;
  padding: 12px 12px;
  border: 1px solid rgba(148,163,184,.18);
  background: rgba(2,6,23,.35);
  margin-bottom: 10px;
}
.alert .hdr { display:flex; align-items:center; justify-content:space-between; gap:10px; }
.alert .title { font-weight: 800; }
.alert .meta { color: rgba(148,163,184,.95); font-size:.82rem; margin-top:6px; }
.badge-high { background: rgba(239,68,68,.18); border: 1px solid rgba(239,68,68,.35); color:#fecaca; }
.badge-med  { background: rgba(245,158,11,.18); border: 1px solid rgba(245,158,11,.35); color:#fde68a; }
.badge-low  { background: rgba(59,130,246,.18); border: 1px solid rgba(59,130,246,.35); color:#bfdbfe; }

/* Dataframe tweaks */
[data-testid="stDataFrame"] {
  border-radius: 14px;
  overflow: hidden;
  border: 1px solid rgba(148,163,184,.18);
}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# -----------------------------
# 2) MATH / ENRICHMENT
# -----------------------------
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    lat1r, lon1r = math.radians(lat1), math.radians(lon1)
    lat2r, lon2r = math.radians(lat2), math.radians(lon2)
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1r) * math.cos(lat2r) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def enrich_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df["time_display"] = df["timestamp"].dt.tz_convert(None).dt.strftime("%H:%M:%S")

    # Distance to port center (vectorized-ish)
    df["distance_to_port_km"] = [
        haversine_km(lat, lon, CFG.PORT_CENTER_LAT, CFG.PORT_CENTER_LON)
        for lat, lon in zip(df["lat"].astype(float), df["lon"].astype(float))
    ]

    # Status category
    df["status_category"] = np.where(
        df["in_port"].astype(bool),
        "port",
        np.where(df["speed"].astype(float) > 0.5, "moving", "stopped"),
    )

    # Freshness
    now_utc = datetime.now(timezone.utc)
    df["age_sec"] = (now_utc - df["timestamp"].dt.tz_convert(timezone.utc)).dt.total_seconds()
    df["age_sec"] = df["age_sec"].fillna(1e9)

    # Quality score (simple heuristic)
    # 100 if <warn, 60 if warn..bad, 20 if >bad
    df["quality_score"] = np.where(
        df["age_sec"] <= CFG.STALENESS_WARN_SEC,
        100,
        np.where(df["age_sec"] <= CFG.STALENESS_BAD_SEC, 60, 20),
    )

    # Map style attributes
    color_map = {
        "moving": [16, 185, 129, 190],
        "stopped": [239, 68, 68, 190],
        "port": [59, 130, 246, 200],
    }
    df["color"] = df["status_category"].map(lambda x: color_map.get(x, [148, 163, 184, 190]))
    df["size"] = df["speed"].astype(float).apply(lambda x: float(min(1200, max(280, x * 220))))

    return df


# -----------------------------
# 3) DATA LAYER
# -----------------------------
@st.cache_resource
def get_db_connection() -> Optional[duckdb.DuckDBPyConnection]:
    try:
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute(f"SET s3_region='{CFG.S3_REGION}';")
        con.execute(f"SET s3_endpoint='{CFG.S3_ENDPOINT}';")
        con.execute(f"SET s3_access_key_id='{CFG.S3_ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key='{CFG.S3_SECRET_KEY}';")
        con.execute(f"SET s3_use_ssl={'true' if CFG.S3_USE_SSL else 'false'};")
        con.execute(f"SET s3_url_style='{CFG.S3_URL_STYLE}';")
        return con
    except Exception as e:
        st.error(f"❌ Fallo inicializando DuckDB (httpfs/S3): {e}")
        return None


@st.cache_data(ttl=CFG.CACHE_TTL_SEC)
def load_latest_vessels() -> pd.DataFrame:
    con = get_db_connection()
    if not con:
        return pd.DataFrame()

    # Latest per vessel via window function (fast)
    query = f"""
    WITH latest AS (
      SELECT
        ship_name, lat, lon, speed, heading, status, in_port, timestamp,
        ROW_NUMBER() OVER (PARTITION BY ship_name ORDER BY timestamp DESC) AS rn
      FROM read_parquet('{CFG.PARQUET_GLOB}')
    )
    SELECT * FROM latest WHERE rn = 1
    """
    try:
        df = con.execute(query).df()
        return enrich_df(df)
    except Exception as e:
        msg = str(e)
        # Common first-run issues when files don't exist yet
        if "No files found" in msg or "HTTP Error" in msg:
            return pd.DataFrame()
        st.sidebar.error(f"⚠️ Error leyendo el lakehouse: {msg}")
        return pd.DataFrame()


@st.cache_data(ttl=CFG.CACHE_TTL_SEC)
def load_traffic_timeseries(hours: int = 24, bucket_minutes: int = 10) -> pd.DataFrame:
    """
    Buckets by time, counts total, moving, port, stopped.
    """
    con = get_db_connection()
    if not con:
        return pd.DataFrame()

    # DuckDB: date_bin exists in newer versions; use epoch math for compatibility
    # Create bucket timestamp by flooring to bucket_minutes.
    query = f"""
    WITH base AS (
      SELECT
        ship_name,
        in_port,
        speed,
        timestamp
      FROM read_parquet('{CFG.PARQUET_GLOB}')
      WHERE timestamp >= (NOW() - INTERVAL '{hours} hours')
    ),
    binned AS (
      SELECT
        to_timestamp(
          floor(extract(epoch FROM timestamp) / ({bucket_minutes}*60)) * ({bucket_minutes}*60)
        ) AS ts_bin,
        ship_name,
        in_port,
        speed
      FROM base
    )
    SELECT
      ts_bin,
      COUNT(DISTINCT ship_name) AS vessels_total,
      SUM(CASE WHEN in_port THEN 1 ELSE 0 END) AS rows_in_port,
      SUM(CASE WHEN (NOT in_port) AND speed > 0.5 THEN 1 ELSE 0 END) AS rows_moving,
      SUM(CASE WHEN (NOT in_port) AND speed <= 0.5 THEN 1 ELSE 0 END) AS rows_stopped
    FROM binned
    GROUP BY ts_bin
    ORDER BY ts_bin
    """
    try:
        df = con.execute(query).df()
        if df.empty:
            return df
        df["ts_bin"] = pd.to_datetime(df["ts_bin"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


# -----------------------------
# 4) ALERTS
# -----------------------------
def generate_alerts(
    df: pd.DataFrame,
    speed_high_kn: float,
    near_port_km: float,
    stopped_near_port_km: float,
) -> List[Dict]:
    if df.empty:
        return []

    alerts: List[Dict] = []
    for _, row in df.iterrows():
        ship = str(row.get("ship_name", "Unknown"))
        speed = float(row.get("speed", 0.0))
        dist = float(row.get("distance_to_port_km", 9999))
        in_port = bool(row.get("in_port", False))
        time_disp = str(row.get("time_display", ""))

        alert = None

        # High severity: too fast near port
        if speed >= speed_high_kn and dist <= near_port_km:
            alert = {
                "ship": ship,
                "priority": "high",
                "type": "🚨 Velocidad excesiva cerca del puerto",
                "message": f"{ship} navega a {speed:.1f} kn a {dist:.1f} km del puerto.",
                "speed": speed,
                "distance": dist,
                "time": time_disp,
            }

        # Medium: stopped outside port but close
        elif (speed < 0.2) and (not in_port) and (dist <= stopped_near_port_km):
            alert = {
                "ship": ship,
                "priority": "medium",
                "type": "⚠️ Buque parado en zona de navegación",
                "message": f"{ship} está parado a {dist:.1f} km del puerto (fuera de geofence).",
                "speed": speed,
                "distance": dist,
                "time": time_disp,
            }

        # Low: approaching (close but not in port)
        elif (not in_port) and (dist <= stopped_near_port_km):
            alert = {
                "ship": ship,
                "priority": "low",
                "type": "🧭 Aproximándose al puerto",
                "message": f"{ship} se aproxima (distancia: {dist:.1f} km).",
                "speed": speed,
                "distance": dist,
                "time": time_disp,
            }

        if alert:
            alerts.append(alert)

    prio_order = {"high": 0, "medium": 1, "low": 2}
    alerts.sort(key=lambda a: prio_order.get(a["priority"], 9))
    return alerts


def badge(priority: str) -> str:
    if priority == "high":
        return '<span class="pill badge-high">HIGH</span>'
    if priority == "medium":
        return '<span class="pill badge-med">MED</span>'
    return '<span class="pill badge-low">LOW</span>'


# -----------------------------
# 5) MAP
# -----------------------------
def build_map(df: pd.DataFrame, show_geofence: bool, highlight_ships: Optional[List[str]] = None) -> pdk.Deck:
    layers: List[pdk.Layer] = []

    if show_geofence:
        layers.append(
            pdk.Layer(
                "PolygonLayer",
                data=[{"polygon": list(CFG.PORT_POLYGON)}],
                get_polygon="polygon",
                filled=True,
                stroked=True,
                get_fill_color=[59, 130, 246, 28],
                get_line_color=[147, 197, 253, 110],
                get_line_width=40,
                pickable=False,
            )
        )

    if df.empty:
        view_state = pdk.ViewState(latitude=CFG.PORT_CENTER_LAT, longitude=CFG.PORT_CENTER_LON, zoom=11, pitch=45, bearing=0)
        return pdk.Deck(map_style=pdk.map_styles.DARK, initial_view_state=view_state, layers=layers)

    highlight_ships = highlight_ships or []
    df = df.copy()

    df_hi = df[df["ship_name"].isin(highlight_ships)]
    df_lo = df[~df["ship_name"].isin(highlight_ships)]

    if not df_lo.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=df_lo,
                get_position="[lon, lat]",
                get_fill_color="color",
                get_radius="size",
                pickable=True,
                auto_highlight=True,
                stroked=True,
                get_line_color=[255, 255, 255, 120],
                get_line_width=1,
                opacity=0.75,
                radius_min_pixels=3,
                radius_max_pixels=60,
            )
        )

    if not df_hi.empty:
        df_hi = df_hi.copy()
        df_hi["alert_color"] = [[245, 158, 11, 220] for _ in range(len(df_hi))]
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=df_hi,
                get_position="[lon, lat]",
                get_fill_color="alert_color",
                get_radius=900,
                pickable=True,
                auto_highlight=True,
                stroked=True,
                get_line_color=[255, 255, 0, 200],
                get_line_width=3,
                opacity=0.95,
                radius_min_pixels=6,
                radius_max_pixels=70,
            )
        )

    view_state = pdk.ViewState(latitude=CFG.PORT_CENTER_LAT, longitude=CFG.PORT_CENTER_LON, zoom=11, pitch=45, bearing=0)
    tooltip = {
        "html": """
        <div style="font-family: ui-sans-serif; font-size: 12px;">
          <div style="font-weight:800; font-size: 13px; margin-bottom: 6px;">{ship_name}</div>
          <div>Velocidad: <b>{speed}</b> kn</div>
          <div>Rumbo: <b>{heading}</b>°</div>
          <div>Estado: <b>{status_category}</b></div>
          <div>Distancia puerto: <b>{distance_to_port_km}</b> km</div>
          <div>Edad dato: <b>{age_sec}</b> s</div>
          <div style="margin-top:6px; color: rgba(226,232,240,.8);">Actualizado: {time_display}</div>
        </div>
        """,
        "style": {"backgroundColor": "#0b1220", "color": "white", "border": "1px solid rgba(148,163,184,.25)"},
    }

    return pdk.Deck(map_style=pdk.map_styles.DARK, initial_view_state=view_state, layers=layers, tooltip=tooltip)


# -----------------------------
# 6) CHARTS
# -----------------------------
def chart_speed_hist(df: pd.DataFrame) -> Optional[go.Figure]:
    if df.empty:
        return None
    fig = go.Figure()
    fig.add_trace(go.Histogram(x=df["speed"], nbinsx=18, opacity=0.8, name="Buques"))
    fig.update_layout(
        title="Distribución de velocidades (kn)",
        height=280,
        margin=dict(l=10, r=10, t=50, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="rgba(226,232,240,.92)"),
        xaxis=dict(gridcolor="rgba(148,163,184,.12)"),
        yaxis=dict(gridcolor="rgba(148,163,184,.12)"),
    )
    return fig


def chart_status_bar(df: pd.DataFrame) -> Optional[go.Figure]:
    if df.empty:
        return None
    counts = df["status_category"].value_counts()
    labels = {"moving": "En movimiento", "stopped": "Parado/Fondeado", "port": "En puerto"}
    x = [labels.get(k, k) for k in counts.index]
    y = counts.values
    fig = go.Figure(go.Bar(x=x, y=y, text=y, textposition="auto"))
    fig.update_layout(
        title="Buques por estado",
        height=280,
        margin=dict(l=10, r=10, t=50, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="rgba(226,232,240,.92)"),
        xaxis=dict(gridcolor="rgba(148,163,184,.12)"),
        yaxis=dict(gridcolor="rgba(148,163,184,.12)"),
    )
    return fig


def chart_timeline(ts: pd.DataFrame) -> Optional[go.Figure]:
    if ts.empty:
        return None
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ts["ts_bin"], y=ts["vessels_total"], mode="lines+markers", name="Vessels (unique)"))
    fig.update_layout(
        title="Timeline de tráfico (últimas horas)",
        height=300,
        margin=dict(l=10, r=10, t=50, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="rgba(226,232,240,.92)"),
        xaxis=dict(gridcolor="rgba(148,163,184,.12)"),
        yaxis=dict(gridcolor="rgba(148,163,184,.12)"),
    )
    return fig


def chart_speed_vs_distance(df: pd.DataFrame) -> Optional[go.Figure]:
    if df.empty:
        return None
    fig = go.Figure(
        data=go.Scatter(
            x=df["distance_to_port_km"],
            y=df["speed"],
            mode="markers",
            text=df["ship_name"],
            hovertemplate="Buque: %{text}<br>Distancia: %{x:.1f} km<br>Velocidad: %{y:.1f} kn<extra></extra>",
        )
    )
    fig.update_layout(
        title="Velocidad vs Distancia al puerto",
        height=300,
        margin=dict(l=10, r=10, t=50, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="rgba(226,232,240,.92)"),
        xaxis=dict(title="km", gridcolor="rgba(148,163,184,.12)"),
        yaxis=dict(title="kn", gridcolor="rgba(148,163,184,.12)"),
    )
    return fig


# -----------------------------
# 7) SIDEBAR
# -----------------------------
with st.sidebar:
    st.markdown(f"## {CFG.PAGE_ICON} Atlantic-Ops")
    st.caption("Control Tower • Puerto de La Luz (Gran Canaria)")
    st.markdown("---")

    refresh_rate = st.slider("Frecuencia de actualización (s)", 1, 30, 5)
    st.markdown("### Filtros")
    status_filter = st.multiselect(
        "Estado",
        ["moving", "stopped", "port"],
        default=["moving", "stopped", "port"],
        help="Filtra por estado operacional",
    )
    min_speed = st.slider("Velocidad mínima (kn)", 0.0, 35.0, 0.0, 0.5)
    max_distance = st.slider("Distancia máx al puerto (km)", 1.0, 100.0, 100.0, 1.0)
    ship_search = st.text_input("Buscar buque (contiene)", value="").strip()
    show_geofence = st.checkbox("Mostrar geofence del puerto", value=True)
    max_rows = st.slider("Máx buques en tabla", 20, 500, 120, 10)

    st.markdown("---")
    st.markdown("### ⚠️ Alertas")
    enable_alerts = st.checkbox("Activar alertas", value=True)
    speed_high_kn = st.slider("Umbral velocidad alta (kn)", 5.0, 25.0, 10.0, 0.5)
    near_port_km = st.slider("Cerca del puerto (km)", 0.5, 20.0, 5.0, 0.5)
    stopped_near_port_km = st.slider("Parado cerca (km)", 0.5, 20.0, 3.0, 0.5)

    st.markdown("---")
    st.markdown("### 📈 Timeline")
    hist_hours = st.slider("Ventana histórica (h)", 1, 48, 24)
    bucket_minutes = st.select_slider("Bucket (min)", options=[5, 10, 15, 30], value=10)

    st.markdown("---")
    st.markdown("### Leyenda")
    st.markdown('<span class="pill">moving</span><span style="color:#10b981;">●</span> En movimiento', unsafe_allow_html=True)
    st.markdown('<span class="pill">stopped</span><span style="color:#ef4444;">●</span> Parado/Fondeado', unsafe_allow_html=True)
    st.markdown('<span class="pill">port</span><span style="color:#3b82f6;">●</span> En puerto', unsafe_allow_html=True)


# -----------------------------
# 8) MAIN
# -----------------------------
st.markdown(f"# {CFG.APP_TITLE}")
st.caption("Monitorización en tiempo real del tráfico marítimo • Lakehouse + DuckDB + Streamlit")

df = load_latest_vessels()

if not df.empty:
    # Apply filters
    f = df.copy()
    f = f[f["status_category"].isin(status_filter)]
    f = f[f["speed"].astype(float) >= float(min_speed)]
    f = f[f["distance_to_port_km"].astype(float) <= float(max_distance)]
    if ship_search:
        f = f[f["ship_name"].str.contains(ship_search, case=False, na=False)]

    # Alerts
    alerts: List[Dict] = []
    if enable_alerts:
        alerts = generate_alerts(f, speed_high_kn=speed_high_kn, near_port_km=near_port_km, stopped_near_port_km=stopped_near_port_km)

    # KPIs
    total = len(df)
    moving = int((df["status_category"] == "moving").sum())
    port = int((df["status_category"] == "port").sum())
    stopped = int((df["status_category"] == "stopped").sum())
    avg_speed = float(df["speed"].mean()) if total else 0.0
    freshness_good = int((df["age_sec"] <= CFG.STALENESS_WARN_SEC).sum())
    freshness_pct = (freshness_good / total * 100) if total else 0.0

    active_alerts = len(alerts)
    critical_alerts = sum(1 for a in alerts if a["priority"] == "high")

    kpi_cols = st.columns(6)
    kpis = [
        ("Buques online", f"{total}", f"{freshness_pct:.0f}% datos frescos (<{CFG.STALENESS_WARN_SEC}s)"),
        ("En movimiento", f"{moving}", "Operación activa"),
        ("En puerto", f"{port}", "Dentro de geofence"),
        ("Parados", f"{stopped}", "Riesgo / fondeo"),
        ("Velocidad media", f"{avg_speed:.1f} kn", f"Máx {float(df['speed'].max()):.1f} kn"),
        ("Alertas", f"{active_alerts}", f"{critical_alerts} críticas"),
    ]
    for col, (t, big, sub) in zip(kpi_cols, kpis):
        with col:
            st.markdown(f"""
            <div class="card">
              <h3>{t}</h3>
              <div class="big">{big}</div>
              <div class="sub">{sub}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("## 🛰️ Vista operativa")
    highlight = [a["ship"] for a in alerts] if alerts else []
    st.pydeck_chart(build_map(f, show_geofence=show_geofence, highlight_ships=highlight), height=520)

    tab_live, tab_analytics, tab_alerts, tab_quality = st.tabs(["📍 Live", "📊 Analytics", "⚠️ Alertas", "🧪 Data Quality"])

    with tab_live:
        left, right = st.columns([1.2, 0.8])
        with left:
            st.markdown("### Tabla operativa")
            cols_show = ["ship_name", "speed", "heading", "status_category", "distance_to_port_km", "age_sec", "time_display"]
            show_df = f[cols_show].sort_values(["status_category", "speed"], ascending=[True, False]).head(max_rows)

            st.dataframe(
                show_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "ship_name": "Buque",
                    "speed": st.column_config.NumberColumn("Kn", format="%.1f"),
                    "heading": st.column_config.NumberColumn("Rumbo", format="%.0f°"),
                    "status_category": "Estado",
                    "distance_to_port_km": st.column_config.NumberColumn("Dist (km)", format="%.1f"),
                    "age_sec": st.column_config.NumberColumn("Age (s)", format="%.0f"),
                    "time_display": "Última",
                },
            )

            st.download_button(
                "⬇️ Descargar CSV (filtrado)",
                data=show_df.to_csv(index=False).encode("utf-8"),
                file_name=f"atlantic_ops_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )

        with right:
            st.markdown("### Ranking (Top velocidades)")
            top = f.sort_values("speed", ascending=False).head(10)[["ship_name", "speed", "distance_to_port_km", "status_category"]]
            st.dataframe(
                top,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "ship_name": "Buque",
                    "speed": st.column_config.NumberColumn("Kn", format="%.1f"),
                    "distance_to_port_km": st.column_config.NumberColumn("Dist (km)", format="%.1f"),
                    "status_category": "Estado",
                },
            )

    with tab_analytics:
        c1, c2 = st.columns(2)
        with c1:
            fig = chart_speed_hist(f)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = chart_status_bar(f)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        ts = load_traffic_timeseries(hours=hist_hours, bucket_minutes=bucket_minutes)
        c3, c4 = st.columns(2)
        with c3:
            fig = chart_timeline(ts)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hay suficiente histórico aún para el timeline.")
        with c4:
            fig = chart_speed_vs_distance(f)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

    with tab_alerts:
        st.markdown("### Sistema de alertas")
        if not enable_alerts:
            st.info("Alertas desactivadas en la barra lateral.")
        elif not alerts:
            st.success("✅ No se detectaron alertas relevantes con los umbrales actuales.")
        else:
            # Track toasts (avoid spamming)
            if "seen_alert_keys" not in st.session_state:
                st.session_state.seen_alert_keys = set()

            for a in alerts:
                key = f"{a['priority']}|{a['ship']}|{a['type']}|{a['time']}"
                if a["priority"] == "high" and key not in st.session_state.seen_alert_keys:
                    st.toast(f"🚨 {a['ship']}: {a['type']}", icon="⚠️")
                    st.session_state.seen_alert_keys.add(key)

                st.markdown(
                    f"""
                    <div class="alert">
                      <div class="hdr">
                        <div class="title">{a['type']}</div>
                        <div>{badge(a['priority'])}</div>
                      </div>
                      <div class="meta">{a['message']}</div>
                      <div class="meta">🕒 {a['time']} • 💨 {a['speed']:.1f} kn • 📍 {a['distance']:.1f} km</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    with tab_quality:
        st.markdown("### Calidad del dato (freshness)")
        q = df.copy()
        good = q[q["age_sec"] <= CFG.STALENESS_WARN_SEC]
        warn = q[(q["age_sec"] > CFG.STALENESS_WARN_SEC) & (q["age_sec"] <= CFG.STALENESS_BAD_SEC)]
        bad = q[q["age_sec"] > CFG.STALENESS_BAD_SEC]

        qc1, qc2, qc3 = st.columns(3)
        with qc1:
            st.metric("✅ Frescos", len(good), help=f"<= {CFG.STALENESS_WARN_SEC}s")
        with qc2:
            st.metric("⚠️ Warning", len(warn), help=f"{CFG.STALENESS_WARN_SEC}s .. {CFG.STALENESS_BAD_SEC}s")
        with qc3:
            st.metric("❌ Stale", len(bad), help=f"> {CFG.STALENESS_BAD_SEC}s")

        # Show worst offenders
        st.markdown("#### Peores (más antiguos)")
        worst = q.sort_values("age_sec", ascending=False).head(25)[
            ["ship_name", "age_sec", "time_display", "status_category", "distance_to_port_km"]
        ]
        st.dataframe(
            worst,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ship_name": "Buque",
                "age_sec": st.column_config.NumberColumn("Age (s)", format="%.0f"),
                "time_display": "Última",
                "status_category": "Estado",
                "distance_to_port_km": st.column_config.NumberColumn("Dist (km)", format="%.1f"),
            },
        )

else:
    st.info("🛰️ Escaneando señales AIS en el Data Lake... (esperando a que Spark escriba el primer lote)")
    st.pydeck_chart(
        pdk.Deck(
            map_style=pdk.map_styles.DARK,
            initial_view_state=pdk.ViewState(latitude=CFG.PORT_CENTER_LAT, longitude=CFG.PORT_CENTER_LON, zoom=11),
        ),
        height=520,
    )

st.markdown("---")
st.caption(f"Última actualización UI: {datetime.now().strftime('%H:%M:%S')} • Motor: DuckDB+MinIO OK")

time.sleep(refresh_rate)
st.rerun()