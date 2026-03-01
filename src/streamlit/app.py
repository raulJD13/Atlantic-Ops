import streamlit as st
import duckdb
import pandas as pd
import numpy as np
import pydeck as pdk
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime
import math

# 1. CONFIGURACIÓN GLOBAL Y ESTILOS LIMPIOS

st.set_page_config(
    page_title="Atlantic-Ops: Maritime Control Tower",
    layout="wide",
    page_icon="⚓",
    initial_sidebar_state="expanded"
)

# CSS minimalista y profesional
st.markdown("""
<style>
    .stApp { background-color: #0e1117; }
    h1, h2, h3 { color: #ffffff; font-weight: 600; margin-bottom: 1rem; }
    
    .metric-container {
        background-color: #1a1c24;
        border: 1px solid #2d303e;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.5rem;
    }
    .metric-value { font-size: 1.8rem; font-weight: 700; color: #ffffff; margin: 0.25rem 0; }
    .metric-label { font-size: 0.9rem; color: #9ca3af; margin: 0; }
    .metric-trend { font-size: 0.8rem; margin: 0; }
    
    .status-indicator { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
    .status-moving { background-color: #10B981; }
    .status-stopped { background-color: #EF4444; }
    .status-port { background-color: #3B82F6; }
    
    .alert-container { background-color: #1a1c24; border: 1px solid #2d303e; border-radius: 8px; padding: 1rem; margin-bottom: 1rem; }
    .alert-high { border-left: 4px solid #EF4444; }
    .alert-medium { border-left: 4px solid #F59E0B; }
    .alert-low { border-left: 4px solid #3B82F6; }
    
    .alert-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem; }
    .alert-title { font-weight: 600; color: white; font-size: 0.9rem; }
    .alert-priority { font-size: 0.7rem; padding: 2px 8px; border-radius: 10px; font-weight: 600; }
    
    .priority-high { background-color: #EF4444; color: white; }
    .priority-medium { background-color: #F59E0B; color: black; }
    .priority-low { background-color: #3B82F6; color: white; }
    
    .alert-message { color: #9ca3af; font-size: 0.85rem; margin: 0.25rem 0; }
    .alert-footer { display: flex; justify-content: space-between; margin-top: 0.5rem; font-size: 0.75rem; color: #6B7280; }
</style>
""", unsafe_allow_html=True)

# 2. FUNCIONES DE UTILIDAD PARA ALERTAS
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def generate_alerts(df, speed_threshold=10, distance_threshold=3):
    alerts = []
    port_center_lat, port_center_lon = 28.14, -15.42
    
    if df.empty: return alerts
    
    for _, row in df.iterrows():
        alert = None
        distance_to_port = calculate_distance(row['lat'], row['lon'], port_center_lat, port_center_lon)
        
        if row['speed'] > speed_threshold and distance_to_port < 5:
            alert = {'ship': row['ship_name'], 'type': '🚨 Velocidad excesiva', 'message': f"{row['ship_name']} navega a {row['speed']:.1f} kn cerca del puerto", 'priority': 'high', 'speed': row['speed'], 'time': row['time_display']}
        elif row['speed'] < 0.1 and not row['in_port'] and distance_to_port < distance_threshold:
            alert = {'ship': row['ship_name'], 'type': '⚠️ Barco parado', 'message': f"{row['ship_name']} está parado en zona de navegación", 'priority': 'medium', 'speed': row['speed'], 'time': row['time_display']}
        elif not row['in_port'] and distance_to_port < distance_threshold:
            alert = {'ship': row['ship_name'], 'type': '📍 Aproximándose', 'message': f"{row['ship_name']} se acerca al puerto ({distance_to_port:.1f} km)", 'priority': 'low', 'speed': row['speed'], 'time': row['time_display']}
        elif row['speed'] > 8 and not row['in_port']:
            alert = {'ship': row['ship_name'], 'type': '⚡ Velocidad alta', 'message': f"{row['ship_name']} navega a {row['speed']:.1f} kn", 'priority': 'low', 'speed': row['speed'], 'time': row['time_display']}
        
        if alert: alerts.append(alert)
    return sorted(alerts, key=lambda x: {'high': 0, 'medium': 1, 'low': 2}[x['priority']])

# 3. FUNCIONES DE VISUALIZACIÓN SIMPLIFICADAS
def create_simple_map(df, show_geofence, alerts=[]):
    layers = []
    if show_geofence:
        PORT_POLYGON = [[-15.45, 28.10], [-15.39, 28.10], [-15.39, 28.18], [-15.45, 28.18]]
        layers.append(pdk.Layer(
            "PolygonLayer", data=[{"polygon": PORT_POLYGON}], get_polygon="polygon",
            filled=True, stroked=True, get_fill_color=[0, 100, 255, 20],
            get_line_color=[0, 200, 255, 100], get_line_width=30, pickable=False
        ))
    
    alert_ships = [alert['ship'] for alert in alerts] if alerts else []
    df_with_alerts = df[df['ship_name'].isin(alert_ships)].copy()
    df_normal = df[~df['ship_name'].isin(alert_ships)].copy()
    
    if not df_normal.empty:
        layers.append(pdk.Layer(
            "ScatterplotLayer", data=df_normal, get_position='[lon, lat]',
            get_fill_color='color', get_radius='size', pickable=True, auto_highlight=True,
            stroked=True, get_line_color=[255, 255, 255, 150], get_line_width=1, opacity=0.7,
            radius_min_pixels=3, radius_max_pixels=50
        ))
    
    if not df_with_alerts.empty:
        df_with_alerts['alert_color'] = [[255, 100, 0, 200] for _ in range(len(df_with_alerts))]
        layers.append(pdk.Layer(
            "ScatterplotLayer", data=df_with_alerts, get_position='[lon, lat]',
            get_fill_color='alert_color', get_radius=500, pickable=True, auto_highlight=True,
            stroked=True, get_line_color=[255, 255, 0, 200], get_line_width=3, opacity=0.9,
            radius_min_pixels=6, radius_max_pixels=60
        ))
    
    view_state = pdk.ViewState(latitude=28.14, longitude=-15.42, zoom=11, pitch=45, bearing=0)
    tooltip = {
        "html": "<div style='padding: 8px; background: #1a1c24; color: white; border-radius: 4px; border: 1px solid #2d303e;'>"
                "<b>{ship_name}</b><br/>Velocidad: {speed:.1f} kn<br/>Rumbo: {heading}°<br/>Estado: {status_category}<br/>Actualizado: {time_display}</div>",
        "style": {"backgroundColor": "#1a1c24", "color": "white"}
    }
    return pdk.Deck(map_style=pdk.map_styles.DARK, initial_view_state=view_state, layers=layers, tooltip=tooltip)

def create_speed_chart(df):
    if df.empty: return None
    fig = go.Figure()
    fig.add_trace(go.Histogram(x=df['speed'], nbinsx=15, marker_color='#4F46E5', opacity=0.7, name='Buques'))
    fig.update_layout(title="Distribución de velocidades", xaxis_title="Velocidad (nudos)", yaxis_title="Número de buques", height=250, margin=dict(l=20, r=20, t=40, b=20), plot_bgcolor='#1a1c24', paper_bgcolor='#1a1c24', font_color='white')
    return fig

def create_status_chart(df):
    if df.empty: return None
    status_counts = df['status_category'].value_counts()
    status_labels = {'moving': 'En movimiento', 'stopped': 'Fondeado', 'port': 'En puerto'}
    colors = {'moving': '#10B981', 'stopped': '#EF4444', 'port': '#3B82F6'}
    fig = go.Figure(data=[go.Bar(x=[status_labels.get(s, s) for s in status_counts.index], y=status_counts.values, marker_color=[colors.get(s, '#6B7280') for s in status_counts.index], text=status_counts.values, textposition='auto')])
    fig.update_layout(title="Buques por estado", height=250, margin=dict(l=20, r=20, t=40, b=20), plot_bgcolor='#1a1c24', paper_bgcolor='#1a1c24', font_color='white')
    return fig

# 4. CAPA DE DATOS CON MANEJO DE ERRORES ROBUSTO
@st.cache_resource
def get_db_connection():
    """Conexión persistente a DuckDB apuntando a la red Docker"""
    try:
        con = duckdb.connect(database=':memory:')
        con.execute("""
            INSTALL httpfs; LOAD httpfs;
            SET s3_region='us-east-1';
            SET s3_endpoint='minio:9000';  -- CORRECCIÓN: Apuntamos al contenedor MinIO, no a localhost
            SET s3_access_key_id='atlantic_admin';
            SET s3_secret_access_key='atlantic_password';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        return con
    except Exception as e:
        st.error(f"Fallo inicializando DuckDB: {e}")
        return None

def load_realtime_data():
    """Lee datos controlando el momento en el que el Data Lake aún está vacío"""
    con = get_db_connection()
    if not con: return pd.DataFrame()
    
    try:
        query = """
        WITH latest_data AS (
            SELECT 
                ship_name, lat, lon, speed, heading, status, in_port, timestamp,
                ROW_NUMBER() OVER (PARTITION BY ship_name ORDER BY timestamp DESC) as rn
            FROM read_parquet('s3://lakehouse/vessel_data_v2/*.parquet')
        )
        SELECT * FROM latest_data WHERE rn = 1
        """
        df = con.execute(query).df()
        
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['time_display'] = df['timestamp'].dt.strftime('%H:%M:%S')
            df['status_category'] = df.apply(lambda row: 'port' if row['in_port'] else ('moving' if row['speed'] > 0.5 else 'stopped'), axis=1)
            
            color_map = {'moving': [16, 185, 129, 200], 'stopped': [239, 68, 68, 200], 'port': [59, 130, 246, 200]}
            df['color'] = df['status_category'].map(lambda x: color_map.get(x, [128, 128, 128, 200]))
            df['size'] = df['speed'].apply(lambda x: min(1000, max(300, x * 200)))
            
        return df
    except Exception as e:
        error_msg = str(e)
        # Si Spark aún no ha guardado ficheros, DuckDB lanza un error de "No files found". Es normal.
        if "No files found" not in error_msg and "HTTP Error" not in error_msg:
            st.sidebar.error(f"⚠️ Error de lectura: {error_msg}")
        return pd.DataFrame()

# 5. SIDEBAR & INTERFAZ
with st.sidebar:
    st.title("⚓ Atlantic-Ops")
    st.markdown("---")
    refresh_rate = st.slider("Frecuencia de actualización (s)", 1, 30, 5)
    st.markdown("### Filtros")
    selected_status = st.selectbox("Estado", ['Todos', 'En movimiento', 'Fondeados', 'En puerto'])
    speed_threshold = st.slider("Velocidad mínima (nudos)", 0.0, 30.0, 0.0, 0.5)
    show_geofence = st.checkbox("Mostrar zona puerto", value=True)
    
    st.markdown("---")
    st.markdown("### ⚠️ Sistema de Alertas")
    show_alerts = st.checkbox("Activar sistema de alertas", value=True)
    if show_alerts:
        speed_threshold_alert = st.slider("Umbral velocidad alta (kn)", 5.0, 20.0, 10.0, 0.5)
        distance_threshold = st.slider("Radio de proximidad (km)", 1.0, 10.0, 3.0, 0.5)
    
    st.markdown("---")
    st.markdown("### Leyenda")
    st.markdown('<span class="status-indicator status-moving"></span> En movimiento', unsafe_allow_html=True)
    st.markdown('<span class="status-indicator status-stopped"></span> Fondeado/Parado', unsafe_allow_html=True)
    st.markdown('<span class="status-indicator status-port"></span> En puerto', unsafe_allow_html=True)

st.title("Atlantic-Ops: Maritime Intelligence")
st.markdown("Monitorización en tiempo real del tráfico marítimo • Puerto de La Luz, Gran Canaria")
st.markdown("---")

df = load_realtime_data()

if not df.empty:
    if selected_status != 'Todos':
        status_map = {'En movimiento': 'moving', 'Fondeados': 'stopped', 'En puerto': 'port'}
        filtered_df = df[df['status_category'] == status_map[selected_status]]
    else:
        filtered_df = df
    
    filtered_df = filtered_df[filtered_df['speed'] >= speed_threshold]
    
    alerts = generate_alerts(filtered_df, speed_threshold_alert, distance_threshold) if show_alerts else []
    alert_count, high_alert_count = len(alerts), len([a for a in alerts if a['priority'] == 'high'])
    
    total_ships, moving_ships, port_ships, avg_speed = len(df), len(df[df['status_category'] == 'moving']), len(df[df['status_category'] == 'port']), df['speed'].mean()
    
    # KPIs SUPERIORES
    cols = st.columns(5 if show_alerts and alert_count > 0 else 4)
    with cols[0]: st.markdown(f'<div class="metric-container"><div class="metric-value">{total_ships}</div><div class="metric-label">Buques totales</div><div class="metric-trend" style="color: #10B981;">Online 🟢</div></div>', unsafe_allow_html=True)
    with cols[1]: st.markdown(f'<div class="metric-container"><div class="metric-value">{moving_ships}</div><div class="metric-label">En movimiento</div></div>', unsafe_allow_html=True)
    with cols[2]: st.markdown(f'<div class="metric-container"><div class="metric-value">{port_ships}</div><div class="metric-label">En puerto</div></div>', unsafe_allow_html=True)
    with cols[3]: st.markdown(f'<div class="metric-container"><div class="metric-value">{avg_speed:.1f}</div><div class="metric-label">Velocidad promedio</div><div class="metric-trend">Máx: {df["speed"].max():.1f} kn</div></div>', unsafe_allow_html=True)
    if show_alerts and alert_count > 0:
        alert_color = "#EF4444" if high_alert_count > 0 else "#F59E0B"
        with cols[4]: st.markdown(f'<div class="metric-container"><div class="metric-value" style="color: {alert_color};">{alert_count}</div><div class="metric-label">Alertas activas</div><div class="metric-trend" style="color: {alert_color};">{high_alert_count} críticas</div></div>', unsafe_allow_html=True)

    st.markdown("### 🗺️ Vista operativa")
    st.pydeck_chart(create_simple_map(filtered_df, show_geofence, alerts), height=500)
    
    st.markdown("### 📊 Análisis y Monitoreo")
    tab1, tab2, tab3 = st.tabs(["📈 Estadísticas", "⚠️ Sistema de Alertas", "📋 Detalle de Buques"])
    
    with tab1:
        c1, c2 = st.columns(2)
        
        with c1: 
            speed_fig = create_speed_chart(filtered_df)
            if speed_fig:
                st.plotly_chart(speed_fig, use_container_width=True)
                
        with c2: 
            status_fig = create_status_chart(filtered_df)
            if status_fig:
                st.plotly_chart(status_fig, use_container_width=True)
    
    with tab2:
        if show_alerts:
            if alerts:
                st.markdown(f"### 🔍 {len(alerts)} Alertas Detectadas")
                for alert in alerts:
                    st.markdown(f"""
                    <div class="alert-container alert-{alert['priority']}">
                        <div class="alert-header">
                            <div class="alert-title">{alert['type']}</div>
                            <div class="alert-priority priority-{alert['priority']}">{alert['priority'].upper()}</div>
                        </div>
                        <div class="alert-message">{alert['message']}</div>
                        <div class="alert-footer">
                            <span>🚢 {alert['ship']}</span> <span>🕐 {alert['time']}</span> <span>💨 {alert['speed']:.1f} kn</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.success("✅ No se detectaron alertas críticas.")
        else:
            st.info("Sistema de alertas desactivado.")
    
    with tab3:
        st.dataframe(
            filtered_df[['ship_name', 'speed', 'heading', 'status_category', 'time_display']].sort_values('speed', ascending=False),
            column_config={"ship_name": "Buque", "speed": st.column_config.NumberColumn("Knots", format="%.1f"), "heading": st.column_config.NumberColumn("Rumbo", format="%.0f°"), "status_category": "Estado", "time_display": "Última Info"},
            use_container_width=True, hide_index=True
        )

else:
    st.info("📡 Escaneando señales AIS en el Data Lake... (Esperando a que Spark escriba el primer lote)")
    st.pydeck_chart(pdk.Deck(map_style=pdk.map_styles.DARK, initial_view_state=pdk.ViewState(latitude=28.14, longitude=-15.42, zoom=11)), height=500)

st.markdown("---")
st.markdown(f"**Última actualización:** {datetime.now().strftime('%H:%M:%S')} | **Estado Motor:** 🟢 Spark Streaming OK")

time.sleep(refresh_rate)
st.rerun()