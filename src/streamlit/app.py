import streamlit as st
import duckdb
import pandas as pd
import pydeck as pdk
import plotly.express as px
import time
from datetime import datetime

# ==========================================
# 1. CONFIGURACIÓN (Debe ser lo primero)
# ==========================================
st.set_page_config(
    page_title="Atlantic-Ops | Command Center",
    layout="wide",
    page_icon="⚓",
    initial_sidebar_state="expanded"
)

# Estilos CSS para modo oscuro profesional
st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    
    /* Tarjetas de Métricas */
    div[data-testid="stMetric"] {
        background-color: #161B22;
        border: 1px solid #30363d;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    
    /* Títulos */
    h1, h2, h3 { color: #58a6ff; font-family: 'Segoe UI', sans-serif; }
    
    /* Arreglo para mapa negro en algunos navegadores */
    .deckgl-overlay { mix-blend-mode: normal !important; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. GESTIÓN DE DATOS (Backend)
# ==========================================
class DataManager:
    @staticmethod
    @st.cache_resource
    def get_connection():
        """Conexión persistente a DuckDB"""
        try:
            con = duckdb.connect(database=':memory:')
            con.execute("""
                INSTALL httpfs; LOAD httpfs;
                SET s3_region='us-east-1';
                SET s3_endpoint='localhost:9000';
                SET s3_access_key_id='atlantic_admin';
                SET s3_secret_access_key='atlantic_password';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
                SET http_keep_alive=false;
            """)
            return con
        except Exception as e:
            return None

    @staticmethod
    def fetch_data(debug_mode=False):
        con = DataManager.get_connection()
        if not con:
            return pd.DataFrame(), "Error de Conexión DB"

        try:
            # 1. Debug: Comprobar ficheros (opcional)
            if debug_mode:
                files = con.execute("SELECT * FROM glob('s3://lakehouse/vessel_data_v2/*.parquet')").fetchall()
                if not files:
                    return pd.DataFrame(), "No se encuentran archivos .parquet en S3"

            # 2. Query Principal (Window Function para coger solo la última posición)
            query = """
            SELECT 
                ship_name, lat, lon, speed, heading, in_port, timestamp,
                CASE 
                    WHEN in_port = True THEN 'En Puerto'
                    WHEN speed < 1.0 THEN 'Fondeado'
                    ELSE 'Navegando'
                END as status
            FROM read_parquet('s3://lakehouse/vessel_data_v2/*.parquet')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ship_name ORDER BY timestamp DESC) = 1
            """
            df = con.execute(query).df()
            
            # Post-procesamiento de Colores para el Mapa (RGB)
            if not df.empty:
                # Verde (Navegando), Rojo (Fondeado), Azul (Puerto)
                df['color_r'] = df.apply(lambda x: 0 if x['speed'] > 1 else (50 if x['in_port'] else 255), axis=1)
                df['color_g'] = df.apply(lambda x: 255 if x['speed'] > 1 else (50 if x['in_port'] else 50), axis=1)
                df['color_b'] = df.apply(lambda x: 50 if x['speed'] > 1 else (255 if x['in_port'] else 50), axis=1)
            
            return df, "OK"
            
        except Exception as e:
            return pd.DataFrame(), str(e)

# ==========================================
# 3. INTERFAZ GRÁFICA (UI)
# ==========================================
def render_dashboard(df, show_geofence):
    # --- FILA 1: KPIs ---
    k1, k2, k3, k4 = st.columns(4)
    
    if df.empty:
        k1.metric("🚢 Flota Total", "0", delta="Offline")
        k2.metric("💨 En Movimiento", "0")
        k3.metric("⚓ En Puerto", "0")
        k4.metric("🚀 Vel. Máxima", "0 kn")
    else:
        total = len(df)
        moving = len(df[df['status']=='Navegando'])
        port = len(df[df['in_port']==True])
        max_spd = df['speed'].max()
        
        k1.metric("🚢 Flota Total", total, delta="Online")
        k2.metric("💨 En Movimiento", moving, delta=f"{int(moving/total*100)}%")
        k3.metric("⚓ En Puerto", port)
        k4.metric("🚀 Vel. Máxima", f"{max_spd:.1f} kn")

    # --- FILA 2: MAPA ---
    st.markdown("### 🗺️ Situación Táctica")
    
    layers = []
    
    # Capa Geofence (Puerto)
    if show_geofence:
        PORT_POLYGON = [[-15.45, 28.10], [-15.39, 28.10], [-15.39, 28.18], [-15.45, 28.18]]
        layers.append(pdk.Layer(
            "PolygonLayer",
            data=[{"polygon": PORT_POLYGON}],
            get_polygon="polygon",
            filled=True, stroked=True, wireframe=True,
            get_fill_color=[0, 100, 255, 20],
            get_line_color=[0, 100, 255, 100],
            get_line_width=30,
        ))

    # Capa Barcos
    if not df.empty:
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position='[lon, lat]',
            get_fill_color='[color_r, color_g, color_b]',
            get_line_color=[255, 255, 255],
            get_line_width=10,
            stroked=True,
            get_radius=300,
            pickable=True,
            auto_highlight=True
        ))

    # Vista Inicial (Gran Canaria)
    view_state = pdk.ViewState(
        latitude=28.14, 
        longitude=-15.42, 
        zoom=11, 
        pitch=45
    )

    st.pydeck_chart(pdk.Deck(
        map_style=pdk.map_styles.CARTO_DARK,
        initial_view_state=view_state,
        layers=layers,
        tooltip={"html": "<b>{ship_name}</b><br/>Vel: {speed} kn<br/>Estado: {status}"}
    ))

    # --- FILA 3: ANALÍTICA ---
    if not df.empty:
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("### 📊 Perfil de Velocidad")
            fig = px.histogram(
                df, x="speed", nbins=15, 
                title="",
                color_discrete_sequence=['#00f2ff'], # Cyan Cyberpunk
                template="plotly_dark"
            )
            fig.update_layout(bargap=0.2, margin=dict(l=20, r=20, t=20, b=20), height=250)
            st.plotly_chart(fig, use_container_width=True)
            
        with c2:
            st.markdown("### 📋 Datos en Tiempo Real")
            st.dataframe(
                df[['ship_name', 'speed', 'status', 'timestamp']].sort_values('timestamp', ascending=False),
                height=250,
                hide_index=True,
                column_config={
                    "ship_name": "Nombre",
                    "speed": st.column_config.NumberColumn("Knots", format="%.1f ⚓"),
                    "timestamp": st.column_config.DatetimeColumn("Última Señal", format="HH:mm:ss")
                }
            )

# ==========================================
# 4. BUCLE PRINCIPAL (Main Loop)
# ==========================================
def main():
    st.title("⚓ Atlantic-Ops Monitor")
    
    # Sidebar (Estática)
    with st.sidebar:
        st.header("Control de Misión")
        refresh = st.slider("Refresco (s)", 2, 10, 5)
        show_geo = st.checkbox("Ver Geofence", True)
        debug = st.checkbox("Modo Depuración", False)
        st.divider()
        status_text = st.empty()

    # CONTENEDOR PRINCIPAL (Crucial para evitar duplicados)
    dashboard_placeholder = st.empty()

    while True:
        # 1. Cargar Datos
        df, msg = DataManager.fetch_data(debug)
        
        # 2. Status Sidebar
        if msg == "OK":
            status_text.success(f"🟢 Online: {datetime.now().strftime('%H:%M:%S')}")
        else:
            status_text.error(f"🔴 Estado: {msg}")

        # 3. Renderizar Dashboard (Limpia lo anterior)
        with dashboard_placeholder.container():
            if msg != "OK" and debug:
                st.warning(f"Diagnóstico: {msg}")
                st.info("💡 Consejo: Si dice 'No se encuentran archivos', es que Spark no ha escrito todavía. Espera 1 minuto.")
            
            render_dashboard(df, show_geo)

        time.sleep(refresh)

if __name__ == "__main__":
    main()
