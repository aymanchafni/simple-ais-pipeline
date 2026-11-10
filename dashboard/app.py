import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import os
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="AIS Maritime Traffic Dashboard",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration API - Détection automatique de l'environnement
def get_api_url():
    """Détermine l'URL de l'API selon l'environnement"""
    if os.getenv("API_URL"):
        return os.environ["API_URL"]
    if os.path.exists('/.dockerenv'):
        return "http://app:8000"
    else:
        return "http://localhost:8000"

API_BASE_URL = get_api_url()

# Styles CSS personnalisés
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e6e9ef;
    }
    .stAlert > div {
        padding-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Header principal
st.title("🚢 Dashboard Trafic Maritime AIS")
st.markdown("**Pipeline de données AIS (batch quotidien, idempotent) pour l'analyse du trafic maritime**")
st.markdown("---")

# Sidebar avec navigation et statut
st.sidebar.header("🧭 Navigation")
st.sidebar.info(f"🔗 API: {API_BASE_URL}")

# Fonction utilitaire pour les requêtes API avec retry intelligent
@st.cache_data(ttl=30, show_spinner=False)
def fetch_api_data(endpoint: str, params: dict = None):
    """Récupère les données depuis l'API avec gestion d'erreurs robuste"""
    urls_to_try = [
        f"{API_BASE_URL}{endpoint}",
        f"http://app:8000{endpoint}",
        f"http://localhost:8000{endpoint}"
    ]
    
    for url in urls_to_try:
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json(), None
            elif response.status_code == 404:
                return None, "Données non trouvées"
            else:
                continue
        except requests.exceptions.ConnectionError:
            continue
        except requests.exceptions.Timeout:
            continue
        except Exception as e:
            continue
    
    return None, "Impossible de se connecter à l'API"

# Test de connexion API avec statut dans la sidebar
def check_api_status():
    """Vérifie et affiche le statut de l'API"""
    with st.sidebar:
        with st.spinner("Test connexion..."):
            health_data, error = fetch_api_data("/health")
            
            if error or not health_data:
                st.error("❌ API Déconnectée")
                with st.expander("Détails de l'erreur"):
                    st.text(f"Erreur: {error}")
                    st.text(f"URL: {API_BASE_URL}")
                return False
            else:
                st.success("✅ API Connectée")
                st.caption(f"Statut: {health_data.get('status', 'unknown')}")
                return True

# Vérification initiale de l'API
api_available = check_api_status()

if not api_available:
    st.error("⚠️ **Dashboard en mode dégradé - API non disponible**")
    st.markdown("""
    ### Solutions possibles :
    1. **Vérifiez les containers** : `docker-compose ps`
    2. **Redémarrez l'API** : `docker-compose restart app`
    3. **Vérifiez les logs** : `docker-compose logs app`
    4. **Attendez quelques secondes** et actualisez la page
    """)
    
    if st.button("🔄 Réessayer la connexion", type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    st.stop()

# Navigation par onglets
tab1, tab2, tab3, tab4 = st.tabs(["📊 Vue d'ensemble", "🚢 Navires", "📈 Métriques", "🔍 Recherche"])

# TAB 1: Vue d'ensemble
with tab1:
    st.header("📊 Vue d'Ensemble du Trafic Maritime")
    
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Actualiser", key="refresh_overview"):
            st.cache_data.clear()
            st.rerun()
    
    # Chargement des statistiques principales
    with st.spinner("Chargement des statistiques..."):
        stats_data, stats_error = fetch_api_data("/statistics")
    
    if stats_error:
        st.error(f"❌ Erreur lors du chargement : {stats_error}")
    else:
        global_stats = stats_data.get('global_statistics', {})
        
        # Métriques principales en 4 colonnes
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="🚢 Navires Total",
                value=int(global_stats.get('total_vessels', 0))
            )
        
        with col2:
            total_pos = int(global_stats.get('total_positions', 0))
            st.metric(
                label="📍 Positions Enregistrées",
                value=f"{total_pos:,}"
            )
        
        with col3:
            avg_distance = float(global_stats.get('avg_distance') or 0)
            st.metric(
                label="📏 Distance Moyenne",
                value=f"{avg_distance:.1f} nm"
            )
        
        with col4:
            max_speed = float(global_stats.get('max_speed_recorded') or 0)
            st.metric(
                label="⚡ Vitesse Max. Observée",
                value=f"{max_speed:.1f} nœuds"
            )
        
        st.markdown("---")
        
        # Section Top 5 Navires
        st.subheader("🏆 Top 5 Navires par Distance Parcourue")
        
        top_vessels = stats_data.get('top_vessels_by_distance', [])
        
        if top_vessels:
            df_top = pd.DataFrame(top_vessels)
            
            # Graphique en barres horizontal
            fig = px.bar(
                df_top, 
                x='total_distance_nm', 
                y='vessel_name',
                orientation='h',
                title="Distance Parcourue (milles nautiques)",
                labels={
                    'vessel_name': 'Navire',
                    'total_distance_nm': 'Distance (nm)'
                },
                color='total_distance_nm',
                color_continuous_scale='viridis',
                height=400
            )
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
            
            # Tableau détaillé avec colonnes configurées
            st.subheader("📋 Détails des Top Navires")
            st.dataframe(
                df_top,
                column_config={
                    'mmsi': st.column_config.NumberColumn("MMSI", format="%d"),
                    'vessel_name': "Nom du Navire",
                    'total_distance_nm': st.column_config.NumberColumn(
                        "Distance (nm)", 
                        format="%.1f"
                    ),
                    'avg_speed_knots': st.column_config.NumberColumn(
                        "Vitesse Moy. (nœuds)", 
                        format="%.1f"
                    ),
                    'point_count': st.column_config.NumberColumn("Points GPS", format="%d")
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("📭 Aucune donnée de navire disponible")

# TAB 2: Liste des navires
with tab2:
    st.header("🚢 Gestion des Navires")
    
    # Contrôles en deux colonnes
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Pagination
        col_limit, col_offset = st.columns(2)
        with col_limit:
            limit = st.selectbox("Navires par page", [25, 50, 100, 200], index=1)
        with col_offset:
            offset = st.number_input("Décalage", min_value=0, value=0, step=limit)
    
    with col2:
        if st.button("🔄 Actualiser", key="refresh_vessels"):
            st.cache_data.clear()
            st.rerun()
    
    # Chargement des navires avec pagination
    with st.spinner("Chargement de la liste des navires..."):
        vessels_params = {"limit": limit, "offset": offset}
        vessels_data, vessels_error = fetch_api_data("/vessels", vessels_params)
    
    if vessels_error:
        st.error(f"❌ Erreur : {vessels_error}")
    else:
        vessels = vessels_data.get('vessels', [])
        count = vessels_data.get('count', 0)
        
        if vessels:
            st.success(f"📊 {count} navire(s) chargé(s) (page {offset//limit + 1})")
            
            # Conversion en DataFrame
            df_vessels = pd.DataFrame(vessels)
            
            # Affichage du tableau avec configuration avancée
            st.dataframe(
                df_vessels,
                column_config={
                    'mmsi': st.column_config.NumberColumn("MMSI", format="%d"),
                    'vessel_name': "Nom du Navire",
                    'total_distance_nm': st.column_config.NumberColumn(
                        "Distance Totale (nm)", 
                        format="%.1f"
                    ),
                    'total_time_hours': st.column_config.NumberColumn(
                        "Temps Total (h)", 
                        format="%.1f"
                    ),
                    'moving_time_hours': st.column_config.NumberColumn(
                        "Temps en Mouvement (h)", 
                        format="%.1f"
                    ),
                    'at_dock_time_hours': st.column_config.NumberColumn(
                        "Temps à Quai (h)", 
                        format="%.1f"
                    ),
                    'point_count': st.column_config.NumberColumn("Points GPS", format="%d"),
                    'avg_speed_knots': st.column_config.NumberColumn(
                        "Vitesse Moy. (nœuds)", 
                        format="%.1f"
                    ),
                    'max_speed_knots': st.column_config.NumberColumn(
                        "Vitesse Max. (nœuds)", 
                        format="%.1f"
                    )
                },
                use_container_width=True,
                hide_index=True
            )
            
            # Graphiques de distribution
            if len(vessels) > 1:
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_dist = px.histogram(
                        df_vessels, 
                        x='total_distance_nm',
                        nbins=20,
                        title="Distribution des Distances",
                        labels={'total_distance_nm': 'Distance (nm)', 'count': 'Nombre de navires'}
                    )
                    st.plotly_chart(fig_dist, use_container_width=True)
                
                with col2:
                    fig_speed = px.histogram(
                        df_vessels, 
                        x='avg_speed_knots',
                        nbins=20,
                        title="Distribution des Vitesses Moyennes",
                        labels={'avg_speed_knots': 'Vitesse (nœuds)', 'count': 'Nombre de navires'}
                    )
                    st.plotly_chart(fig_speed, use_container_width=True)
        else:
            st.info("📭 Aucun navire trouvé")

# TAB 3: Métriques avancées
with tab3:
    st.header("📈 Métriques Avancées")
    
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Actualiser", key="refresh_metrics"):
            st.cache_data.clear()
            st.rerun()
    
    # Chargement des métriques temporelles et de qualité
    with st.spinner("Chargement des métriques avancées..."):
        time_data, time_error = fetch_api_data("/metrics/time-analysis")
        quality_data, quality_error = fetch_api_data("/metrics/quality")
    
    if time_error or quality_error:
        st.error("❌ Erreur lors du chargement des métriques")
    else:
        # Métriques temporelles
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("⏱️ Analyse Temporelle")
            
            time_analysis = time_data.get('time_analysis', {})
            
            # Données pour le graphique en secteurs
            total_moving = time_analysis.get('total_moving_time', 0)
            total_dock = time_analysis.get('total_dock_time', 0)
            
            if total_moving > 0 or total_dock > 0:
                time_chart_data = pd.DataFrame({
                    'Statut': ['En Mouvement', 'À Quai'],
                    'Temps (heures)': [total_moving, total_dock]
                })
                
                fig_pie = px.pie(
                    time_chart_data, 
                    values='Temps (heures)', 
                    names='Statut',
                    title="Répartition du Temps Total de la Flotte",
                    color_discrete_sequence=['#1f77b4', '#ff7f0e']
                )
                st.plotly_chart(fig_pie, use_container_width=True)
                
                # Métriques détaillées
                st.metric("Temps moyen en mouvement", f"{time_analysis.get('avg_moving_time_per_vessel', 0):.1f}h")
                st.metric("Temps moyen à quai", f"{time_analysis.get('avg_dock_time_per_vessel', 0):.1f}h")
            else:
                st.info("Pas de données temporelles disponibles")
        
        with col2:
            st.subheader("✅ Qualité des Données")
            
            data_quality = quality_data.get('data_quality', {})
            
            # Métriques de qualité
            quality_metrics = [
                ('Vitesses Renseignées', data_quality.get('valid_speeds_percentage', 0)),
                ('Caps Renseignés', data_quality.get('valid_headings_percentage', 0)),
                ('Types Renseignés', data_quality.get('valid_types_percentage', 0)),
                ('Noms Renseignés', data_quality.get('valid_names_percentage', 0))
            ]