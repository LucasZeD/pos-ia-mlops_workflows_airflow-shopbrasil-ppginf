"""
Configurações centralizadas do pipeline.

Concentram-se neste módulo as constantes de ambiente, de modo que as
equipes não precisem localizar valores dispersos pela DAG. Alterações como as
mudanças da URL da API ocorrem, portanto, em um único ponto.
"""

# --- Fontes de dados ----------------------------------------------------------
API_BASE_URL = "https://fakestoreapi.com"
API_TIMEOUT_SEGUNDOS = 30
# Diversas APIs públicas bloqueiam requisições sem User-Agent; por isso, definem-se cabeçalhos explícitos.
API_HEADERS = {"User-Agent": "shopbrasil-airflow-pipeline/1.0", "Accept": "application/json"}

# --- Airflow ------------------------------------------------------------------
TIMEZONE = "America/Sao_Paulo"
POSTGRES_CONN_ID = "postgres_dw"   # os valores provêm da variável AIRFLOW_CONN_POSTGRES_DW (docker-compose)
POOL_METRICAS = "ecommerce_pool"   # os comandos do airflow-init criam este pool com 2 slots

# --- Tabelas de destino -------------------------------------------------------
TABELA_SNAPSHOT = "metricas_categoria_snapshot"
TABELA_HISTORICO = "metricas_categoria_historico"
