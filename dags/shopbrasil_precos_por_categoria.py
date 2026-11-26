"""
# ShopBrasil - Panorama de preços por categoria

Substituem-se, por meio desta DAG, os antigos scripts Python agendados no cron.

**Ações executadas:** as tasks buscam os produtos da API de catálogo (FakeStore API),
calculam, por categoria, as quantidades de produtos e os preços médio, mínimo e máximo,
e gravam os resultados no PostgreSQL analítico que alimenta o painel das equipes de pricing.

**Periodicidade:** as execuções ocorrem diariamente às 06:00 (America/Sao_Paulo).

**Fluxo (topologias):**

```
[ingestao]      buscar_produtos -> validar_produtos -> agrupar_por_categoria     (LINEAR)
[analise]       calcular_metricas x N categorias  (.expand)                      (FAN-OUT)
                        -> consolidar_metricas                                   (FAN-IN)
[persistencia]  criar_tabelas -> salvar_snapshot  (upsert, idempotente)
                              -> salvar_historico (append)
```
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from common.alertas import alerta_falha, alerta_retry, alerta_sla, alerta_sucesso
from common.config import (
    API_BASE_URL,
    API_HEADERS,
    API_TIMEOUT_SEGUNDOS,
    POOL_METRICAS,
    POSTGRES_CONN_ID,
    TABELA_HISTORICO,
    TABELA_SNAPSHOT,
    TIMEZONE,
)
from operators.validar_produtos import ValidarProdutosOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONCEITO - Justificativas para a adoção do Airflow em substituição ao cron
# Os agendadores cron apenas executam comandos em horários fixos (ex.: 06:00).
# Eles não realizam novas tentativas, não emitem alertas em caso de falha, não
# exibem os históricos das execuções e não reconhecem dependências entre etapas.
# No Airflow, modelam-se os pipelines como DAGs (Directed Acyclic Graphs):
# um grafo de tasks com dependências e sem ciclos. Os schedulers determinam
# quais tasks executar e em que momento, e as execuções (DagRuns) permanecem
# registradas, podendo ser reprocessadas pela interface.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Funções auxiliares (não constituem tasks): datas de referência das execuções
# ---------------------------------------------------------------------------
def _data_referencia(context) -> str:
    """
    Retornam-se as datas "de negócio" das execuções, no fuso horário de Brasília.

    CONCEITO - data_interval: no Airflow, as runs cobrem "intervalos de dados"
    (data_interval_start -> data_interval_end). Com agendamentos diários às 06:00,
    as runs do dia D cobrem o período de D-1 06:00 a D 06:00 e iniciam somente
    ao FIM do intervalo. Por esse motivo, adotam-se os valores de data_interval_end
    como as datas adequadas ao painel.

    No Airflow 2, as runs agendadas para 06:00 do dia D apresentam
    data_interval_end = D 06:00. Os usos dessas datas (e não do horário corrente)
    tornam os reprocessamentos idempotentes: as reexecuções de uma mesma run
    geram sempre a mesma data_referencia, de modo que os upserts recaem sobre
    as mesmas linhas.
    """
    # Retornam-se strings no formato 'YYYY-MM-DD'; o PostgreSQL as converte automaticamente para DATE.
    return context["data_interval_end"].in_timezone(TIMEZONE).to_date_string()


# ---------------------------------------------------------------------------
# Argumentos padrão: aplicam-se a TODAS as tasks, exceto quando estas os sobrescrevem
# ---------------------------------------------------------------------------
default_args = {
    "owner": "time-dados",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # As tasks que falham definitivamente disparam os alertas simulados.
    "on_failure_callback": alerta_falha,
}


@dag(
    dag_id="shopbrasil_precos_por_categoria",
    description="Coleta produtos da API de catálogo e grava métricas de preço por categoria",
    # As expressões cron seguem os campos: minuto hora dia-do-mês mês dia-da-semana.
    # "0 6 * * *" = minuto 0, hora 6, todos os dias => execuções diárias às 06:00.
    schedule="0 6 * * *",
    # CONCEITO - timezone: o Airflow armazena internamente todos os horários em UTC.
    # Caso as start_dates não possuíssem fuso, os valores "6" corresponderiam a
    # 06:00 UTC (03:00 em Brasília). Com as datas criadas via pendulum e
    # tz=America/Sao_Paulo, fixam-se as 06:00 no horário de Brasília
    # (acompanhando eventuais mudanças de horário de verão).
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    # CONCEITO - catchup: com o valor True, as ativações da DAG levariam o Airflow
    # a criar runs para todos os dias desde a start_date ("backfill" automático).
    # Como interessam apenas os dados do dia corrente, adotam-se valores False:
    # executam-se somente os intervalos mais recentes.
    catchup=False,
    max_active_runs=1,      # evitam-se execuções concorrentes sobre as mesmas tabelas
    default_args=default_args,
    sla_miss_callback=alerta_sla,
    tags=["shopbrasil", "pricing", "fakestore"],
    doc_md=__doc__,
)
def shopbrasil_precos_por_categoria():

    # =======================================================================
    # GRUPO 1 - INGESTÃO  (topologia LINEAR)
    # =======================================================================
    # CONCEITO - TaskGroup: os TaskGroups organizam apenas as VISUALIZAÇÕES na
    # interface (as tasks aparecem agrupadas e os ids recebem o prefixo
    # "ingestao."). Eles não alteram a forma nem o momento de execução das tasks.
    @task_group(group_id="ingestao", tooltip="Coleta, validação e agrupamento dos produtos")
    def ingestao():

        @task(
            task_id="buscar_produtos",
            # --- Resiliência: como as APIs oscilam durante a madrugada, configuram-se novas tentativas espaçadas.
            retries=5,
            retry_delay=timedelta(seconds=30),   # as primeiras esperas duram ~30s
            # CONCEITO - exponential backoff: as novas tentativas aguardam o dobro
            # do intervalo anterior (~30s, ~1min, ~2min, ~4min...). Diante de APIs
            # instáveis, as insistências rápidas agravam os problemas; os intervalos
            # crescentes concedem tempo para as recuperações dos serviços.
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=10),
            # --- Callbacks de ciclo de vida (tasks críticas do pipeline)
            on_failure_callback=alerta_falha,
            on_retry_callback=alerta_retry,
            on_success_callback=alerta_sucesso,
            # --- SLA: caso as tasks não terminem em 30min após os horários agendados, emitem-se alertas.
            sla=timedelta(minutes=30),
        )
        def buscar_produtos() -> list[dict]:
            """Buscam-se todos os produtos da API de catálogo."""
            url = f"{API_BASE_URL}/products"
            try:
                log.info("Buscando produtos em %s", url)
                resposta = requests.get(url, headers=API_HEADERS, timeout=API_TIMEOUT_SEGUNDOS)
                resposta.raise_for_status()  # as respostas 4xx/5xx convertem-se em exceções
                produtos = resposta.json()

                if not isinstance(produtos, list) or not produtos:
                    # As respostas "200 vazias" também indicam problemas; por isso, provocam-se novas tentativas.
                    raise ValueError("API retornou uma lista vazia ou em formato inesperado")

            except (requests.RequestException, ValueError) as erro:
                # Registram-se os erros com contexto e RELANÇAM-SE as exceções: são os
                # raises que levam o Airflow a marcar as tentativas como falhas e a
                # agendar os retries.
                log.error("Falha ao buscar produtos na API: %s", erro)
                raise

            log.info("%d produtos recebidos da API", len(produtos))

            # CONCEITO - XCom: os XComs constituem o mecanismo de troca de dados
            # entre as tasks. No TaskFlow, os "returns" das funções convertem-se
            # automaticamente em XComs, e as tasks que recebem esses valores como
            # parâmetros passam a depender desta task.
            # Os XComs permanecem salvos no banco de METADADOS do Airflow e, por
            # isso, devem transportar apenas dados pequenos: mantêm-se
            # somente os campos utilizados (descrições longas e URLs de imagem
            # ficam de fora).
            return [
                {
                    "id": p.get("id"),
                    "title": p.get("title"),
                    "price": p.get("price"),
                    "category": p.get("category"),
                }
                for p in produtos
            ]

        @task(task_id="agrupar_por_categoria")
        def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
            """
            Montam-se itens por categoria, contendo as respectivas listas de preços.

            As categorias derivam dos PRÓPRIOS DADOS: caso surjam novas categorias
            na API, elas aparecem nesta etapa e recebem automaticamente suas tasks
            mapeadas, sem que os desenvolvedores editem o código.
            """
            precos_por_categoria: dict[str, list[float]] = defaultdict(list)
            for produto in produtos:
                precos_por_categoria[produto["category"]].append(float(produto["price"]))

            grupos = [
                {"categoria": categoria, "precos": precos}
                for categoria, precos in sorted(precos_por_categoria.items())
            ]
            log.info("Categorias encontradas (%d): %s", len(grupos), [g["categoria"] for g in grupos])
            return grupos

        # Encadeamento LINEAR: as dependências decorrem das próprias chamadas das funções.
        produtos_brutos = buscar_produtos()

        produtos_validos = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=produtos_brutos,  # os XComArgs resolvem-se em tempo de execução
        )

        return agrupar_por_categoria(produtos_validos.output)

    # =======================================================================
    # GRUPO 2 - ANÁLISE  (topologias FAN-OUT e FAN-IN)
    # =======================================================================
    @task_group(group_id="analise", tooltip="Métricas por categoria em paralelo + consolidação")
    def analise(grupos):

        @task(
            task_id="calcular_metricas",
            pool=POOL_METRICAS,  # limitam-se a 2 as categorias processadas simultaneamente
            pool_slots=1,
        )
        def calcular_metricas(grupo: dict) -> dict:
            """Calculam-se as métricas de UMA categoria (as instâncias mapeadas correspondem às categorias)."""
            precos = grupo["precos"]
            metricas = {
                "categoria": grupo["categoria"],
                "qtd_produtos": len(precos),
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_min": round(min(precos), 2),
                "preco_max": round(max(precos), 2),
            }
            log.info("Métricas calculadas: %s", metricas)
            return metricas

        @task(task_id="consolidar_metricas")
        def consolidar_metricas(metricas) -> list[dict]:
            """FAN-IN: reúnem-se os resultados de todas as tasks mapeadas em uma única lista."""
            consolidado = sorted(metricas, key=lambda m: m["categoria"])

            # Registram-se no log resumos legíveis, semelhantes aos exibidos pelo painel.
            log.info("%-25s %5s %10s %10s %10s", "categoria", "qtd", "medio", "min", "max")
            for m in consolidado:
                log.info(
                    "%-25s %5d %10.2f %10.2f %10.2f",
                    m["categoria"], m["qtd_produtos"], m["preco_medio"], m["preco_min"], m["preco_max"],
                )
            return consolidado

        # CONCEITO - Dynamic Task Mapping: em vez de tasks escritas por categoria,
        # utilizam-se os .expand(), que criam em TEMPO DE EXECUÇÃO cópias da
        # task para os itens da lista recebida. Listas com 4 categorias geram 4
        # tasks; listas com 5 geram 5. Caracteriza-se, assim, o FAN-OUT.
        # Os pools (2 slots) limitam a 2 as cópias executadas simultaneamente.
        metricas_por_categoria = calcular_metricas.expand(grupo=grupos)

        # CONCEITO - FAN-IN: realizam-se os caminhos inversos. As tasks únicas
        # recebem as listas com os retornos de TODAS as cópias mapeadas e
        # executam somente após as conclusões de todas elas.
        return consolidar_metricas(metricas_por_categoria)

    # =======================================================================
    # GRUPO 3 - PERSISTÊNCIA  (PostgreSQL via PostgresHook + Connection)
    # =======================================================================
    @task_group(group_id="persistencia", tooltip="Gravação no PostgreSQL analítico")
    def persistencia(metricas):

        @task(task_id="criar_tabelas")
        def criar_tabelas() -> None:
            """Garantem-se as existências das tabelas (DDLs idempotentes com IF NOT EXISTS)."""
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            hook.run(f"""
                CREATE TABLE IF NOT EXISTS {TABELA_SNAPSHOT} (
                    data_referencia DATE          NOT NULL,
                    categoria       VARCHAR(100)  NOT NULL,
                    qtd_produtos    INTEGER       NOT NULL,
                    preco_medio     NUMERIC(12,2) NOT NULL,
                    preco_min       NUMERIC(12,2) NOT NULL,
                    preco_max       NUMERIC(12,2) NOT NULL,
                    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (data_referencia, categoria)
                );
                CREATE TABLE IF NOT EXISTS {TABELA_HISTORICO} (
                    id              BIGSERIAL     PRIMARY KEY,
                    data_referencia DATE          NOT NULL,
                    run_id          VARCHAR(250)  NOT NULL,
                    categoria       VARCHAR(100)  NOT NULL,
                    qtd_produtos    INTEGER       NOT NULL,
                    preco_medio     NUMERIC(12,2) NOT NULL,
                    preco_min       NUMERIC(12,2) NOT NULL,
                    preco_max       NUMERIC(12,2) NOT NULL,
                    coletado_em     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
                );
            """)

        @task(task_id="salvar_snapshot")
        def salvar_snapshot(metricas: list[dict], **context) -> int:
            """
            Gravam-se os snapshots diários de forma IDEMPOTENTE.

            CONCEITO - idempotência: as operações são idempotentes quando suas
            execuções, 1 ou 10 vezes, deixam os bancos no mesmo estado. Os scripts
            do cron careciam dessa propriedade (as execuções manuais duplicavam os
            números). Neste pipeline, obtêm-se as idempotências por meio da chave primária
            (data_referencia, categoria) combinada ao upsert descrito abaixo.

            - INSERT ... ON CONFLICT (data_referencia, categoria) DO UPDATE:
              atualizam-se as linhas existentes (reprocessamentos) e inserem-se as demais.
            - DELETE das categorias do dia ausentes na execução corrente:
              os snapshots refletem, assim, exatamente as últimas execuções do dia.
            As operações ocorrem em transações únicas: gravam-se todos os registros ou nenhum.
            """
            data_ref = _data_referencia(context)
            linhas = [
                (data_ref, m["categoria"], m["qtd_produtos"], m["preco_medio"], m["preco_min"], m["preco_max"])
                for m in metricas
            ]
            categorias = [m["categoria"] for m in metricas]

            sql_upsert = f"""
                INSERT INTO {TABELA_SNAPSHOT}
                    (data_referencia, categoria, qtd_produtos, preco_medio, preco_min, preco_max)
                VALUES %s
                ON CONFLICT (data_referencia, categoria) DO UPDATE SET
                    qtd_produtos  = EXCLUDED.qtd_produtos,
                    preco_medio   = EXCLUDED.preco_medio,
                    preco_min     = EXCLUDED.preco_min,
                    preco_max     = EXCLUDED.preco_max,
                    atualizado_em = NOW();
            """
            sql_limpeza = f"""
                DELETE FROM {TABELA_SNAPSHOT}
                WHERE data_referencia = %s AND NOT (categoria = ANY(%s));
            """

            # CONCEITO - Hook + Connection: as Connections armazenam hosts, usuários e
            # senhas FORA do código (neste projeto, provêm de variáveis de ambiente do
            # docker-compose). Os Hooks acessam essas Connections pelos ids e fornecem
            # conexões prontas com os bancos. As trocas de servidor exigem apenas
            # alterações nas Connections, sem modificações na DAG.
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            try:
                # Os blocos "with conn" efetuam commits em caso de sucesso e rollbacks em caso de erro.
                with conn, conn.cursor() as cur:
                    execute_values(cur, sql_upsert, linhas)
                    cur.execute(sql_limpeza, (data_ref, categorias))
            finally:
                conn.close()

            log.info("Snapshot de %s gravado/atualizado: %d categorias", data_ref, len(linhas))
            return len(linhas)

        @task(task_id="salvar_historico")
        def salvar_historico(metricas: list[dict], **context) -> int:
            """
            Gravam-se os históricos em modo APPEND (componentes opcionais da atividade).

            Neste caso, as duplicações são propositais: as execuções convertem-se em
            "retratos" com run_id e horário de coleta, permitindo-se acompanhar as
            evoluções dos preços.
            """
            data_ref = _data_referencia(context)
            run_id = context["run_id"]
            linhas = [
                (data_ref, run_id, m["categoria"], m["qtd_produtos"], m["preco_medio"], m["preco_min"], m["preco_max"])
                for m in metricas
            ]

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            hook.insert_rows(
                table=TABELA_HISTORICO,
                rows=linhas,
                target_fields=[
                    "data_referencia", "run_id", "categoria",
                    "qtd_produtos", "preco_medio", "preco_min", "preco_max",
                ],
            )
            log.info("%d linhas adicionadas ao histórico (run_id=%s)", len(linhas), run_id)
            return len(linhas)

        tabelas = criar_tabelas()
        snapshot = salvar_snapshot(metricas)
        historico = salvar_historico(metricas)

        # As tasks criar_tabelas não retornam dados; por isso, explicitam-se
        # as dependências com >>.
        tabelas >> [snapshot, historico]

    # =======================================================================
    # Montagem da DAG: as saídas dos grupos constituem as entradas dos grupos seguintes
    # =======================================================================
    grupos_por_categoria = ingestao()
    metricas_consolidadas = analise(grupos_por_categoria)
    persistencia(metricas_consolidadas)


# Instanciam-se as DAGs no nível do módulo, onde o Airflow procura os objetos DAG.
shopbrasil_precos_por_categoria()
