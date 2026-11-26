"""
Callbacks de ciclo de vida das tasks e de violação de SLA.

Em ambientes de produção, os callbacks enviariam mensagens para canais como
Slack, Teams ou e-mail. Neste projeto, SIMULAM-SE os envios: as rotinas
registram as mensagens no log da task e as acrescentam ao arquivo
`logs/alertas.log`, que funciona como uma "caixa de entrada" de alertas
destinada a conferências posteriores.

Os callbacks recebem o `context` da task (dicionário com dag, task_instance,
exception, run_id, entre outros campos).
"""

import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

# Gravam-se os alertas simulados neste arquivo; as pastas logs permanecem montadas no host.
ARQUIVO_ALERTAS = Path("/opt/airflow/logs/alertas.log")


def _registrar_alerta(nivel: str, mensagem: str) -> None:
    """Simulam-se os envios de alertas: registram-se as mensagens no log e gravam-se as linhas em arquivo."""
    linha = f"[{datetime.now().isoformat(timespec='seconds')}] [{nivel}] {mensagem}"
    log.warning("ALERTA SIMULADO -> %s", linha)
    try:
        ARQUIVO_ALERTAS.parent.mkdir(parents=True, exist_ok=True)
        with ARQUIVO_ALERTAS.open("a", encoding="utf-8") as f:
            f.write(linha + "\n")
    except OSError:
        # Os alertas nunca interrompem as tasks; quando as gravações falham,
        # registram-se os erros no log e as execuções prosseguem.
        log.exception("Não foi possível gravar o alerta em %s", ARQUIVO_ALERTAS)


def _descricao(context) -> str:
    """Montam-se as descrições textuais das task instances (dag, task, run_id e tentativa)."""
    ti = context["task_instance"]
    return (
        f"dag={ti.dag_id} task={ti.task_id} run_id={context.get('run_id')} "
        f"tentativa={ti.try_number}"
    )


def alerta_falha(context) -> None:
    """on_failure_callback: acionam-se estes alertas quando as tentativas da task se esgotam e as falhas se tornam definitivas."""
    erro = context.get("exception")
    _registrar_alerta(
        "CRITICO",
        f"Falha definitiva! {_descricao(context)} erro={erro!r} "
        f"-> painel de preços pode ficar sem dados hoje. Log: {context['task_instance'].log_url}",
    )


def alerta_retry(context) -> None:
    """on_retry_callback: acionam-se estes alertas quando as tasks falham e o Airflow agenda novas tentativas."""
    erro = context.get("exception")
    _registrar_alerta("AVISO", f"Retentando. {_descricao(context)} erro={erro!r}")


def alerta_sucesso(context) -> None:
    """on_success_callback: acionam-se estes alertas quando as tasks concluem com sucesso."""
    _registrar_alerta("INFO", f"Sucesso. {_descricao(context)}")


def alerta_sla(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """sla_miss_callback: acionam-se estes alertas quando as tasks ultrapassam os tempos acordados (SLA)."""
    _registrar_alerta(
        "SLA",
        f"SLA estourado na DAG {dag.dag_id}. Tasks atrasadas: {task_list.strip()} "
        f"| Bloqueando: {blocking_task_list.strip() or '-'}",
    )
