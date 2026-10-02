"""Métricas do broker no formato de texto do Prometheus (exposição ``/metrics``).

Funções puras sobre o ledger: nenhum estado é guardado aqui, o texto é montado a
partir de :meth:`~taskflow.core.broker.Broker.stats`. O mesmo texto é servido pelo
dashboard em ``GET /metrics`` e pelo comando ``taskflow.cli metrics``, que lê o
broker em modo somente-leitura (sem tomar a trava de escrita).
"""

from __future__ import annotations

from typing import Final

from taskflow.core.broker import Broker, BrokerStats

#: Prefixo das métricas.
NAMESPACE: Final[str] = "taskflow"

#: Estados expostos como rótulo em ``taskflow_tasks_total``.
ESTADOS: Final[tuple[str, ...]] = (
    "pending",
    "retrying",
    "running",
    "succeeded",
    "failed",
    "dead",
    "cancelled",
)


def escape_label(value: str) -> str:
    """Escapa um valor de rótulo segundo o formato do Prometheus."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def build_metrics(broker: Broker, *, namespace: str = NAMESPACE) -> list[str]:
    """Devolve as linhas de métrica (sem o cabeçalho de content type)."""
    stats: BrokerStats = broker.stats()
    linhas: list[str] = [
        f"# HELP {namespace}_tasks_total Tasks do ledger por fila e por estado.",
        f"# TYPE {namespace}_tasks_total gauge",
    ]
    for fila in stats.queues:
        contagens = fila.to_dict()
        for estado in ESTADOS:
            linhas.append(
                f'{namespace}_tasks_total{{queue="{escape_label(fila.queue)}",'
                f'state="{estado}"}} {contagens[estado]}'
            )
    linhas += [
        f"# HELP {namespace}_ready Tasks prontas para execução agora.",
        f"# TYPE {namespace}_ready gauge",
        f"{namespace}_ready {stats.ready}",
        f"# HELP {namespace}_queued Tasks na fila, inclusive as que aguardam retry.",
        f"# TYPE {namespace}_queued gauge",
        f"{namespace}_queued {stats.queued}",
        f"# HELP {namespace}_in_flight Tasks com lease ativa em um worker.",
        f"# TYPE {namespace}_in_flight gauge",
        f"{namespace}_in_flight {stats.in_flight}",
        f"# HELP {namespace}_dead_lettered Tasks na dead letter queue (DEAD + FAILED).",
        f"# TYPE {namespace}_dead_lettered gauge",
        f"{namespace}_dead_lettered {stats.dead_lettered}",
        f"# HELP {namespace}_succeeded Tasks concluídas com sucesso.",
        f"# TYPE {namespace}_succeeded gauge",
        f"{namespace}_succeeded {stats.succeeded}",
        f"# HELP {namespace}_cancelled Tasks canceladas antes de executar.",
        f"# TYPE {namespace}_cancelled gauge",
        f"{namespace}_cancelled {sum(item.cancelled for item in stats.queues)}",
        f"# HELP {namespace}_uptime_seconds Tempo de vida do processo broker.",
        f"# TYPE {namespace}_uptime_seconds gauge",
        f"{namespace}_uptime_seconds {stats.uptime_seconds:.3f}",
        f"# HELP {namespace}_journal_lines Linhas no journal append-only.",
        f"# TYPE {namespace}_journal_lines gauge",
        f"{namespace}_journal_lines {stats.journal_lines}",
    ]
    return linhas


def build_metrics_text(broker: Broker, *, namespace: str = NAMESPACE) -> str:
    """Métricas completas em texto, prontas para ``text/plain``."""
    return "\n".join(build_metrics(broker, namespace=namespace)) + "\n"


#: Content type oficial da exposição de métricas do Prometheus.
METRICS_CONTENT_TYPE: Final[str] = "text/plain; version=0.0.4; charset=utf-8"