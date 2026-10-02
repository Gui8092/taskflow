"""Testes das três funcionalidades somadas à v1.0.0.

Cobre o cancelamento de task, a exposição de métricas no formato Prometheus e o
agendamento por intervalo fixo (inclusive em intervalos menores que um minuto,
o que a expressão cron não permite).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from conftest import EventRecorder
from taskflow.core.broker import Broker, BrokerError, TaskNotFoundError
from taskflow.core.config import Config
from taskflow.core.events import EventType
from taskflow.core.metrics import (
    ESTADOS,
    build_metrics,
    build_metrics_text,
    escape_label,
)
from taskflow.core.registry import TaskRegistry
from taskflow.core.states import TaskState
from taskflow.scheduler.cron import CronExpression
from taskflow.scheduler.scheduler import Schedule, Scheduler, SchedulerError, load_schedules
from taskflow.worker.pool import WorkerPool
from taskflow.worker.retry import RetryPolicy


# ------------------------------------------------------------------ cancelamento


async def test_cancelar_task_pendente(broker: Broker, recorder: EventRecorder) -> None:
    """Uma task pendente vira CANCELLED, sai da fila e publica o evento."""
    task = await broker.submit("ok", "ser cancelada")

    cancelada = await broker.cancel(task.id, reason="não é mais necessária")

    assert cancelada.state is TaskState.CANCELLED
    assert cancelada.state.is_cancelled is True
    assert cancelada.finished_at is not None
    assert cancelada.eta is None
    assert cancelada.last_error == "não é mais necessária"
    assert broker.ready_count() == 0
    assert broker.queued_count() == 0
    assert recorder.types_for(task.id) == ["enqueued", "cancelled"]

    evento = recorder.of(EventType.CANCELLED)[0]
    assert evento.payload["reason"] == "não é mais necessária"
    assert evento.name == "ok"


async def test_task_cancelada_nao_e_entregue_ao_worker(
    broker: Broker, registry: TaskRegistry
) -> None:
    """Depois de cancelada, a task nunca chega a um worker."""
    task = await broker.submit("ok", "x")
    await broker.cancel(task.id)

    pool = WorkerPool(broker, registry, broker.config)
    assert await pool.process_one(timeout=0.1) is None
    assert broker.require(task.id).state is TaskState.CANCELLED
    assert broker.require(task.id).attempts == 0


async def test_cancelar_task_em_retry_e_permitido(
    broker: Broker, registry: TaskRegistry, fast_policy: RetryPolicy
) -> None:
    """Task aguardando retry ainda está na fila, então pode ser cancelada."""
    pool = WorkerPool(broker, registry, broker.config, fast_policy, queues=("demo",))
    task = await broker.submit("boom", queue="demo", max_retries=3)
    await pool.process_one(timeout=1.0)
    assert broker.require(task.id).state is TaskState.RETRY

    cancelada = await broker.cancel(task.id, reason="cancelada durante o retry")

    assert cancelada.state is TaskState.CANCELLED
    assert cancelada.attempts == 1  # a tentativa já feita é preservada


async def test_cancelar_task_em_execucao_falha_com_mensagem(broker: Broker) -> None:
    """Task em execução não pode ser cancelada: não há cancelamento cooperativo."""
    await broker.submit("ok", "rodando")
    executando = await broker.fetch("default", timeout=1.0)
    assert executando is not None

    with pytest.raises(BrokerError) as erro:
        await broker.cancel(executando.id)

    mensagem = str(erro.value)
    assert "não é possível cancelar" in mensagem
    assert "RUNNING" in mensagem


async def test_cancelar_task_terminal_ou_inexistente_falha(broker: Broker) -> None:
    """Task já concluída e id desconhecido são recusados com erros claros."""
    task = await broker.submit("ok", "x")
    executando = await broker.fetch("default", timeout=1.0)
    assert executando is not None
    await broker.ack(executando, "fim")

    with pytest.raises(BrokerError):
        await broker.cancel(task.id)
    with pytest.raises(TaskNotFoundError):
        await broker.cancel("nao-existe")


async def test_task_cancelada_sobrevive_a_restart(
    config: Config, registry: TaskRegistry
) -> None:
    """O estado CANCELLED é persistido e não volta para a fila após reiniciar."""
    primeiro = Broker(config, registry)
    await primeiro.start()
    task = await primeiro.submit("ok", "x")
    await primeiro.cancel(task.id, reason="cancelada")
    await primeiro.stop()

    segundo = Broker(config, registry)
    await segundo.start()
    try:
        recuperada = segundo.require(task.id)
        assert recuperada.state is TaskState.CANCELLED
        assert recuperada.last_error == "cancelada"
        assert segundo.ready_count() == 0
        assert segundo.queued_count() == 0
        assert second_cancelled_count(segundo) == 1
    finally:
        await segundo.stop()


def second_cancelled_count(broker: Broker) -> int:
    """Soma as tasks canceladas em todas as filas (atalho de teste)."""
    return sum(item.cancelled for item in broker.queue_stats())


async def test_task_cancelada_pode_ser_reenfileirada(broker: Broker, registry: TaskRegistry) -> None:
    """Requeue devolve uma task cancelada para a fila com orçamento novo."""
    task = await broker.submit("ok", "x")
    await broker.cancel(task.id)

    reenfileirada = await broker.requeue(task.id)

    assert reenfileirada.state is TaskState.PENDING
    assert reenfileirada.attempts == 0
    pool = WorkerPool(broker, registry, broker.config)
    await pool.process_one(timeout=1.0)
    assert broker.require(task.id).state is TaskState.SUCCESS


async def test_contagens_incluem_canceladas(broker: Broker) -> None:
    """As estatísticas por fila contabilizam as tasks canceladas."""
    task = await broker.submit("ok", "x")
    await broker.cancel(task.id)

    fila = next(item for item in broker.queue_stats() if item.queue == "default")

    assert fila.cancelled == 1
    assert fila.total == 1
    assert fila.to_dict()["cancelled"] == 1


# ------------------------------------------------------------------ métricas


def test_escape_label_do_prometheus() -> None:
    """Barras, aspas e quebras de linha são escapadas no rótulo."""
    assert escape_label('fila"com"aspas') == 'fila\\"com\\"aspas'
    assert escape_label("a\\b") == "a\\\\b"
    assert escape_label("a\nb") == "a\\nb"


async def test_metricas_em_texto_do_prometheus(broker: Broker) -> None:
    """O texto tem formato de métrica válido, com rótulos de fila e estado."""
    await broker.submit("ok", "a")
    executando = await broker.fetch("default", timeout=1.0)
    assert executando is not None
    await broker.ack(executando, "fim")
    cancelada = await broker.submit("ok", "b")
    await broker.cancel(cancelada.id)

    texto = build_metrics_text(broker)

    assert texto.endswith("\n")
    assert "# HELP taskflow_tasks_total" in texto
    assert "# TYPE taskflow_tasks_total gauge" in texto
    assert 'taskflow_tasks_total{queue="default",state="pending"} 0' in texto
    assert 'taskflow_tasks_total{queue="default",state="succeeded"} 1' in texto
    assert 'taskflow_tasks_total{queue="default",state="cancelled"} 1' in texto
    assert "\ntaskflow_ready 0\n" in texto
    assert "\ntaskflow_in_flight 0\n" in texto
    assert "\ntaskflow_cancelled 1\n" in texto
    assert "taskflow_journal_lines " in texto

    # toda linha de métrica (fora de #) precisa ter nome, rótulos opcionais e valor
    metricas = [linha for linha in texto.splitlines() if linha and not linha.startswith("#")]
    assert metricas
    for linha in metricas:
        nome, _, valor = linha.rpartition(" ")
        assert nome.startswith("taskflow_")
        assert valor.replace(".", "", 1).isdigit()


async def test_metricas_listam_todos_os_estados(broker: Broker) -> None:
    """Cada fila expõe uma série para todos os estados conhecidos."""
    await broker.submit("ok", "a")
    linhas = build_metrics(broker)

    for estado in ESTADOS:
        assert any(f'state="{estado}"}}' in linha for linha in linhas)


async def test_metricas_com_fila_de_nome_especial(broker: Broker) -> None:
    """Nome de fila com aspas não quebra o formato das métricas."""
    await broker.submit("ok", "x", queue='fila"estranha')
    texto = build_metrics_text(broker)

    assert 'queue="fila\\"estranha"' in texto


# ------------------------------------------------------------------ agendamento por intervalo


class RelogioFake:
    """Relógio controlado por testes, para não esperar de verdade."""

    def __init__(self, inicio: datetime) -> None:
        """Começa no instante informado."""
        self.agora = inicio

    def __call__(self) -> float:
        """Devolve o instante atual em epoch."""
        return self.agora.timestamp()

    def avancar(self, segundos: float) -> None:
        """Move o relógio para a frente."""
        self.agora = self.agora + timedelta(seconds=segundos)


def test_schedule_exige_cron_ou_interval() -> None:
    """Exatamente um modo de disparo precisa ser informado."""
    with pytest.raises(SchedulerError) as erro:
        Schedule(name="x")
    assert "exatamente um" in str(erro.value)

    with pytest.raises(SchedulerError):
        Schedule(name="x", cron=CronExpression.parse("* * * * *"), interval=10)

    with pytest.raises(SchedulerError) as erro:
        Schedule(name="x", interval=0)
    assert "interval precisa ser > 0" in str(erro.value)

    valido = Schedule(name="x", interval=15)
    assert valido.expression == "a cada 15s"


def test_schedule_por_intervalo_nao_acumula_atraso() -> None:
    """Se o processo ficou parado, o próximo disparo é ``agora + intervalo``."""
    inicio = datetime(2024, 3, 5, 9, 0)
    schedule = Schedule(name="x", interval=30, last_run_at=inicio.timestamp())

    # um minuto depois: o prazo de 30s já passou, então não há rajada pendente
    depois = (inicio + timedelta(seconds=60)).timestamp()
    assert schedule.compute_next(depois) == pytest.approx(depois + 30)

    # dentro do prazo: conta a partir da última execução
    dentro = (inicio + timedelta(seconds=10)).timestamp()
    assert schedule.compute_next(dentro) == pytest.approx(inicio.timestamp() + 30)


async def test_scheduler_dispara_por_intervalo_sub_minuto(
    broker: Broker, registry: TaskRegistry
) -> None:
    """Dispara a cada 15 segundos — algo impossível com cron de 1 minuto."""
    relogio = RelogioFake(datetime(2024, 3, 5, 9, 0))
    agendador = Scheduler(broker, broker.config, clock=relogio, max_sleep=0.5)
    schedule = agendador.add_interval("ok", 15, "batida")

    assert schedule.interval == 15
    assert schedule.cron is None
    assert schedule.tags["scheduled"] == "every 15s"

    await agendador.tick()  # calcula o primeiro horário
    assert schedule.next_run_at == pytest.approx(relogio() + 15)

    relogio.avancar(15)
    assert await agendador.tick() == 1
    assert broker.list_tasks()[0].args == ["batida"]

    relogio.avancar(15)
    assert await agendador.tick() == 1
    assert schedule.run_count == 2
    assert broker.stats().total == 2


async def test_add_interval_herda_metadados_da_task(
    broker: Broker, registry: TaskRegistry
) -> None:
    """``add_interval`` herda fila, prioridade e retries da task registrada."""
    agendador = Scheduler(broker, broker.config)
    schedule = agendador.add_interval(registry.get("flaky"), 30, 1)

    assert schedule.queue == "demo"
    assert schedule.max_retries == 5
    assert schedule.args == [1]
    assert schedule.to_dict()["interval"] == 30
    assert schedule.to_dict()["cron"] is None


async def test_scheduler_loop_real_com_intervalo_curto(
    broker: Broker, registry: TaskRegistry
) -> None:
    """O loop de fundo dispara intervalos sem depender do cron."""
    agendador = Scheduler(broker, broker.config, max_sleep=0.02)
    agendador.add_interval("ok", 0.05, "tick")

    await agendador.start()
    try:
        import asyncio

        for _ in range(200):
            if broker.stats().total >= 2:
                break
            await asyncio.sleep(0.01)
    finally:
        await agendador.stop()

    assert broker.stats().total >= 2
    assert all(item.args == ["tick"] for item in broker.list_tasks())


async def test_next_runs_mostra_intervalo(broker: Broker, registry: TaskRegistry) -> None:
    """``next_runs`` também cobre agendamentos por intervalo."""
    relogio = RelogioFake(datetime(2024, 3, 5, 9, 0))
    agendador = Scheduler(broker, broker.config, clock=relogio)
    agendador.add_interval("ok", 45, "rapido")

    lista = agendador.next_runs()

    assert len(lista) == 1
    assert lista[0]["interval"] == 45
    assert lista[0]["expression"] == "a cada 45s"
    assert lista[0]["next_run_at"] == pytest.approx(relogio() + 45)


async def test_load_schedules_aceita_intervalo(broker: Broker, registry: TaskRegistry) -> None:
    """``load_schedules`` cria os dois tipos de agendamento."""
    agendador = Scheduler(broker, broker.config, clock=RelogioFake(datetime(2024, 3, 5, 9, 0)))

    criados = load_schedules(
        agendador,
        [
            {"name": "ok", "cron": "*/5 * * * *"},
            {"name": "alta", "interval": 20, "queue": "alta"},
        ],
    )

    assert [item.expression for item in criados] == ["*/5 * * * *", "a cada 20s"]
    assert criados[1].queue == "alta"