"""Testes da política de retry e da dead letter queue."""

from __future__ import annotations

import asyncio
import random

import pytest

from conftest import EventRecorder
from taskflow.core.broker import Broker, BrokerError, TaskNotFoundError
from taskflow.core.config import Config
from taskflow.core.events import EventType
from taskflow.core.registry import TaskRegistry
from taskflow.core.states import TaskState
from taskflow.worker.deadletter import DeadLetterQueue
from taskflow.worker.pool import WorkerPool
from taskflow.worker.retry import RetryPolicy


async def _executar_ate_terminar(broker: Broker, pool: WorkerPool, task_id: str, limite: float = 5.0) -> None:
    """Processa tasks uma a uma até a task observada chegar a um estado terminal."""
    loop = asyncio.get_running_loop()
    fim = loop.time() + limite
    while loop.time() < fim:
        if broker.require(task_id).state.is_terminal:
            return
        if not broker.ready_count():
            await asyncio.sleep(0.01)  # espera o eta do retry
            continue
        await pool.process_one(timeout=0.2)


async def _encher_dlq(
    broker: Broker, registry: TaskRegistry, policy: RetryPolicy, *, queue: str = "demo", total: int = 3
) -> list[str]:
    """Enfileira tasks que sempre falham e as leva até a dead letter queue."""
    pool = WorkerPool(broker, registry, broker.config, policy, queues=(queue,))
    ids: list[str] = []
    for _ in range(total):
        ids.append((await broker.submit("boom", queue=queue, max_retries=1)).id)
    loop = asyncio.get_running_loop()
    fim = loop.time() + 10.0
    while loop.time() < fim:
        if all(broker.require(task_id).state.is_terminal for task_id in ids):
            return ids
        if not broker.ready_count():
            await asyncio.sleep(0.01)
            continue
        await pool.process_one(timeout=0.2)
    return ids


# ------------------------------------------------------------------ política de retry


def test_backoff_cresce_exponencialmente() -> None:
    """O teto do backoff dobra a cada tentativa."""
    policy = RetryPolicy(base=1.0, cap=30.0, jitter=False)

    tetos = [policy.ceiling_for(tentativa) for tentativa in range(1, 7)]

    assert tetos == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
    assert policy.delay_for(1) == 1.0
    assert policy.delay_for(3) == 4.0


def test_backoff_respeita_o_teto() -> None:
    """Nenhum atraso passa de ``cap``."""
    policy = RetryPolicy(base=2.0, cap=5.0, jitter=False)

    assert policy.delay_for(10) == 5.0
    assert policy.ceiling_for(0) == 0.0
    assert policy.delay_for(0) == 0.0


def test_jitter_sorteia_dentro_do_intervalo() -> None:
    """Com jitter, o atraso fica entre 0 e o teto exponencial."""
    policy = RetryPolicy(base=1.0, cap=60.0, jitter=True)
    rng = random.Random(1234)

    amostras = [policy.delay_for(4, rng=rng) for _ in range(200)]

    assert all(0.0 <= atraso <= policy.ceiling_for(4) for atraso in amostras)
    assert len(set(amostras)) > 1
    assert min(amostras) < 1.0 and max(amostras) > 6.0


def test_should_retry_conta_o_orcamento() -> None:
    """``should_retry`` compara as tentativas feitas com o orçamento de retries."""
    policy = RetryPolicy(max_retries=2)

    assert policy.should_retry(attempt=1, max_retries=2) is True
    assert policy.should_retry(attempt=2, max_retries=2) is True
    assert policy.should_retry(attempt=3, max_retries=2) is False
    assert policy.should_retry(attempt=1, max_retries=2, permanent=True) is False


def test_politica_vem_da_configuracao(config: Config) -> None:
    """``RetryPolicy.from_config`` lê base, teto, jitter e retries padrão."""
    policy = RetryPolicy.from_config(config)

    assert policy.base == config.retry_base
    assert policy.cap == config.retry_cap
    assert policy.jitter is config.retry_jitter
    assert policy.max_retries == config.default_max_retries
    assert "base=" in policy.describe()
    assert 100.0 <= policy.eta_for(1, now=100.0) <= 100.0 + config.retry_base


# ------------------------------------------------------------------ retry fim a fim


async def test_falha_duas_vezes_e_acerta_na_terceira(
    broker: Broker, registry: TaskRegistry, calls: dict, fast_policy: RetryPolicy
) -> None:
    """Cenário de aceitação: falha 2x, acerta na 3ª ⇒ SUCCESS com ``attempts=3``."""
    pool = WorkerPool(broker, registry, broker.config, fast_policy, queues=("demo",))
    task = await broker.submit("flaky", 2, queue="demo", max_retries=5)

    await _executar_ate_terminar(broker, pool, task.id)

    concluida = broker.require(task.id)
    assert concluida.state is TaskState.SUCCESS
    assert concluida.attempts == 3
    assert concluida.result is not None and concluida.result.value == "acertei"
    assert len(calls["flaky"]) == 3


async def test_esgotar_retries_manda_para_dead_letter(
    broker: Broker, registry: TaskRegistry, recorder: EventRecorder, fast_policy: RetryPolicy
) -> None:
    """Depois de ``max_retries + 1`` execuções a task fica DEAD e emite ``failed``+``dead``."""
    pool = WorkerPool(broker, registry, broker.config, fast_policy, queues=("demo",))
    task = await broker.submit("boom", queue="demo", max_retries=2)

    await _executar_ate_terminar(broker, pool, task.id)

    morta = broker.require(task.id)
    assert morta.state is TaskState.DEAD
    assert morta.attempts == 3
    assert morta.finished_at is not None
    assert morta.result is not None
    assert morta.result.attempts == 3
    assert morta.result.error is not None and "falha definitiva" in morta.result.error

    assert recorder.types_for(task.id).count("failed") == 3
    assert recorder.types_for(task.id)[-2:] == ["failed", "dead"]
    assert DeadLetterQueue(broker).count() == 1


async def test_backoff_do_broker_usa_a_configuracao(
    broker: Broker, registry: TaskRegistry
) -> None:
    """Sem ``retry_delay`` do worker, o broker calcula o backoff pela configuração."""
    await broker.submit("ok", "x")
    executada = await broker.fetch("default", timeout=1.0)
    assert executada is not None

    await broker.nack(executada, "erro manual")

    reagendada = broker.require(executada.id)
    assert reagendada.state is TaskState.RETRY
    atraso = (reagendada.eta or 0) - (reagendada.finished_at or 0)
    assert 0.0 <= atraso <= broker.config.retry_base


# ------------------------------------------------------------------ dead letter queue


async def test_dlq_lista_e_filtra_por_fila(
    broker: Broker, registry: TaskRegistry, fast_policy: RetryPolicy
) -> None:
    """A DLQ lista por recência e aceita filtro de fila."""
    await _encher_dlq(broker, registry, fast_policy, queue="demo")

    dlq = DeadLetterQueue(broker)
    assert dlq.count() == 3
    assert dlq.count(queue="demo") == 3
    assert dlq.count(queue="outra") == 0
    assert len(dlq.list(limit=2)) == 2
    assert all(item.state is TaskState.DEAD for item in dlq.list())
    assert dlq.reasons()["DEAD"] == 3
    assert dlq.reasons()["FAILED"] == 0
    assert dlq.get(dlq.list()[0].id) is not None


async def test_dlq_requeue_zera_tentativas_e_executa(
    broker: Broker, registry: TaskRegistry, fast_policy: RetryPolicy
) -> None:
    """Requeue da DLQ devolve a task para PENDING com ``attempts=0`` e ela executa."""
    ids = await _encher_dlq(broker, registry, fast_policy, queue="demo")
    dlq = DeadLetterQueue(broker)
    assert broker.require(ids[0]).state is TaskState.DEAD

    reenfileirada = await dlq.requeue(ids[0], queue="alta", priority=8)

    assert reenfileirada.state is TaskState.PENDING
    assert reenfileirada.attempts == 0
    assert reenfileirada.queue == "alta"
    assert reenfileirada.priority == 8
    assert reenfileirada.result is None
    assert dlq.count() == 2

    pool = WorkerPool(broker, registry, broker.config, fast_policy, queues=("alta",))
    await pool.process_one(timeout=1.0)

    reprocessada = broker.require(ids[0])
    assert reprocessada.state in {TaskState.RETRY, TaskState.DEAD}
    assert reprocessada.attempts == 1


async def test_requeue_preserva_tentativas_quando_pedido(
    broker: Broker, registry: TaskRegistry, fast_policy: RetryPolicy
) -> None:
    """Com ``reset_attempts=False`` o orçamento de retries continua o mesmo."""
    ids = await _encher_dlq(broker, registry, fast_policy, queue="demo")
    antes = broker.require(ids[0]).attempts

    reenfileirada = await DeadLetterQueue(broker).requeue(ids[0], reset_attempts=False)

    assert reenfileirada.attempts == antes


async def test_requeue_de_task_inexistente_falha(broker: Broker) -> None:
    """Requeue de id desconhecido levanta erro explícito."""
    with pytest.raises(TaskNotFoundError):
        await DeadLetterQueue(broker).requeue("nao-existe")


async def test_requeue_de_task_pendente_falha(broker: Broker) -> None:
    """Só tasks terminais podem ser reenfileiradas."""
    task = await broker.submit("ok", "x")

    with pytest.raises(BrokerError):
        await broker.requeue(task.id)


async def test_requeue_all_e_purge(
    broker: Broker, registry: TaskRegistry, fast_policy: RetryPolicy
) -> None:
    """``requeue_all`` reenfileira tudo; ``purge`` remove definitivamente do ledger."""
    ids = await _encher_dlq(broker, registry, fast_policy, queue="demo")
    dlq = DeadLetterQueue(broker)
    assert dlq.count() == 3

    reenfileiradas = await dlq.requeue_all()
    assert len(reenfileiradas) == 3
    assert dlq.count() == 0
    assert all(broker.require(task_id).state is TaskState.PENDING for task_id in ids)

    pool = WorkerPool(broker, registry, broker.config, fast_policy, queues=("demo",))
    loop = asyncio.get_running_loop()
    fim = loop.time() + 10.0
    while loop.time() < fim and dlq.count() < 3:
        if broker.ready_count():
            await pool.process_one(timeout=0.2)
        else:
            await asyncio.sleep(0.01)
    assert dlq.count() == 3

    assert await dlq.purge() == 3
    assert broker.stats().total == 0
    assert dlq.count() == 0


async def test_evento_de_requeue_marca_a_origem(
    broker: Broker, registry: TaskRegistry, recorder: EventRecorder, fast_policy: RetryPolicy
) -> None:
    """O ``enqueued`` de um requeue carrega ``requeued`` e o estado anterior."""
    ids = await _encher_dlq(broker, registry, fast_policy, queue="demo")
    recorder.clear()

    await DeadLetterQueue(broker).requeue(ids[0])

    evento = recorder.of(EventType.ENQUEUED)[-1]
    assert evento.payload["requeued"] is True
    assert evento.payload["previous_state"] == TaskState.DEAD.value