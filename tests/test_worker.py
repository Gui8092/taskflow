"""Testes do pool de workers: execução ponta a ponta, timeout, duplicidade e ordem."""

from __future__ import annotations

import asyncio

from conftest import EventRecorder
from taskflow.core.broker import Broker
from taskflow.core.events import EventType
from taskflow.core.registry import TaskRegistry
from taskflow.core.states import TaskState
from taskflow.worker.deadletter import DeadLetterQueue
from taskflow.worker.pool import WorkerPool
from taskflow.worker.retry import RetryPolicy


async def _run_pool(pool: WorkerPool, timeout: float = 5.0) -> int:
    """Executa tasks uma a uma até a fila esvaziar (sem subir o pool)."""
    processadas = 0
    loop = asyncio.get_running_loop()
    limite = loop.time() + timeout
    while loop.time() < limite:
        if not pool.broker.has_pending_work():
            break
        if await pool.process_one(timeout=0.1) is not None:
            processadas += 1
        await asyncio.sleep(0.01)
    return processadas


async def test_end_to_end_task_sincrona(broker: Broker, registry: TaskRegistry, calls: dict) -> None:
    """Task síncrona vai do submit ao SUCCESS com o valor devolvido."""
    pool = WorkerPool(broker, registry, broker.config)
    task = await broker.submit("ok", "ana")

    assert await _run_pool(pool) == 1

    concluida = broker.require(task.id)
    assert concluida.state is TaskState.SUCCESS
    assert concluida.attempts == 1
    assert concluida.result is not None
    assert concluida.result.value == "concluído:ana"
    assert calls["ok"] == ["ana"]


async def test_end_to_end_task_assincrona(broker: Broker, registry: TaskRegistry) -> None:
    """Task ``async def`` é aguardada e devolve os argumentos recebidos."""
    pool = WorkerPool(broker, registry, broker.config)
    task = await broker.submit("echo", 1, 2, chave="valor")

    await _run_pool(pool)

    concluida = broker.require(task.id)
    assert concluida.state is TaskState.SUCCESS
    assert concluida.result is not None
    assert concluida.result.value == {"args": [1, 2], "kwargs": {"chave": "valor"}}


async def test_eventos_publicados_na_ordem_ciclo_completo(
    broker: Broker, registry: TaskRegistry, recorder: EventRecorder
) -> None:
    """Uma task bem-sucedida emite ``enqueued``, ``started`` e ``success`` nessa ordem."""
    pool = WorkerPool(broker, registry, broker.config)
    task = await broker.submit("ok", "ana")

    await _run_pool(pool)

    assert recorder.types_for(task.id) == ["enqueued", "started", "success"]
    iniciada = recorder.of(EventType.STARTED)[0]
    assert iniciada.payload["worker_id"] == "worker-1"
    assert iniciada.payload["attempt"] == 1
    sucesso = recorder.of(EventType.SUCCESS)[0]
    assert sucesso.payload["attempts"] == 1


async def test_excecao_gera_failed_com_will_retry(
    broker: Broker, registry: TaskRegistry, recorder: EventRecorder, fast_policy: RetryPolicy
) -> None:
    """A falha da tentativa publica ``failed`` com ``will_retry=True`` e agenda retry."""
    pool = WorkerPool(broker, registry, broker.config, fast_policy, queues=("demo",))
    task = await broker.submit("boom", queue="demo", max_retries=3)

    await pool.process_one(timeout=1.0)

    falhou = broker.require(task.id)
    assert falhou.state is TaskState.RETRY
    assert falhou.attempts == 1
    assert falhou.last_error is not None and "falha definitiva" in falhou.last_error
    assert falhou.lease is None
    assert falhou.eta is not None and falhou.eta > 0

    evento = recorder.of(EventType.FAILED)[0]
    assert evento.payload["will_retry"] is True
    assert evento.payload["retry_in"] >= 0
    assert recorder.types_for(task.id) == ["enqueued", "started", "failed"]


async def test_timeout_conta_como_falha_e_dispara_retry(
    broker: Broker, registry: TaskRegistry, recorder: EventRecorder, fast_policy: RetryPolicy
) -> None:
    """Task que ultrapassa o timeout vira falha e entra em retry."""
    pool = WorkerPool(broker, registry, broker.config, fast_policy, queues=("demo",))
    task = await broker.submit("lenta", 5.0, queue="demo", timeout=0.05)

    await pool.process_one(timeout=1.0)

    falhou = broker.require(task.id)
    assert falhou.state is TaskState.RETRY
    assert falhou.attempts == 1
    assert falhou.last_error is not None and "timeout" in falhou.last_error
    assert recorder.of(EventType.FAILED)[0].payload["will_retry"] is True


async def test_nenhuma_task_executa_duas_vezes(
    broker: Broker, registry: TaskRegistry, calls: dict
) -> None:
    """Com 4 workers concorrentes, 20 tasks executam exatamente uma vez cada."""
    total = 20
    for indice in range(total):
        await broker.submit("contador", indice)

    pool = WorkerPool(broker, registry, broker.config, concurrency=4)
    await pool.start()
    try:
        assert await pool.drain(timeout=10.0) is True
    finally:
        await pool.stop(drain=False)

    executados = calls["contador"]
    assert sorted(executados) == list(range(total))
    assert len(executados) == total

    estados = [task.state for task in broker.list_tasks()]
    assert estados.count(TaskState.SUCCESS) == total


async def test_task_desconhecida_vai_para_dead_letter(
    broker: Broker, registry: TaskRegistry, recorder: EventRecorder
) -> None:
    """Task cujo nome não está no registro falha permanentemente (sem gastar retries)."""
    pool = WorkerPool(broker, registry, broker.config)
    task = await broker.submit("ok", "placeholder")
    task.name = "fantasma"  # nome gravado por um processo que registrou a task depois

    await pool.process_one(timeout=1.0)

    falhou = broker.require(task.id)
    assert falhou.state is TaskState.FAILED
    assert falhou.attempts == 1
    assert "fantasma" in (falhou.last_error or "")
    assert DeadLetterQueue(broker).count() == 1
    assert recorder.of(EventType.DEAD) == []


async def test_resultado_nao_serializavel_vira_falha_permanente(
    broker: Broker, registry: TaskRegistry
) -> None:
    """Retorno não JSON-safe não quebra o journal: vira FAILED na DLQ."""
    pool = WorkerPool(broker, registry, broker.config)
    task = await broker.submit("indecoravel")

    await pool.process_one(timeout=1.0)

    falhou = broker.require(task.id)
    assert falhou.state is TaskState.FAILED
    assert "não serializável" in (falhou.last_error or "")
    assert falhou.attempts == 1


async def test_prioridade_respeitada_com_worker_unico(
    broker: Broker, registry: TaskRegistry, calls: dict
) -> None:
    """Dez tasks de mesma fila e um worker saem na ordem de prioridade."""
    prioridades = [5, 1, 9, 3, 7, 0, 8, 2, 6, 4]
    for indice, prioridade in enumerate(prioridades):
        await broker.submit("alta", indice, queue="alta", priority=prioridade)

    pool = WorkerPool(broker, registry, broker.config, concurrency=1, queues=("alta",))
    await pool.start()
    try:
        assert await pool.drain(timeout=10.0) is True
    finally:
        await pool.stop(drain=False)

    esperado = sorted(range(len(prioridades)), key=lambda indice: prioridades[indice], reverse=True)
    assert calls["alta"] == esperado


async def test_drain_aguarda_a_fila_esvaziar(
    broker: Broker, registry: TaskRegistry
) -> None:
    """``drain`` só retorna quando não há task pronta nem em execução."""
    for indice in range(3):
        await broker.submit("ok", str(indice))

    pool = WorkerPool(broker, registry, broker.config, concurrency=2)
    await pool.start()
    assert await pool.drain(timeout=10.0) is True
    assert pool.inflight == 0
    assert pool.processed == 3
    await pool.stop(drain=False)


async def test_pool_stats_refletem_o_trabalho(
    broker: Broker, registry: TaskRegistry
) -> None:
    """``stats()`` do pool informa concorrência, filas e contadores."""
    pool = WorkerPool(broker, registry, broker.config, concurrency=3, queues=("default", "alta"))

    assert pool.describe().startswith("3 worker(s)")
    assert len(pool.workers) == 3
    assert [worker.queue for worker in pool.workers] == ["default", "alta", "default"]

    await broker.submit("ok", "x")
    await pool.process_one(timeout=1.0)

    snapshot = pool.stats()
    assert snapshot["concurrency"] == 3
    assert snapshot["processed"] == 1
    assert snapshot["queues"] == ["default", "alta"]
    assert snapshot["workers"][0]["queue"] == "default"


async def test_lease_renovada_mantem_task_viva(broker: Broker, registry: TaskRegistry) -> None:
    """Com heartbeat ativo, uma task longa não é recolhida pelo varredor de leases."""
    config = broker.config.with_overrides(lease_seconds=0.1, reclaim_interval=0.02)
    pool = WorkerPool(
        broker, registry, config, concurrency=1, queues=("demo",), heartbeat=True
    )
    task = await broker.submit("lenta", 0.4, queue="demo", timeout=5.0)

    processar = asyncio.create_task(pool.process_one(timeout=1.0))
    await asyncio.sleep(0.25)  # bem mais que a lease, mas o heartbeat está ativo
    assert processar.done() is False

    em_execucao = broker.require(task.id)
    assert em_execucao.state is TaskState.RUNNING
    assert em_execucao.lease is not None
    assert em_execucao.lease.expires_at > 0

    await processar
    assert broker.require(task.id).state is TaskState.SUCCESS
    assert broker.require(task.id).attempts == 1