"""Testes de persistência: recuperação após restart, compaction e leitor concorrente."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from taskflow.core.broker import Broker
from taskflow.core.config import Config
from taskflow.core.registry import TaskRegistry
from taskflow.core.states import TaskState
from taskflow.worker.deadletter import DeadLetterQueue
from taskflow.worker.pool import WorkerPool
from taskflow.worker.retry import RetryPolicy


async def test_task_enfileirada_sobrevive_a_restart(
    data_dir: Path, config: Config, registry: TaskRegistry
) -> None:
    """Uma task enviada antes do 'restart' aparece inteira no novo broker."""
    primeiro = Broker(config, registry)
    await primeiro.start()
    task = await primeiro.submit("echo", "ana", destino="inbox")
    await primeiro.stop()

    segundo = Broker(config, registry)
    await segundo.start()
    try:
        recuperada = segundo.require(task.id)
        assert recuperada.name == "echo"
        assert recuperada.queue == "default"
        assert recuperada.args == ["ana"]
        assert recuperada.kwargs == {"destino": "inbox"}
        assert recuperada.state is TaskState.PENDING
        assert recuperada.max_retries == 3
        assert segundo.ready_count() == 1

        pool = WorkerPool(segundo, registry, config)
        await pool.process_one(timeout=1.0)
        assert segundo.require(task.id).state is TaskState.SUCCESS
    finally:
        await segundo.stop()


async def test_resultado_sobrevive_a_restart(
    config: Config, registry: TaskRegistry
) -> None:
    """O resultado de uma task bem-sucedida também é recuperado do disco."""
    primeiro = Broker(config, registry)
    await primeiro.start()
    task = await primeiro.submit("ok", "ana")
    executando = await primeiro.fetch("default", timeout=1.0)
    assert executando is not None
    await primeiro.ack(executando, {"ok": True})
    await primeiro.stop()

    segundo = Broker(config, registry)
    await segundo.start()
    try:
        recuperada = segundo.require(task.id)
        assert recuperada.state is TaskState.SUCCESS
        assert recuperada.attempts == 1
        assert recuperada.result is not None
        assert recuperada.result.value == {"ok": True}
        assert recuperada.result.duration_ms is not None
        assert recuperada.lease is None
    finally:
        await segundo.stop()


async def test_dlq_sobrevive_a_restart(
    config: Config, registry: TaskRegistry
) -> None:
    """Tasks mortas continuam na DLQ depois de reiniciar o processo."""
    policy = RetryPolicy(base=0.01, cap=0.02, jitter=False)
    primeiro = Broker(config, registry)
    await primeiro.start()
    pool = WorkerPool(primeiro, registry, config, policy, queues=("demo",))
    task = await primeiro.submit("boom", queue="demo", max_retries=0)
    await pool.process_one(timeout=1.0)
    assert primeiro.require(task.id).state is TaskState.DEAD
    await primeiro.stop()

    segundo = Broker(config, registry)
    await segundo.start()
    try:
        morta = segundo.require(task.id)
        assert morta.state is TaskState.DEAD
        assert DeadLetterQueue(segundo).count() == 1
        assert DeadLetterQueue(segundo).get(task.id) is not None
    finally:
        await segundo.stop()


async def test_task_running_e_devolvida_ao_iniciar(
    config: Config, registry: TaskRegistry
) -> None:
    """Task que ficou RUNNING quando o processo morreu volta para PENDING."""
    primeiro = Broker(config, registry)
    await primeiro.start()
    await primeiro.submit("ok", "interrompida")
    executando = await primeiro.fetch("default", timeout=1.0)
    assert executando is not None and executando.state is TaskState.RUNNING
    await primeiro.stop()  # simula o processo morrendo com a task em execução

    segundo = Broker(config.with_overrides(recover_running_on_start=True), registry)
    await segundo.start()
    try:
        recuperada = segundo.require(executando.id)
        assert recuperada.state is TaskState.PENDING
        assert recuperada.recovered == 1
        assert recuperada.lease is None
        assert recuperada.started_at is None
        assert segundo.ready_count() == 1
    finally:
        await segundo.stop()


async def test_lease_expirada_e_recolhida_pelo_varredor(
    config: Config, registry: TaskRegistry
) -> None:
    """Worker morto no meio da execução: a lease vence e a task volta à fila."""
    config = config.with_overrides(lease_seconds=0.05, reclaim_interval=30.0)
    broker = Broker(config, registry)
    await broker.start()
    try:
        task = await broker.submit("ok", "abandonada")
        orfa = await broker.fetch("default", timeout=1.0, worker_id="worker-morto")
        assert orfa is not None and orfa.lease is not None
        assert orfa.lease.worker_id == "worker-morto"

        await asyncio.sleep(0.12)  # deixa a lease vencer sem heartbeat

        devolvidas = await broker.reclaim_expired()
        assert [item.id for item in devolvidas] == [task.id]
        assert broker.require(task.id).state is TaskState.PENDING
        assert broker.require(task.id).recovered == 1
        assert broker.require(task.id).attempts == 1  # a tentativa consumida é preservada
        assert broker.require(task.id).lease is None

        # Uma segunda varredura não devolve a mesma task de novo.
        assert await broker.reclaim_expired() == []
    finally:
        await broker.stop()


async def test_varredor_automatico_recupera_task_abandonada(
    config: Config, registry: TaskRegistry
) -> None:
    """O laço de fundo devolve tasks abandonadas sem ninguém pedir explicitamente."""
    config = config.with_overrides(lease_seconds=0.05, reclaim_interval=0.02)
    broker = Broker(config, registry)
    await broker.start()
    try:
        task = await broker.submit("ok", "sozinha")
        orfa = await broker.fetch("default", timeout=1.0, worker_id="worker-morto")
        assert orfa is not None

        for _ in range(100):
            if broker.require(task.id).state is TaskState.PENDING:
                break
            await asyncio.sleep(0.01)

        recuperada = broker.require(task.id)
        assert recuperada.state is TaskState.PENDING
        assert recuperada.recovered == 1
    finally:
        await broker.stop()


async def test_journal_e_append_only_uma_linha_por_mudanca(
    broker: Broker, config: Config
) -> None:
    """Cada mudança de estado acrescenta exatamente uma linha ao journal."""
    await broker.submit("ok", "x")
    assert broker.journal_lines == 1
    executando = await broker.fetch("default", timeout=1.0)
    assert executando is not None
    assert broker.journal_lines == 2
    await broker.ack(executando, "fim")
    assert broker.journal_lines == 3

    operacoes = [
        json.loads(linha)["op"]
        for linha in config.journal_path.read_text(encoding="utf-8").strip().splitlines()
    ]
    assert operacoes == ["enqueue", "start", "ack"]


async def test_compaction_escreve_snapshot_e_trunca_journal(
    data_dir: Path, registry: TaskRegistry
) -> None:
    """Ao passar de ``compact_lines``, o snapshot é escrito e o journal reinicia."""
    config = Config(
        data_dir=data_dir,
        queues=("default",),
        compact_lines=3,
        ledger_limit=100,
        lock_enabled=False,
        retry_base=0.01,
    )
    broker = Broker(config, registry)
    await broker.start()
    try:
        for indice in range(3):
            await broker.submit("ok", indice)

        assert config.snapshot_path.exists()
        assert broker.journal_lines < 3
        payload = json.loads(config.snapshot_path.read_text(encoding="utf-8"))
        assert payload["version"] == 1
        assert len(payload["tasks"]) == 3
    finally:
        await broker.stop()

    reencontrado = Broker(config, registry)
    await reencontrado.start()
    try:
        assert reencontrado.stats().total == 3
        assert reencontrado.ready_count() == 3
    finally:
        await reencontrado.stop()


async def test_linha_truncada_no_journal_e_ignorada(
    broker: Broker, config: Config
) -> None:
    """Uma gravação pela metade no fim do journal não impede a recuperação."""
    await broker.submit("ok", "boa")
    with config.journal_path.open("a", encoding="utf-8") as arquivo:
        arquivo.write('{"op": "enqueue", "id": "abc", "tas')
    await broker.stop()

    reencontrado = Broker(config, registry_of(broker))
    await reencontrado.start()
    try:
        assert reencontrado.stats().total == 1
        assert reencontrado.stats().ready == 1
    finally:
        await reencontrado.stop()


def registry_of(broker: Broker) -> TaskRegistry:
    """Devolve o registro usado por um broker (atalho para os testes de disco)."""
    return broker.registry


async def test_leitor_atualiza_a_partir_do_journal(
    config: Config, registry: TaskRegistry
) -> None:
    """Um broker somente-leitura acompanha as escritas de outro processo via ``refresh``."""
    escritor = Broker(config, registry)
    await escritor.start()
    leitor = Broker(config, registry, read_only=True)
    await leitor.start()
    try:
        assert leitor.stats().total == 0

        await escritor.submit("ok", "primeira")
        assert leitor.refresh() == 1
        assert leitor.stats().total == 1

        executando = await escritor.fetch("default", timeout=1.0)
        assert executando is not None
        await escritor.ack(executando, "pronto")
        leitor.refresh()

        assert leitor.stats().succeeded == 1
        assert leitor.get(executando.id) is not None
    finally:
        await leitor.stop()
        await escritor.stop()


async def test_remove_grava_operacao_de_remocao(
    broker: Broker, config: Config
) -> None:
    """``remove`` apaga a task do ledger e do journal."""
    task = await broker.submit("ok", "x")

    assert await broker.remove(task.id) is True
    assert await broker.remove(task.id) is False
    assert broker.get(task.id) is None

    await broker.stop()
    reencontrado = Broker(config, registry_of(broker))
    await reencontrado.start()
    try:
        assert reencontrado.stats().total == 0
        assert "remove" in config.journal_path.read_text(encoding="utf-8")
    finally:
        await reencontrado.stop()