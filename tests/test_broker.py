"""Testes do broker: filas, prioridade, leases e validações de entrada."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from conftest import EventRecorder
from taskflow.core.broker import (
    Broker,
    BrokerError,
    BrokerLockedError,
    ReadOnlyBrokerError,
    StaleLeaseError,
)
from taskflow.core.config import Config
from taskflow.core.events import EventType
from taskflow.core.registry import TaskRegistry, UnknownTaskError
from taskflow.core.serialization import SerializationError
from taskflow.core.states import TaskState
from taskflow.core.task import TaskFilter


async def test_submit_enfileira_e_persiste(broker: Broker, config: Config) -> None:
    """A task enviada aparece no ledger, na fila e no journal."""
    task = await broker.submit("ok", "ana")

    assert task.state is TaskState.PENDING
    assert task.queue == "default"
    assert task.attempts == 0
    assert broker.get(task.id) is task
    assert broker.ready_count() == 1

    linhas = config.journal_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(linhas) == 1
    registro = json.loads(linhas[0])
    assert registro["op"] == "enqueue"
    assert registro["task"]["name"] == "ok"
    assert registro["task"]["args"] == ["ana"]


async def test_fifo_dentro_da_mesma_prioridade(broker: Broker) -> None:
    """Sem prioridade explícita, a fila respeita a ordem de chegada."""
    tasks = [await broker.submit("contador", index) for index in range(5)]

    retiradas = []
    for _ in tasks:
        atual = await broker.fetch("default", timeout=1.0)
        assert atual is not None
        retiradas.append(atual.args[0])
        await broker.ack(atual, atual.args[0])

    assert retiradas == [0, 1, 2, 3, 4]


async def test_prioridade_maior_executa_primeiro(broker: Broker) -> None:
    """Dez tasks com prioridades misturadas saem em ordem decrescente de prioridade."""
    prioridades = [3, 9, 1, 7, 0, 9, 5, 2, 8, 4]
    for prioridade in prioridades:
        await broker.submit("alta", prioridade, priority=prioridade, queue="alta")

    ordem = []
    for _ in prioridades:
        task = await broker.fetch("alta", timeout=1.0)
        assert task is not None
        ordem.append(task.priority)
        await broker.ack(task, None)

    assert ordem == sorted(prioridades, reverse=True)


async def test_prioridade_negativa_e_empates_preservam_fifo(broker: Broker) -> None:
    """Prioridades negativas são aceitas e empates mantêm a ordem de chegada."""
    await broker.submit("ok", "baixa", priority=-5, queue="alta")
    await broker.submit("ok", "primeira", priority=3, queue="alta")
    await broker.submit("ok", "segunda", priority=3, queue="alta")

    primeira = await broker.fetch("alta", timeout=1.0)
    segunda = await broker.fetch("alta", timeout=1.0)
    terceira = await broker.fetch("alta", timeout=1.0)

    assert primeira is not None and primeira.args[0] == "primeira"
    assert segunda is not None and segunda.args[0] == "segunda"
    assert terceira is not None and terceira.args[0] == "baixa"


async def test_filas_sao_isoladas(broker: Broker) -> None:
    """Uma task da fila 'alta' não aparece na busca da fila 'default'."""
    await broker.submit("alta", 1, queue="alta")

    assert await broker.fetch("default", timeout=0.05) is None
    assert await broker.fetch("alta", timeout=0.5) is not None


async def test_eta_impede_entrega_antecipada(broker: Broker) -> None:
    """Uma task com ``delay`` não é entregue antes do horário."""
    task = await broker.submit("ok", "depois", delay=0.3)

    assert await broker.fetch("default", timeout=0.1) is None
    assert broker.ready_count() == 0

    entregue = await broker.fetch("default", timeout=1.0)
    assert entregue is not None
    assert entregue.id == task.id
    assert entregue.attempts == 1


async def test_submit_com_nome_desconhecido_falha(broker: Broker) -> None:
    """Nome não registrado levanta erro claro antes de tocar no journal."""
    with pytest.raises(UnknownTaskError) as erro:
        await broker.submit("nao_existe")
    assert "nao_existe" in str(erro.value)


async def test_submit_rejeita_argumentos_nao_serializaveis(broker: Broker, config: Config) -> None:
    """Payloads não JSON-safe são recusados com o caminho do valor na mensagem."""
    with pytest.raises(SerializationError) as erro:
        await broker.submit("ok", {"fn": lambda: None})
    assert "function" in str(erro.value)
    assert "args[0].fn" in str(erro.value)

    with pytest.raises(SerializationError):
        await broker.submit("ok", float("nan"))

    assert broker.stats().total == 0
    journal = config.journal_path
    assert not journal.exists() or journal.read_text(encoding="utf-8").strip() == ""


async def test_submit_rejeita_max_retries_negativo(broker: Broker) -> None:
    """``max_retries`` negativo é erro de uso, não estado de task."""
    with pytest.raises(BrokerError):
        await broker.submit("ok", max_retries=-1)


async def test_submit_rejeita_delay_e_eta_juntos(broker: Broker) -> None:
    """``delay`` e ``eta`` são mutuamente exclusivos."""
    with pytest.raises(BrokerError):
        await broker.submit("ok", delay=1, eta=1)


async def test_ack_com_lease_obsoleta_falha(broker: Broker) -> None:
    """Um worker com lease velha não consegue gravar o resultado."""
    await broker.submit("ok", "em andamento")
    task = await broker.fetch("default", timeout=1.0)
    assert task is not None and task.lease is not None
    lease_id = task.lease.lease_id

    await broker.reclaim_expired(now=float("inf"))

    with pytest.raises(StaleLeaseError):
        await broker.ack(task, "resultado antigo", lease_id=lease_id)


async def test_fetch_desperta_ao_receber_nova_task(broker: Broker) -> None:
    """Um worker bloqueado no ``fetch`` acorda quando outra task é enfileirada."""

    async def consumidor() -> str | None:
        """Espera a próxima task da fila e devolve seu primeiro argumento."""
        task = await broker.fetch("default", timeout=1.0)
        return task.args[0] if task else None

    espera = asyncio.create_task(consumidor())
    await asyncio.sleep(0.02)
    await broker.submit("ok", "acordou")
    assert await espera == "acordou"


async def test_list_tasks_aplica_filtros(broker: Broker) -> None:
    """``list_tasks`` filtra por estado, fila e limite."""
    primeira = await broker.submit("ok", "a")
    await broker.submit("alta", 1, queue="alta")
    segunda = await broker.submit("ok", "b")

    por_fila = broker.list_tasks(TaskFilter(queue="alta"))
    assert len(por_fila) == 1
    assert por_fila[0].queue == "alta"

    task = await broker.fetch("default", timeout=1.0)
    assert task is not None
    assert task.id == primeira.id  # FIFO dentro da mesma prioridade
    await broker.ack(task, "feito")

    concluidas = broker.list_tasks(TaskFilter(state=TaskState.SUCCESS))
    assert [item.id for item in concluidas] == [primeira.id]
    assert segunda.state is TaskState.PENDING
    assert len(broker.list_tasks(TaskFilter(limit=1))) == 1


async def test_stats_e_counts_somam_por_estado(broker: Broker) -> None:
    """``stats()`` agrega por fila e por estado."""
    await broker.submit("ok", "a")
    await broker.submit("alta", 1, queue="alta")
    task = await broker.fetch("default", timeout=1.0)
    assert task is not None
    await broker.ack(task, None)

    stats = broker.stats()
    assert stats.total == 2
    assert stats.ready == 1
    assert stats.in_flight == 0
    assert stats.succeeded == 1
    assert stats.dead_lettered == 0
    assert {"alta", "default"}.issubset({item.queue for item in stats.queues})
    assert sum(broker.counts().values()) == 2


async def test_broker_somente_leitura_recusa_escrita(
    config: Config, registry: TaskRegistry, data_dir: Path
) -> None:
    """Um broker ``read_only`` carrega o estado mas recusa ``submit`` e ``fetch``."""
    escritor = Broker(config, registry)
    await escritor.start()
    await escritor.submit("ok", "gravada")
    await escritor.stop()

    leitor = Broker(config, registry, read_only=True)
    await leitor.start()
    try:
        assert leitor.stats().total == 1
        with pytest.raises(ReadOnlyBrokerError):
            await leitor.submit("ok", "nova")
        with pytest.raises(ReadOnlyBrokerError):
            await leitor.fetch("default", timeout=0.05)
    finally:
        await leitor.stop()


async def test_trava_de_escrita_impede_dois_autores(data_dir: Path, registry: TaskRegistry) -> None:
    """Dois processos não podem escrever no mesmo ``data_dir`` ao mesmo tempo."""
    config = Config(data_dir=data_dir, lock_enabled=True, queues=("default",), retry_base=0.01)
    primeiro = Broker(config, registry)
    await primeiro.start()
    try:
        with pytest.raises(BrokerLockedError) as erro:
            await Broker(config, registry).start()
        assert "escrevendo" in str(erro.value)
    finally:
        await primeiro.stop()

    terceiro = Broker(config, registry)
    await terceiro.start()
    await terceiro.stop()


async def test_evento_enqueued_publica_prioridade_e_fila(
    broker: Broker, recorder: EventRecorder
) -> None:
    """O evento ``enqueued`` carrega os metadados úteis da task."""
    await broker.submit("ok", "ana", priority=7, queue="default")

    evento = recorder.of(EventType.ENQUEUED)[0]
    assert evento.name == "ok"
    assert evento.queue == "default"
    assert evento.payload["priority"] == 7
    assert evento.payload["max_retries"] == 3