"""Testes da infraestrutura do núcleo: configuração, estados, eventos e leases.

Cobre as partes que os demais arquivos de teste exercitam apenas de passagem:
parsing de variáveis de ambiente, validação de configuração, transições do ciclo
de vida, event bus (assinantes assíncronos e ``wait_for``), renovação de lease,
``fsync`` ligado e armazenamento do traceback no resultado.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest

from taskflow.core.broker import Broker, StaleLeaseError
from taskflow.core.config import (
    ENV_PREFIX,
    Config,
    ConfigError,
    KeyValueFormatter,
    env_var,
    get_config,
    setup_logging,
)
from taskflow.core.events import Event, EventBus, EventType
from taskflow.core.registry import TaskRegistry
from taskflow.core.states import (
    TERMINAL_STATES,
    StateError,
    TaskState,
    can_transition,
    describe_state,
    is_terminal,
    validate_transition,
)
from taskflow.core.task import Task
from taskflow.worker.pool import WorkerPool
from taskflow.worker.retry import RetryPolicy


# ------------------------------------------------------------------ configuração


def test_deFAULTS_sao_coeridos_e_validados(tmp_path: Path) -> None:
    """Mesmo constructed direto, os campos de texto são normalizados."""
    config = Config(data_dir=str(tmp_path / "estado"), queues=["a", "b"], modules=("m",))

    assert isinstance(config.data_dir, Path)
    assert config.queues == ("a", "b")
    assert config.modules == ("m",)
    assert config.default_queue == "default"
    assert config.journal_path.name == "journal.jsonl"


@pytest.mark.parametrize(
    ("variavel", "esperado"),
    [
        ("TASKFLOW_WORKER_CONCURRENCY", 8),
        ("TASKFLOW_LEDGER_LIMIT", 42),
        ("TASKFLOW_DASHBOARD_PORT", 9999),
    ],
)
def test_from_env_converte_inteiros(monkeypatch: pytest.MonkeyPatch, variavel: str, esperado: int) -> None:
    """Variáveis inteiras são convertidas e aplicadas."""
    monkeypatch.setenv(variavel, str(esperado))
    campo = variavel.removeprefix(ENV_PREFIX).lower()

    assert getattr(Config.from_env(), campo) == esperado


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [("1", True), ("true", True), ("yes", True), ("on", True), ("0", False), ("false", False), ("off", False)],
)
def test_from_env_converte_booleanos(monkeypatch: pytest.MonkeyPatch, valor: str, esperado: bool) -> None:
    """Booleanos aceitam as formas usuais de escrita."""
    monkeypatch.setenv("TASKFLOW_RETRY_JITTER", valor)

    assert Config.from_env().retry_jitter is esperado


def test_from_env_converte_listas_e_booleano_opcional(monkeypatch: pytest.MonkeyPatch) -> None:
    """Listas viram tupla e timeout vazio significa "sem timeout"."""
    monkeypatch.setenv("TASKFLOW_QUEUES", "default, alta ,demo")
    monkeypatch.setenv("TASKFLOW_MODULES", "a.b, c.d")
    monkeypatch.setenv("TASKFLOW_DEFAULT_TIMEOUT", "")

    config = Config.from_env()

    assert config.queues == ("default", "alta", "demo")
    assert config.modules == ("a.b", "c.d")
    assert config.default_timeout is None


@pytest.mark.parametrize(
    ("variavel", "valor"),
    [
        ("TASKFLOW_WORKER_CONCURRENCY", "muitos"),
        ("TASKFLOW_LEASE_SECONDS", "dez"),
        ("TASKFLOW_RETRY_JITTER", "talvez"),
        ("TASKFLOW_LEDGER_LIMIT", "1.5"),
    ],
)
def test_from_env_rejeita_valor_invalido_com_mensagem_clara(
    monkeypatch: pytest.MonkeyPatch, variavel: str, valor: str
) -> None:
    """Valor inválido levanta ConfigError citando a variável e o motivo."""
    monkeypatch.setenv(variavel, valor)

    with pytest.raises(ConfigError) as erro:
        Config.from_env()

    mensagem = str(erro.value)
    assert variavel in mensagem
    assert valor in mensagem


@pytest.mark.parametrize(
    ("kwargs", "trecho"),
    [
        ({"worker_concurrency": 0}, "worker_concurrency"),
        ({"default_max_retries": -1}, "default_max_retries"),
        ({"lease_seconds": 0}, "lease_seconds"),
        ({"reclaim_interval": -5}, "reclaim_interval"),
        ({"retry_base": 5.0, "retry_cap": 1.0}, "retry_cap"),
        ({"compact_lines": -1}, "compact_lines"),
        ({"ledger_limit": -2}, "ledger_limit"),
        ({"queues": ()}, "queues"),
        ({"dashboard_port": 70000}, "dashboard_port"),
        ({"default_timeout": -1}, "default_timeout"),
    ],
)
def test_validacao_rejeita_configuracoes_invalidas(tmp_path: Path, kwargs: dict, trecho: str) -> None:
    """As invariantes da configuração são verificadas na construção."""
    with pytest.raises(ConfigError) as erro:
        Config(data_dir=tmp_path, **kwargs)

    assert trecho in str(erro.value)


def test_data_dir_vazio_e_rejeitado() -> None:
    """Um ``data_dir`` em branco não é aceito."""
    with pytest.raises(ConfigError) as erro:
        Config(data_dir="   ")
    assert "data_dir" in str(erro.value)


def test_overrides_tem_precedencia_e_rejeitam_desconhecidos(tmp_path: Path) -> None:
    """Overrides explícitos vencem o ambiente; chaves desconhecidas são erro."""
    base = Config(data_dir=tmp_path, worker_concurrency=2)

    alterada = base.with_overrides(worker_concurrency=6, log_level=None)

    assert alterada.worker_concurrency == 6
    assert alterada.log_level == base.log_level

    with pytest.raises(ConfigError):
        base.with_overrides(opcao_inexistente=1)
    with pytest.raises(ConfigError):
        Config.from_env(data_dir=tmp_path, opcao_inexistente=1)


def test_caminhos_derivados_e_ensure_dirs(tmp_path: Path) -> None:
    """As propriedades de caminho apontam para dentro do ``data_dir``."""
    config = Config(data_dir=tmp_path / "estado")

    assert config.journal_path.parent == config.data_dir
    assert config.snapshot_path.parent == config.data_dir
    assert config.lock_path.parent == config.data_dir
    assert not config.data_dir.exists()

    config.ensure_dirs()
    config.ensure_dirs()  # idempotente

    assert config.data_dir.is_dir()


def test_env_var_e_cache_de_configuracao(tmp_path: Path) -> None:
    """``env_var`` monta o nome da variável e ``get_config`` cacheia a instância."""
    assert env_var("lease_seconds") == "TASKFLOW_LEASE_SECONDS"

    primeira = get_config()
    assert get_config() is primeira


# ------------------------------------------------------------------ logging


def test_key_value_formatter_anexa_campos_extras() -> None:
    """Campos passados em ``extra`` viram ``chave=valor`` na linha de log."""
    formato = KeyValueFormatter("%(levelname)s %(message)s")
    registro = logging.LogRecord(
        name="taskflow.teste",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="task enfileirada",
        args=(),
        exc_info=None,
    )
    registro.task_id = "abc123"
    registro.priority = 7
    registro.mensagem = "com espaços"

    linha = formato.format(registro)

    assert "task enfileirada" in linha
    assert "task_id=abc123" in linha
    assert "priority=7" in linha
    assert "mensagem='com espaços'" in linha


def test_setup_logging_idempotente_e_rejeita_nivel_invalido() -> None:
    """Chamar várias vezes não duplica handler; nível inválido é erro claro."""
    raiz = logging.getLogger("taskflow")
    handlers_antes = list(raiz.handlers)
    try:
        setup_logging("DEBUG")
        setup_logging("DEBUG")
        depois = [h for h in raiz.handlers if getattr(h, "taskflow_handler", False)]

        assert len(depois) == 1
        assert raiz.level == logging.DEBUG
        assert raiz.propagate is False

        with pytest.raises(ConfigError):
            setup_logging("MUITO_BARULHENTO")
    finally:
        for handler in list(raiz.handlers):
            if handler not in handlers_antes:
                raiz.removeHandler(handler)
        raiz.setLevel(logging.NOTSET)


# ------------------------------------------------------------------ estados


@pytest.mark.parametrize(
    ("origem", "destino"),
    [
        (TaskState.PENDING, TaskState.RUNNING),
        (TaskState.RUNNING, TaskState.SUCCESS),
        (TaskState.RUNNING, TaskState.RETRY),
        (TaskState.RUNNING, TaskState.PENDING),
        (TaskState.RETRY, TaskState.RUNNING),
        (TaskState.DEAD, TaskState.PENDING),
        (TaskState.FAILED, TaskState.PENDING),
    ],
)
def test_transicoes_permitidas(origem: TaskState, destino: TaskState) -> None:
    """As transições do ciclo de vida são aceitas."""
    assert can_transition(origem, destino) is True
    validate_transition(origem, destino)


@pytest.mark.parametrize(
    ("origem", "destino"),
    [
        (TaskState.SUCCESS, TaskState.RUNNING),
        (TaskState.SUCCESS, TaskState.PENDING),
        (TaskState.PENDING, TaskState.SUCCESS),
        (TaskState.FAILED, TaskState.RUNNING),
        (TaskState.DEAD, TaskState.SUCCESS),
    ],
)
def test_transicoes_proibidas(origem: TaskState, destino: TaskState) -> None:
    """Transições inválidas levantam StateError listando as permitidas."""
    assert can_transition(origem, destino) is False

    with pytest.raises(StateError) as erro:
        validate_transition(origem, destino)

    mensagem = str(erro.value)
    assert origem.value in mensagem
    assert destino.value in mensagem
    assert "permitido" in mensagem


def test_estados_terminais_e_rotulos() -> None:
    """Estados finais são reconhecidos e descritos em português."""
    assert TERMINAL_STATES == {
        TaskState.SUCCESS,
        TaskState.FAILED,
        TaskState.DEAD,
        TaskState.CANCELLED,
    }
    assert is_terminal(TaskState.SUCCESS) is True
    assert is_terminal(TaskState.PENDING) is False
    assert is_terminal("dead") is True

    assert describe_state(TaskState.RUNNING) == "executando"
    assert describe_state(TaskState.DEAD) == "dead letter"
    assert describe_state(TaskState.CANCELLED) == "cancelada"
    assert TaskState.SUCCESS.is_terminal is True
    assert TaskState.RETRY.is_pending is True
    assert TaskState.FAILED.is_failure is True
    assert TaskState.CANCELLED.is_cancelled is True
    assert TaskState.CANCELLED.is_failure is False
    assert str(TaskState.SUCCESS) == "SUCCESS"


def test_coerce_de_estado() -> None:
    """Texto vira estado, ignorando caixa; texto inválido vira erro."""
    assert TaskState.coerce("success") is TaskState.SUCCESS
    assert TaskState.coerce(" Retry ") is TaskState.RETRY
    assert TaskState.coerce(TaskState.DEAD) is TaskState.DEAD

    with pytest.raises(StateError) as erro:
        TaskState.coerce("inventado")
    assert "PENDING" in str(erro.value)


# ------------------------------------------------------------------ event bus


async def test_subscriber_assincrono_recebe_eventos() -> None:
    """Um callback ``async def`` é aguardado durante a publicação."""
    bus = EventBus()
    recebidos: list[Event] = []

    async def assinante(evento: Event) -> None:
        """Assinante assíncrono de teste."""
        await asyncio.sleep(0)
        recebidos.append(evento)

    bus.subscribe(assinante)
    evento = Event(EventType.ENQUEUED, task_id="t1", queue="default", name="ok", timestamp=1.0)
    await bus.publish(evento)

    assert recebidos == [evento]
    assert bus.subscriber_count == 1
    assert bus.history() == [evento]


async def test_filtro_por_tipo_e_inscricao_com_nome() -> None:
    """Inscrições podem filtrar por tipo de evento."""
    bus = EventBus()
    vistos: list[str] = []
    inscricao = bus.subscribe(lambda e: vistos.append(e.type.value), event_type=EventType.SUCCESS, name="sucesso")

    await bus.publish(Event(EventType.ENQUEUED, "t1", "default", "ok", 1.0))
    await bus.publish(Event(EventType.SUCCESS, "t1", "default", "ok", 2.0))

    assert vistos == ["success"]
    assert inscricao.name == "sucesso"

    inscricao.unsubscribe()
    await bus.publish(Event(EventType.SUCCESS, "t1", "default", "ok", 3.0))
    assert vistos == ["success"]


async def test_excecao_do_assinante_nao_quebra_a_publicacao() -> None:
    """Um assinante que falha é registrado, mas os demais recebem o evento."""
    bus = EventBus()
    recebidos: list[str] = []

    def ruim(_: Event) -> None:
        """Assinante que sempre estoura."""
        raise RuntimeError("assinante quebrado")

    bus.subscribe(ruim, name="ruim")
    bus.subscribe(lambda e: recebidos.append(e.task_id), name="bom")

    await bus.publish(Event(EventType.SUCCESS, "t1", "default", "ok", 1.0))

    assert recebidos == ["t1"]


async def test_history_filtra_e_limita() -> None:
    """O histórico circular filtra por tipo e por task, respeitando o limite."""
    bus = EventBus(history=3)
    for indice in range(5):
        await bus.publish(Event(EventType.SUCCESS, f"t{indice}", "default", "ok", float(indice)))

    assert len(bus.history()) == 3  # deque(maxlen=3)
    assert [e.task_id for e in bus.history()] == ["t2", "t3", "t4"]
    assert [e.task_id for e in bus.history(task_id="t4")] == ["t4"]
    assert len(bus.history(event_type=EventType.ENQUEUED)) == 0
    assert len(bus.history(limit=1)) == 1


async def test_wait_for_encontra_evento_e_respeita_timeout() -> None:
    """``wait_for`` entrega o evento que casa e devolve ``None`` no timeout."""
    bus = EventBus()
    alvo = Event(EventType.SUCCESS, "t9", "default", "ok", 1.0)
    await bus.publish(alvo)

    achado = await bus.wait_for(lambda e: e.type is EventType.SUCCESS, timeout=0.2)
    assert achado is alvo

    assert await bus.wait_for(lambda e: e.task_id == "inexistente", timeout=0.1) is None

    async def publicar_depois() -> None:
        """Publica depois de um instante, para testar a espera ativa."""
        await asyncio.sleep(0.02)
        await bus.publish(Event(EventType.DEAD, "t10", "default", "ok", 2.0))

    tarefa = asyncio.create_task(publicar_depois())
    achado = await bus.wait_for(lambda e: e.type is EventType.DEAD, timeout=1.0)
    await tarefa
    assert achado is not None
    assert achado.task_id == "t10"


def test_event_to_dict_e_from_dict() -> None:
    """O evento sobrevive a uma ida e volta pelo dicionário JSON."""
    original = Event(EventType.FAILED, "t1", "alta", "ok", 12.5, {"will_retry": True, "retry_in": 1.5})

    dados = original.to_dict()
    reconstituido = Event.from_dict(dados)

    assert dados["type"] == "failed"
    assert dados["payload"] == {"will_retry": True, "retry_in": 1.5}
    assert reconstituido == original
    assert EventType.coerce("failed") is EventType.FAILED


# ------------------------------------------------------------------ leases e disco


async def test_renew_lease_renova_e_recusa_lease_errada(broker: Broker) -> None:
    """Só a lease corrente pode ser renovada, e a renovação estende o prazo."""
    await broker.submit("ok", "x")
    task = await broker.fetch("default", timeout=1.0)
    assert task is not None and task.lease is not None
    lease_id = task.lease.lease_id
    antes = task.lease.expires_at

    assert broker.renew_lease(task, lease_id, lease_seconds=30.0) is True
    assert task.lease is not None and task.lease.expires_at > antes

    assert broker.renew_lease(task, "lease-inventada") is False
    assert task.lease is not None and task.lease.expires_at > antes

    await broker.ack(task, None, lease_id=lease_id)
    assert broker.renew_lease(task, lease_id) is False  # já não está em execução


async def test_ack_sem_lease_e_recusado(broker: Broker) -> None:
    """Registrar resultado de uma task que não está em execução é erro."""
    await broker.submit("ok", "x")
    task = await broker.submit("ok", "y")
    assert task.lease is None

    with pytest.raises(StaleLeaseError):
        await broker.ack(task, "sem lease")


async def test_traceback_e_guardado_no_resultado(
    broker: Broker, registry: TaskRegistry, fast_policy: RetryPolicy
) -> None:
    """O traceback da exceção fica guardado no TaskResult da tentativa."""
    pool = WorkerPool(broker, registry, broker.config, fast_policy, queues=("demo",))
    task = await broker.submit("boom", queue="demo", max_retries=0)

    await pool.process_one(timeout=1.0)

    morta = broker.require(task.id)
    assert morta.state is TaskState.DEAD
    assert morta.result is not None
    assert morta.result.traceback is not None
    assert "Traceback (most recent call last)" in morta.result.traceback
    assert "RuntimeError" in morta.result.traceback
    assert "falha definitiva" in morta.result.traceback
    assert morta.result.worker_id is not None


async def test_task_em_eta_nao_e_renovavel_apos_reclaim(broker: Broker) -> None:
    """Depois de recolhida, a task não aceita mais renovação de lease."""
    await broker.submit("ok", "x")
    task = await broker.fetch("default", timeout=1.0)
    assert task is not None and task.lease is not None
    await broker.reclaim_expired(now=float("inf"))

    assert broker.renew_lease(task, "qualquer") is False


async def test_fsync_ligado_mantem_o_journal_consistente(
    data_dir: Path, config: Config, registry: TaskRegistry
) -> None:
    """Com ``fsync`` ligado o journal é escrito e relido corretamente."""
    config = config.with_overrides(fsync=True)
    broker = Broker(config, registry)
    await broker.start()
    try:
        task = await broker.submit("ok", "com-fsync")
        executando = await broker.fetch("default", timeout=1.0)
        assert executando is not None
        await broker.ack(executando, "fim")
    finally:
        await broker.stop()

    assert config.journal_path.exists()
    linhas = config.journal_path.read_text(encoding="utf-8").strip().splitlines()
    assert [linha.split('"op":"')[1].split('"')[0] for linha in linhas] == ["enqueue", "start", "ack"]

    reencontrado = Broker(config, registry)
    await reencontrado.start()
    try:
        assert reencontrado.require(task.id).state is TaskState.SUCCESS
    finally:
        await reencontrado.stop()


async def test_apply_dict_mantem_a_identidade_do_objeto(broker: Broker) -> None:
    """``apply_dict`` atualiza a mesma instância (o worker mantém a referência)."""
    await broker.submit("ok", "original")
    task = broker.list_tasks()[0]
    identidade = id(task)

    dados = task.to_dict()
    dados["args"] = ["alterado"]
    dados["state"] = TaskState.RUNNING.value
    task.apply_dict(dados)

    assert id(task) == identidade
    assert task.args == ["alterado"]
    assert task.state is TaskState.RUNNING
    assert broker.require(task.id) is task


def test_task_resumo_e_filtros(data_dir: Path) -> None:
    """``summary()`` devolve os campos usados pela CLI e pelo dashboard."""
    task = Task(id="abc123", name="ok", queue="alta", priority=4, args=[1], kwargs={"k": "v"})

    resumo = task.summary()

    assert resumo["id"] == "abc123"
    assert resumo["name"] == "ok"
    assert resumo["state"] == TaskState.PENDING.value
    assert resumo["worker_id"] is None
    assert resumo["duration_ms"] is None
    assert task.short_id == "abc123"
    assert task.is_ready is True