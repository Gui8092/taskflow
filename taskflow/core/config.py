"""Configuração do taskflow carregada de variáveis de ambiente ``TASKFLOW_*``.

A configuração é um :class:`dataclass` imutável. Todos os valores possuem
padrão sensato, de forma que o projeto funciona sem nenhuma variável definida.

Variáveis reconhecidas (todas opcionais)::

    TASKFLOW_DATA_DIR              diretório de estado (journal, snapshot, lock)
    TASKFLOW_QUEUES                filas consumidas pelos workers, separadas por vírgula
    TASKFLOW_DEFAULT_QUEUE         fila usada quando a task não informa uma
    TASKFLOW_WORKER_CONCURRENCY    número de corrotinas por pool
    TASKFLOW_DEFAULT_PRIORITY      prioridade padrão das tasks
    TASKFLOW_DEFAULT_MAX_RETRIES   número de retries padrão
    TASKFLOW_DEFAULT_TIMEOUT       timeout padrão em segundos (vazio = sem timeout)
    TASKFLOW_LEASE_SECONDS         duração da lease de execução
    TASKFLOW_RECLAIM_INTERVAL      intervalo do varredor de leases expiradas
    TASKFLOW_RECOVER_RUNNING_ON_START  devolve tasks RUNNING ao estado pendente no start
    TASKFLOW_RETRY_BASE            base do backoff exponencial
    TASKFLOW_RETRY_CAP             teto do backoff
    TASKFLOW_RETRY_JITTER          aplica jitter ao backoff
    TASKFLOW_JOURNAL_FILENAME      nome do journal append-only
    TASKFLOW_SNAPSHOT_FILENAME     nome do snapshot compactado
    TASKFLOW_LOCK_FILENAME         nome do arquivo de trava de escrita
    TASKFLOW_LOCK                  liga/desliga a trava de escrita exclusiva
    TASKFLOW_COMPACT_LINES         linhas de journal que disparam compaction
    TASKFLOW_LEDGER_LIMIT          tasks terminais mantidas no histórico
    TASKFLOW_FSYNC                 faz fsync a cada append no journal
    TASKFLOW_DASHBOARD_HOST        host do dashboard
    TASKFLOW_DASHBOARD_PORT        porta do dashboard
    TASKFLOW_DASHBOARD_WORKERS     workers embutidos no dashboard
    TASKFLOW_MODULES               módulos importados para popular o registro de tasks
    TASKFLOW_LOG_LEVEL             nível de log (DEBUG/INFO/WARNING/ERROR)
    TASKFLOW_MONITOR_INTERVAL      intervalo de redesenho do ``monitor``
    TASKFLOW_MONITOR_ROWS          linhas exibidas pelo ``monitor``
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any, Callable, Final, Mapping

ENV_PREFIX: Final[str] = "TASKFLOW_"

#: Atributos padrão de um :class:`logging.LogRecord` que nunca devem virar campos.
_RESERVED_LOG_KEYS: Final[frozenset[str]] = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


class ConfigError(ValueError):
    """Valor de configuração inválido, com mensagem apontando o problema."""


def env_var(name: str) -> str:
    """Devolve o nome da variável de ambiente correspondente a um campo de :class:`Config`."""
    return f"{ENV_PREFIX}{name.upper()}"


def _parse_bool(raw: str, *, name: str) -> bool:
    """Converte texto em ``bool`` aceitando ``true/false``, ``1/0``, ``yes/no``, ``on/off``."""
    lowered = raw.strip().lower()
    if lowered in {"1", "true", "yes", "y", "on", "sim"}:
        return True
    if lowered in {"0", "false", "no", "n", "off", "nao"}:
        return False
    raise ConfigError(f"{env_var(name)} inválido: {raw!r} (esperado true ou false)")


def _parse_int(raw: str, *, name: str) -> int:
    """Converte texto em ``int``, falhando com mensagem explícita."""
    try:
        return int(raw.strip())
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{env_var(name)} inválido: {raw!r} (esperado inteiro)") from exc


def _parse_float(raw: str, *, name: str) -> float:
    """Converte texto em ``float``, falhando com mensagem explícita."""
    try:
        return float(raw.strip())
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{env_var(name)} inválido: {raw!r} (esperado número)") from exc


def _parse_optional_float(raw: str, *, name: str) -> float | None:
    """Converte texto em ``float | None``; texto vazio ou ``none`` significam "sem timeout"."""
    cleaned = raw.strip()
    if not cleaned or cleaned.lower() in {"none", "null", "off"}:
        return None
    return _parse_float(cleaned, name=name)


def _parse_str_tuple(raw: str) -> tuple[str, ...]:
    """Converte texto separado por vírgula em tupla, removendo espaços e vazios."""
    parts = tuple(part.strip() for part in raw.split(",") if part.strip())
    return parts


def _parse_path(raw: str) -> Path:
    """Converte texto em :class:`pathlib.Path` expandindo ``~``."""
    return Path(raw.strip()).expanduser()


_COERCERS: Final[Mapping[str, Callable[[str], Any]]] = {
    "data_dir": _parse_path,
    "queues": _parse_str_tuple,
    "default_queue": str,
    "worker_concurrency": _parse_int,
    "default_priority": _parse_int,
    "default_max_retries": _parse_int,
    "default_timeout": _parse_optional_float,
    "lease_seconds": _parse_float,
    "reclaim_interval": _parse_float,
    "recover_running_on_start": _parse_bool,
    "retry_base": _parse_float,
    "retry_cap": _parse_float,
    "retry_jitter": _parse_bool,
    "journal_filename": str,
    "snapshot_filename": str,
    "lock_filename": str,
    "lock_enabled": _parse_bool,
    "compact_lines": _parse_int,
    "ledger_limit": _parse_int,
    "fsync": _parse_bool,
    "dashboard_host": str,
    "dashboard_port": _parse_int,
    "dashboard_workers": _parse_int,
    "modules": _parse_str_tuple,
    "log_level": str,
    "monitor_interval": _parse_float,
    "monitor_rows": _parse_int,
}


@dataclass(frozen=True, slots=True)
class Config:
    """Configuração imutável do taskflow.

    Todos os campos possuem padrão; use :meth:`from_env` para mesclar variáveis
    de ambiente e :meth:`with_overrides` para derivar uma nova instância.
    """

    data_dir: Path = Path(".taskflow")
    queues: tuple[str, ...] = ("default",)
    default_queue: str = "default"
    worker_concurrency: int = 4
    default_priority: int = 0
    default_max_retries: int = 3
    default_timeout: float | None = None
    lease_seconds: float = 60.0
    reclaim_interval: float = 5.0
    recover_running_on_start: bool = True
    retry_base: float = 0.5
    retry_cap: float = 30.0
    retry_jitter: bool = True
    journal_filename: str = "journal.jsonl"
    snapshot_filename: str = "snapshot.json"
    lock_filename: str = "broker.lock"
    lock_enabled: bool = True
    compact_lines: int = 500
    ledger_limit: int = 1000
    fsync: bool = False
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8000
    dashboard_workers: int = 0
    modules: tuple[str, ...] = ()
    log_level: str = "INFO"
    monitor_interval: float = 0.25
    monitor_rows: int = 15

    def __post_init__(self) -> None:
        """Normaliza os campos recebidos como texto e valida as invariantes."""
        object.__setattr__(self, "data_dir", Path(str(self.data_dir).strip()).expanduser())
        object.__setattr__(self, "queues", tuple(self.queues))
        object.__setattr__(self, "modules", tuple(self.modules))
        self.validate()

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        **overrides: Any,
    ) -> Config:
        """Cria a configuração lendo variáveis ``TASKFLOW_*`` e aplicando *overrides*.

        Args:
            environ: Mapeamento de variáveis (padrão: :data:`os.environ`).
            **overrides: Valores explícitos que têm precedência sobre o ambiente.
                Chaves iguais a ``None`` são ignoradas.

        Returns:
            Uma nova instância de :class:`Config` já validada.

        Raises:
            ConfigError: Se algum valor presente no ambiente não puder ser convertido.
        """
        env: Mapping[str, str] = os.environ if environ is None else environ
        values: dict[str, Any] = {}
        known = {field.name for field in fields(cls)}
        for name in known:
            raw = env.get(env_var(name))
            if raw is None:
                continue
            coercer = _COERCERS.get(name)
            values[name] = coercer(raw) if coercer is not None else raw
        for name, value in overrides.items():
            if name not in known:
                raise ConfigError(f"opção de configuração desconhecida: {name!r}")
            if value is None:
                continue
            values[name] = value
        return cls(**values)

    def with_overrides(self, **overrides: Any) -> Config:
        """Devolve uma cópia com os campos informados substituídos (``None`` é ignorado)."""
        known = {field.name for field in fields(self)}
        for name in overrides:
            if name not in known:
                raise ConfigError(f"opção de configuração desconhecida: {name!r}")
        clean = {name: value for name, value in overrides.items() if value is not None}
        return replace(self, **clean)

    def validate(self) -> None:
        """Valida invariantes básicas, levantando :class:`ConfigError` quando necessário."""
        if not str(self.data_dir):
            raise ConfigError("data_dir não pode ser vazio")
        if self.worker_concurrency < 1:
            raise ConfigError(f"worker_concurrency deve ser >= 1 (recebido {self.worker_concurrency})")
        if self.default_max_retries < 0:
            raise ConfigError(f"default_max_retries deve ser >= 0 (recebido {self.default_max_retries})")
        if self.lease_seconds <= 0:
            raise ConfigError(f"lease_seconds deve ser > 0 (recebido {self.lease_seconds})")
        if self.reclaim_interval <= 0:
            raise ConfigError(f"reclaim_interval deve ser > 0 (recebido {self.reclaim_interval})")
        if self.retry_base < 0:
            raise ConfigError(f"retry_base deve ser >= 0 (recebido {self.retry_base})")
        if self.retry_cap < self.retry_base:
            raise ConfigError(f"retry_cap ({self.retry_cap}) não pode ser menor que retry_base ({self.retry_base})")
        if self.compact_lines < 0:
            raise ConfigError(f"compact_lines deve ser >= 0 (recebido {self.compact_lines})")
        if self.ledger_limit < 0:
            raise ConfigError(f"ledger_limit deve ser >= 0 (recebido {self.ledger_limit})")
        if not 1 <= self.dashboard_port <= 65535:
            raise ConfigError(f"dashboard_port fora da faixa válida (recebido {self.dashboard_port})")
        if not self.queues:
            raise ConfigError("queues não pode ser vazio")
        if self.default_timeout is not None and self.default_timeout <= 0:
            raise ConfigError(f"default_timeout deve ser > 0 ou None (recebido {self.default_timeout})")

    @property
    def journal_path(self) -> Path:
        """Caminho absoluto do journal append-only."""
        return self.data_dir / self.journal_filename

    @property
    def snapshot_path(self) -> Path:
        """Caminho absoluto do snapshot compactado."""
        return self.data_dir / self.snapshot_filename

    @property
    def lock_path(self) -> Path:
        """Caminho absoluto do arquivo de trava de escrita."""
        return self.data_dir / self.lock_filename

    def ensure_dirs(self) -> None:
        """Cria o diretório de estado se ainda não existir."""
        self.data_dir.mkdir(parents=True, exist_ok=True)


_CONFIG: Config | None = None


def get_config(refresh: bool = False) -> Config:
    """Devolve a configuração do processo (cacheada), opcionalmente relendo o ambiente."""
    global _CONFIG
    if refresh or _CONFIG is None:
        _CONFIG = Config.from_env()
    return _CONFIG


def set_config(config: Config | None) -> Config | None:
    """Substitui (ou limpa com ``None``) a configuração cacheada do processo."""
    global _CONFIG
    _CONFIG = config
    return _CONFIG


def _format_field(value: Any) -> str:
    """Formata um valor de campo estruturado de log de forma compacta."""
    if isinstance(value, str):
        return value if value and " " not in value else repr(value)
    return repr(value)


class KeyValueFormatter(logging.Formatter):
    """Formatter que anexa ao log os campos extras passados via ``extra={...}``."""

    def format(self, record: logging.LogRecord) -> str:
        """Formata o registro e acrescenta ``campo=valor`` para cada chave extra."""
        base = super().format(record)
        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _RESERVED_LOG_KEYS and not key.startswith("_")
        }
        if not extras:
            return base
        rendered = " ".join(f"{key}={_format_field(extras[key])}" for key in sorted(extras))
        return f"{base} {rendered}"


_HANDLER_TAG: Final[str] = "taskflow_handler"


def setup_logging(level: str | None = None, *, stream: Any = None, force: bool = False) -> logging.Logger:
    """Configura o logging estruturado do taskflow e devolve o logger raiz do pacote.

    Idempotente: chamar várias vezes não duplica handlers.

    Args:
        level: Nível desejado (ex.: ``"DEBUG"``). Padrão: ``TASKFLOW_LOG_LEVEL``.
        stream: Stream de saída (padrão: :data:`sys.stderr`).
        force: Reconfigura o handler existente em vez de preservá-lo.

    Returns:
        O logger ``taskflow``.
    """
    resolved = (level or os.environ.get(env_var("log_level")) or "INFO").upper()
    numeric = getattr(logging, resolved, None)
    if not isinstance(numeric, int):
        raise ConfigError(f"nível de log inválido: {resolved!r}")
    root = logging.getLogger("taskflow")
    root.setLevel(numeric)
    for handler in list(root.handlers):
        if getattr(handler, _HANDLER_TAG, False):
            if not force:
                continue
            root.removeHandler(handler)
            handler.close()
    if not any(getattr(handler, _HANDLER_TAG, False) for handler in root.handlers):
        handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
        handler.setFormatter(
            KeyValueFormatter("%(asctime)s %(levelname)-7s %(name)-22s %(message)s", datefmt="%H:%M:%S")
        )
        setattr(handler, _HANDLER_TAG, True)
        root.addHandler(handler)
    root.propagate = False
    return root