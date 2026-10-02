"""Pool de workers assíncronos que executa as tasks enfileiradas.

Cada worker é uma corrotina que faz o ciclo ``fetch → run → ack/nack``. Como não
existe *prefetch*, uma task é buscada somente quando o worker está livre para
executá-la, o que dá a garantia de que **nenhuma task é executada por dois workers
ao mesmo tempo**. A proteção contra execução tardia (worker lento demais, lease
vencida e task reexecutada por outro) vem do *fencing token* da lease, validado
pelo broker em :meth:`~taskflow.core.broker.Broker.ack` e
:meth:`~taskflow.core.broker.Broker.nack`.
"""

from __future__ import annotations

import asyncio
import logging
import time
import traceback
from typing import Any, Final, Iterable, Sequence

from taskflow.core.broker import Broker
from taskflow.core.config import Config
from taskflow.core.registry import TaskRegistry
from taskflow.core.task import Task
from taskflow.worker.retry import RetryPolicy

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.worker")

#: Intervalo padrão de espera do ``fetch`` quando não há trabalho pronto.
DEFAULT_FETCH_TIMEOUT: Final[float] = 0.25


class Worker:
    """Uma corrotina que consome tasks de uma fila."""

    def __init__(
        self,
        worker_id: str,
        broker: Broker,
        registry: TaskRegistry,
        config: Config,
        policy: RetryPolicy,
        queue: str,
        *,
        fetch_timeout: float = DEFAULT_FETCH_TIMEOUT,
        heartbeat: bool = True,
    ) -> None:
        """Cria o worker ligado a uma fila específica."""
        self.worker_id = worker_id
        self.queue = queue
        self.broker = broker
        self.registry = registry
        self.config = config
        self.policy = policy
        self.fetch_timeout = fetch_timeout
        self.heartbeat = heartbeat
        self.processed = 0
        self.failures = 0
        self._stopping = False
        self._current: Task | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

    @property
    def current(self) -> Task | None:
        """Task em execução neste momento (``None`` se o worker está ocioso)."""
        return self._current

    def stop(self) -> None:
        """Pede o encerramento do loop (a task em andamento termina normalmente)."""
        self._stopping = True

    async def run(self) -> None:
        """Loop principal: busca, executa e repete até receber :meth:`stop`."""
        LOGGER.info("worker iniciado", extra={"worker": self.worker_id, "queue": self.queue})
        try:
            while not self._stopping:
                try:
                    task = await self.broker.fetch(
                        self.queue, worker_id=self.worker_id, timeout=self.fetch_timeout
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception(
                        "falha ao buscar task", extra={"worker": self.worker_id, "queue": self.queue}
                    )
                    await asyncio.sleep(self.fetch_timeout)
                    continue
                if task is None:
                    continue
                self._current = task
                try:
                    await self.process(task)
                finally:
                    self._current = None
        finally:
            LOGGER.info("worker encerrado", extra={"worker": self.worker_id, "queue": self.queue})

    async def process(self, task: Task) -> Task:
        """Executa uma task já buscada e registra o resultado no broker.

        O timeout usa :func:`asyncio.timeout` (equivalente a ``asyncio.wait_for``)
        e conta como falha: a task entra em retry ou na dead letter queue.
        """
        lease_id = task.lease.lease_id if task.lease else ""
        registered = self.registry.find(task.name)
        if registered is None:
            self.failures += 1
            return await self.broker.nack(
                task,
                f"task desconhecida: {task.name!r} (não está registrada neste processo)",
                permanent=True,
                lease_id=lease_id,
            )
        timeout = task.timeout if task.timeout is not None else self.config.default_timeout
        beat = self._start_heartbeat(task, lease_id)
        started = time.monotonic()
        try:
            try:
                async with asyncio.timeout(timeout):
                    value = await registered.call(*task.args, **task.kwargs)
            except TimeoutError:
                self.failures += 1
                LOGGER.warning(
                    "task excedeu o timeout",
                    extra={"task_id": task.short_id, "task_name": task.name, "timeout": timeout},
                )
                return await self.broker.nack(
                    task,
                    f"timeout após {timeout}s",
                    traceback_text=None,
                    retry_delay=self.policy.delay_for(task.attempts),
                    lease_id=lease_id,
                )
            except Exception as exc:
                self.failures += 1
                message = f"{type(exc).__name__}: {exc}"
                LOGGER.warning(
                    "task levantou exceção",
                    extra={
                        "task_id": task.short_id,
                        "task_name": task.name,
                        "attempt": task.attempts,
                        "erro": message,
                    },
                )
                return await self.broker.nack(
                    task,
                    message,
                    traceback_text="".join(traceback.format_exception(exc)),
                    retry_delay=self.policy.delay_for(task.attempts),
                    lease_id=lease_id,
                )
            self.processed += 1
            LOGGER.debug(
                "task executada",
                extra={
                    "task_id": task.short_id,
                    "task_name": task.name,
                    "duracao_ms": round((time.monotonic() - started) * 1000, 3),
                },
            )
            return await self.broker.ack(task, value, lease_id=lease_id)
        finally:
            await self._stop_heartbeat(beat)

    def _start_heartbeat(self, task: Task, lease_id: str) -> asyncio.Task[None] | None:
        """Inicia a rotina que prorroga a lease enquanto a task executa."""
        if not self.heartbeat:
            return None
        interval = max(0.05, self.config.lease_seconds / 3)

        async def beat() -> None:
            """Prorroga a lease periodicamente até a task terminar."""
            while True:
                await asyncio.sleep(interval)
                if not self.broker.renew_lease(task, lease_id):
                    return

        handle = asyncio.create_task(beat(), name=f"taskflow:heartbeat:{self.worker_id}")
        self._heartbeat_task = handle
        return handle

    async def _stop_heartbeat(self, handle: asyncio.Task[None] | None) -> None:
        """Cancela e aguarda a rotina de heartbeat."""
        self._heartbeat_task = None
        if handle is None:
            return
        handle.cancel()
        try:
            await handle
        except asyncio.CancelledError:
            return


class WorkerPool:
    """Conjunto de workers que compartilham um broker."""

    def __init__(
        self,
        broker: Broker,
        registry: TaskRegistry,
        config: Config | None = None,
        policy: RetryPolicy | None = None,
        *,
        concurrency: int | None = None,
        queues: Sequence[str] | None = None,
        worker_prefix: str = "worker",
        heartbeat: bool = True,
    ) -> None:
        """Cria o pool.

        Args:
            broker: Broker de onde as tasks saem.
            registry: Registro usado para resolver os nomes das tasks.
            config: Configuração (padrão: a do broker).
            policy: Política de retry (padrão: derivada da configuração).
            concurrency: Número de workers (padrão: ``config.worker_concurrency``).
            queues: Filas a consumir, em round-robin (padrão: ``config.queues``).
            worker_prefix: Prefixo dos ids ``worker-1``, ``worker-2``…
            heartbeat: Prorrogar a lease de tasks longas.
        """
        self.broker = broker
        self.registry = registry
        self.config = config if config is not None else broker.config
        self.policy = policy if policy is not None else RetryPolicy.from_config(self.config)
        self.concurrency = max(1, int(concurrency or self.config.worker_concurrency))
        self.queues: tuple[str, ...] = tuple(queues or self.config.queues or (self.config.default_queue,))
        self.workers: list[Worker] = [
            Worker(
                worker_id=f"{worker_prefix}-{index + 1}",
                broker=broker,
                registry=registry,
                config=self.config,
                policy=self.policy,
                queue=self.queues[index % len(self.queues)],
                heartbeat=heartbeat,
            )
            for index in range(self.concurrency)
        ]
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = False

    @property
    def inflight(self) -> int:
        """Quantas tasks estão em execução agora."""
        return sum(1 for worker in self.workers if worker.current is not None)

    @property
    def processed(self) -> int:
        """Total de tasks concluídas com sucesso por este pool."""
        return sum(worker.processed for worker in self.workers)

    @property
    def failures(self) -> int:
        """Total de execuções que falharam neste pool."""
        return sum(worker.failures for worker in self.workers)

    @property
    def started(self) -> bool:
        """``True`` se o pool está rodando."""
        return bool(self._tasks)

    async def start(self) -> None:
        """Sobe uma corrotina por worker."""
        if self._tasks:
            return
        self._stopping = False
        self._tasks = [
            asyncio.create_task(worker.run(), name=f"taskflow:{worker.worker_id}")
            for worker in self.workers
        ]
        LOGGER.info(
            "pool de workers iniciado",
            extra={
                "workers": self.concurrency,
                "filas": ",".join(self.queues),
                "politica": self.policy.describe(),
            },
        )

    async def drain(self, timeout: float | None = None) -> bool:
        """Espera a fila esvaziar (sem tasks prontas e sem nada em execução).

        Args:
            timeout: Tempo máximo de espera em segundos (``None`` = sem limite).

        Returns:
            ``True`` se a fila esvaziou dentro do tempo, ``False`` caso contrário.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if not self.broker.has_pending_work():
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.01)

    async def stop(self, *, drain: bool = True, timeout: float | None = 30.0) -> None:
        """Encerra o pool, opcionalmente esperando a fila esvaziar antes.

        Args:
            drain: Espera as tasks em andamento terminarem antes de cancelar.
            timeout: Tempo máximo para o drain e para o cancelamento.
        """
        self._stopping = True
        for worker in self.workers:
            worker.stop()
        if drain and self._tasks:
            await self.drain(timeout)
        if not self._tasks:
            return
        done, pending = await asyncio.wait(self._tasks, timeout=timeout)
        for handle in pending:
            handle.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._tasks = []
        LOGGER.info(
            "pool de workers encerrado",
            extra={"workers": self.concurrency, "processadas": self.processed, "falhas": self.failures},
        )

    async def process_one(self, *, worker_id: str | None = None, timeout: float | None = 5.0) -> Task | None:
        """Executa uma única task da primeira fila, sem subir o pool.

        Usa o primeiro worker do próprio pool, de modo que os contadores de
        ``processed``/``failures`` sejam atualizados normalmente.

        Returns:
            A task processada ou ``None`` se nada apareceu no tempo informado.
        """
        worker = self.workers[0]
        if worker_id:
            worker.worker_id = worker_id
        task = await self.broker.fetch(worker.queue, worker_id=worker.worker_id, timeout=timeout)
        if task is None:
            return None
        await worker.process(task)
        return task

    def stats(self) -> dict[str, Any]:
        """Fotografia do pool em forma de dicionário."""
        return {
            "concurrency": self.concurrency,
            "queues": list(self.queues),
            "inflight": self.inflight,
            "processed": self.processed,
            "failures": self.failures,
            "started": self.started,
            "workers": [
                {
                    "id": worker.worker_id,
                    "queue": worker.queue,
                    "processed": worker.processed,
                    "failures": worker.failures,
                    "current": worker.current.short_id if worker.current else None,
                }
                for worker in self.workers
            ],
            "retry_policy": {
                "base": self.policy.base,
                "cap": self.policy.cap,
                "jitter": self.policy.jitter,
            },
        }

    def describe(self) -> str:
        """Resumo de uma linha do pool, para a CLI."""
        return (
            f"{self.concurrency} worker(s) em {', '.join(self.queues)} — {self.policy.describe()}"
        )


def worker_queues(config: Config, queues: Iterable[str] | None = None) -> tuple[str, ...]:
    """Normaliza a lista de filas informada na CLI.

    Args:
        config: Configuração com as filas padrão.
        queues: Filas explicitadas pelo usuário (podem ser vazias ou ter repetições).

    Returns:
        Tupla de filas sem repetição; cai para a configuração quando vazia.
    """
    if queues:
        return tuple(dict.fromkeys(str(name).strip() for name in queues if str(name).strip()))
    return tuple(config.queues) or (config.default_queue,)


__all__ = ["DEFAULT_FETCH_TIMEOUT", "Worker", "WorkerPool", "worker_queues"]