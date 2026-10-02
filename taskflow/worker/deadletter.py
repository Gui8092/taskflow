"""Dead letter queue: leitura, requeue e limpeza das tasks que falharam de vez.

A DLQ é uma *visão* sobre o ledger do broker: toda task em ``DEAD`` (retries
esgotados) ou ``FAILED`` (falha permanente) está nela. Reenfileirar zera
``attempts`` por padrão, o que dá um orçamento novo de retries.
"""

from __future__ import annotations

import logging
from typing import Final

from taskflow.core.broker import Broker
from taskflow.core.states import FAILURE_STATES, TaskState
from taskflow.core.task import Task, TaskFilter

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.dlq")


class DeadLetterQueue:
    """Operações sobre as tasks que não podem mais ser executadas sozinhas."""

    def __init__(self, broker: Broker) -> None:
        """Liga a DLQ a um broker (somente leitura para listar, escrita para requeue)."""
        self.broker = broker

    def list(self, *, queue: str | None = None, limit: int = 100) -> list[Task]:
        """Lista as tasks da DLQ, da mais recente para a mais antiga.

        Args:
            queue: Filtra por fila.
            limit: Quantidade máxima devolvida (``0`` devolve todas).

        Returns:
            As tasks em ``DEAD``/``FAILED``.
        """
        selected = [
            task for task in self.broker.list_tasks(TaskFilter(limit=0)) if task.state in FAILURE_STATES
        ]
        if queue is not None:
            selected = [task for task in selected if task.queue == queue]
        selected.sort(key=lambda task: (-(task.finished_at or task.created_at), task.id))
        if limit and limit > 0:
            return selected[:limit]
        return selected

    def get(self, task_id: str) -> Task | None:
        """Devolve uma task da DLQ pelo id, ou ``None`` se ela não existir/estiver nela."""
        task = self.broker.get(task_id)
        if task is None or task.state not in FAILURE_STATES:
            return None
        return task

    def count(self, *, queue: str | None = None) -> int:
        """Quantas tasks estão na DLQ."""
        if queue is None:
            return sum(1 for task in self.broker.list_tasks(TaskFilter(limit=0)) if task.state in FAILURE_STATES)
        return sum(1 for task in self.list(queue=queue, limit=0) if task.state in FAILURE_STATES)

    def reasons(self) -> dict[str, int]:
        """Contagem da DLQ por estado (``DEAD`` = retries esgotados, ``FAILED`` = permanente)."""
        counter = {state.value: 0 for state in TaskState}
        for task in self.list(limit=0):
            counter[task.state.value] += 1
        return counter

    async def requeue(
        self,
        task_id: str,
        *,
        queue: str | None = None,
        priority: int | None = None,
        reset_attempts: bool = True,
        delay: float | None = None,
    ) -> Task:
        """Reenfileira uma task da DLQ.

        Args:
            task_id: Id da task.
            queue: Fila de destino (padrão: a fila original).
            priority: Nova prioridade.
            reset_attempts: Zera ``attempts`` para um orçamento novo de retries.
            delay: Segundos até a task ficar pronta.

        Returns:
            A task reenfileirada.

        Raises:
            TaskNotFoundError: Se a task não existir.
            BrokerError: Se a task não estiver em estado terminal de falha.
        """
        task = await self.broker.requeue(
            task_id,
            queue=queue,
            priority=priority,
            reset_attempts=reset_attempts,
            delay=delay,
        )
        LOGGER.info(
            "task saiu da DLQ",
            extra={
                "task_id": task.short_id,
                "task_name": task.name,
                "fila": task.queue,
                "attempts_resetados": reset_attempts,
            },
        )
        return task

    async def requeue_all(
        self,
        *,
        queue: str | None = None,
        limit: int | None = None,
        reset_attempts: bool = True,
        delay: float | None = None,
    ) -> list[Task]:
        """Reenfileira todas (ou as *limit* mais recentes) tasks da DLQ.

        Returns:
            As tasks reenfileiradas.
        """
        targets = self.list(queue=queue, limit=limit or 0)
        requeued: list[Task] = []
        for task in targets:
            requeued.append(
                await self.requeue(
                    task.id,
                    queue=queue,
                    reset_attempts=reset_attempts,
                    delay=delay,
                )
            )
        LOGGER.info("lote requeueado", extra={"quantidade": len(requeued)})
        return requeued

    async def purge(self, *, queue: str | None = None) -> int:
        """Remove definitivamente as tasks da DLQ do ledger.

        Returns:
            Quantas tasks foram removidas.
        """
        targets = self.list(queue=queue, limit=0)
        removed = 0
        for task in targets:
            if await self.broker.remove(task.id):
                removed += 1
        LOGGER.info("DLQ limpa", extra={"quantidade": removed, "fila": queue or "*"})
        return removed