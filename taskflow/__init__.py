"""taskflow — fila de tarefas distribuída ("mini-Celery") implementada do zero.

Uso mínimo::

    from taskflow import task
    from taskflow.core import Broker, Config
    from taskflow.worker import WorkerPool

    @task(name="send_email", queue="emails")
    def send_email(destino: str) -> str:
        return f"enviado para {destino}"

    async def main() -> None:
        broker = Broker(Config(), registry)
        await broker.start()
        await broker.submit("send_email", "ana@exemplo.com")
        pool = WorkerPool(broker, registry)
        await pool.process_one()
        await broker.stop()

O pacote não depende de Celery, Redis, RQ ou Dramatiq: broker, persistência,
event bus, workers, cron, DLQ, CLI e dashboard são código próprio.
"""

from taskflow.core.broker import Broker, BrokerStats, QueueStats
from taskflow.core.config import Config, get_config, setup_logging
from taskflow.core.events import Event, EventBus, EventType
from taskflow.core.registry import RegisteredTask, TaskRegistry, build_registry, registry, task
from taskflow.core.serialization import SerializationError
from taskflow.core.states import TaskState
from taskflow.core.task import Task, TaskResult
from taskflow.scheduler.cron import CronError, CronExpression
from taskflow.scheduler.scheduler import Schedule, Scheduler
from taskflow.worker.deadletter import DeadLetterQueue
from taskflow.worker.pool import Worker, WorkerPool
from taskflow.worker.retry import RetryPolicy

__version__ = "1.0.0"

__all__ = [
    "Broker",
    "BrokerStats",
    "Config",
    "CronError",
    "CronExpression",
    "DeadLetterQueue",
    "Event",
    "EventBus",
    "EventType",
    "QueueStats",
    "RegisteredTask",
    "RetryPolicy",
    "Schedule",
    "Scheduler",
    "SerializationError",
    "Task",
    "TaskRegistry",
    "TaskResult",
    "TaskState",
    "Worker",
    "WorkerPool",
    "__version__",
    "build_registry",
    "get_config",
    "registry",
    "setup_logging",
    "task",
]