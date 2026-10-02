"""Tasks de demonstração, usadas pelos testes da CLI e por ``--modules``.

Este módulo existe para mostrar (e testar) como um worker em outro processo
descobre as tasks: basta apontar ``TASKFLOW_MODULES=demo_tasks`` ou passar
``--modules demo_tasks``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from taskflow import task

#: Contador de falhas acumulado da task ``demo.flaky``.
_FALHAS: dict[str, int] = {"flaky": 0}


@task(name="demo.ok", queue="default", tags={"origem": "demo_tasks"})
def ok(valor: str = "mundo") -> str:
    """Devolve uma string de confirmação."""
    return f"demo ok: {valor}"


@task(name="demo.echo", queue="default")
async def echo(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """Devolve os argumentos recebidos."""
    await asyncio.sleep(0)
    return {"args": list(args), "kwargs": kwargs}


@task(name="demo.flaky", queue="demo", max_retries=5)
def flaky(falhar_ate: int = 2) -> str:
    """Falha ``falhar_ate`` vezes e depois acerta."""
    _FALHAS["flaky"] += 1
    if _FALHAS["flaky"] <= falhar_ate:
        raise RuntimeError(f"falha número {_FALHAS['flaky']}")
    return f"acertou na tentativa {_FALHAS['flaky']}"


@task(name="demo.boom", queue="demo", max_retries=1)
def boom() -> None:
    """Falha sempre, servindo para provar a dead letter queue."""
    raise RuntimeError("falha definitiva")


@task(name="demo.lenta", queue="demo", timeout=30.0)
async def lenta(segundos: float = 5.0) -> str:
    """Dorme alguns segundos (útil para observar execução e timeout)."""
    await asyncio.sleep(segundos)
    return f"demorei {segundos}s"


def reset() -> None:
    """Zera o contador de falhas da task ``demo.flaky``."""
    _FALHAS["flaky"] = 0