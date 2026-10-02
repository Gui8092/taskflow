"""Registro global de tasks e o decorador :func:`task`.

Um worker em outro processo precisa resolver o *nome* da task para uma função
Python. Isso é feito importando os módulos indicados em ``TASKFLOW_MODULES``,
que por sua vez registram suas funções através do decorador :func:`task`.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Final, Iterable, Mapping, Sequence

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.registry")

#: Tipo da função exposta pelo decorador :func:`task`.
TaskFunction = Callable[..., Any]


class RegistryError(RuntimeError):
    """Problema genérico do registro de tasks."""


class UnknownTaskError(RegistryError, KeyError):
    """A task solicitada não está registrada no processo."""

    def __str__(self) -> str:
        """Mensagem legível (sem as aspas extras de ``KeyError``)."""
        return self.args[0] if self.args else "task desconhecida"


class DuplicateTaskError(RegistryError):
    """Já existe uma task registrada com o mesmo nome."""


class ModuleLoadError(RegistryError):
    """Um módulo de ``TASKFLOW_MODULES`` não pôde ser importado."""


@dataclass(frozen=True, slots=True)
class RegisteredTask:
    """Task registrada: metadados + função Python:callável."""

    name: str
    func: TaskFunction
    queue: str = "default"
    priority: int = 0
    max_retries: int = 3
    timeout: float | None = None
    tags: Mapping[str, str] = field(default_factory=dict)
    is_async: bool = False
    signature: str = ""

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Chama a função original diretamente (o objeto também é uma callable)."""
        return self.func(*args, **kwargs)

    async def call(self, *args: Any, **kwargs: Any) -> Any:
        """Executa a task, aguardando o resultado se a função for assíncrona."""
        outcome = self.func(*args, **kwargs)
        if inspect.isawaitable(outcome):
            return await outcome
        return outcome

    def to_dict(self) -> dict[str, Any]:
        """Metadados da task em forma JSON-safe (usados pela CLI e pelo dashboard)."""
        return {
            "name": self.name,
            "queue": self.queue,
            "priority": self.priority,
            "max_retries": self.max_retries,
            "timeout": self.timeout,
            "tags": dict(self.tags),
            "is_async": self.is_async,
            "signature": self.signature,
            "module": getattr(self.func, "__module__", ""),
            "doc": (inspect.getdoc(self.func) or "").split("\n")[0],
        }


class TaskRegistry:
    """Mapa ``nome → RegisteredTask`` consultável de forma síncrona."""

    def __init__(self) -> None:
        """Cria um registro vazio."""
        self._tasks: dict[str, RegisteredTask] = {}

    def register(self, registered: RegisteredTask, *, replace: bool = False) -> RegisteredTask:
        """Adiciona uma task ao registro.

        Raises:
            DuplicateTaskError: Se o nome já existir e ``replace`` for ``False``.
        """
        if not registered.name:
            raise RegistryError("nome de task vazio")
        if registered.name in self._tasks and not replace:
            raise DuplicateTaskError(
                f"task {registered.name!r} já registrada "
                f"(em {self._tasks[registered.name].func.__module__!r}); "
                "use replace=True ou um nome diferente"
            )
        self._tasks[registered.name] = registered
        LOGGER.debug("task registrada nome=%s fila=%s", registered.name, registered.queue)
        return registered

    def task(
        self,
        *,
        name: str | None = None,
        queue: str = "default",
        priority: int = 0,
        max_retries: int = 3,
        timeout: float | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Callable[[TaskFunction], RegisteredTask]:
        """Devolve um decorador que registra a função decorada.

        Args:
            name: Nome lógico da task. Padrão: ``nome_do_módulo.nome_da_função``.
            queue: Fila padrão da task.
            priority: Prioridade padrão (maior executa antes).
            max_retries: Número de retries permitidos por padrão.
            timeout: Timeout padrão em segundos.
            tags: Metadados textuais livres.

        Returns:
            Um decorador que devolve um :class:`RegisteredTask`.
        """

        def decorator(func: TaskFunction) -> RegisteredTask:
            """Registra ``func`` e devolve o :class:`RegisteredTask` correspondente."""
            task_name = name or f"{func.__module__}.{func.__qualname__}"
            registered = RegisteredTask(
                name=task_name,
                func=func,
                queue=queue,
                priority=priority,
                max_retries=max_retries,
                timeout=timeout,
                tags=dict(tags or {}),
                is_async=inspect.iscoroutinefunction(func),
                signature=str(inspect.signature(func)),
            )
            return self.register(registered)

        return decorator

    def find(self, name: str) -> RegisteredTask | None:
        """Devolve a task registrada ou ``None`` se não existir."""
        return self._tasks.get(name)

    def get(self, name: str) -> RegisteredTask:
        """Devolve a task registrada.

        Raises:
            UnknownTaskError: Se o nome não estiver registrado.
        """
        registered = self._tasks.get(name)
        if registered is None:
            known = ", ".join(sorted(self._tasks)) or "nenhuma"
            raise UnknownTaskError(f"task desconhecida: {name!r} (registradas: {known})")
        return registered

    def names(self) -> list[str]:
        """Lista os nomes registrados em ordem alfabética."""
        return sorted(self._tasks)

    def all(self) -> list[RegisteredTask]:
        """Lista as tasks registradas em ordem alfabética."""
        return [self._tasks[name] for name in sorted(self._tasks)]

    def describe(self) -> list[dict[str, Any]]:
        """Lista os metadados de todas as tasks registradas."""
        return [registered.to_dict() for registered in self.all()]

    def clear(self) -> None:
        """Remove todas as tasks do registro."""
        self._tasks.clear()

    def load_modules(
        self,
        modules: Iterable[str],
        *,
        force_reload: bool = False,
    ) -> list[str]:
        """Importa módulos e transpõe as tasks declaradas por eles para este registro.

        As tasks são lidas dos próprios módulos (``RegisteredTask`` exposto como
        atributo de módulo), e não do registro global. Assim funciona tanto para
        módulos ainda não importados quanto para módulos já carregados, e cada
        processo pode manter um registro isolado sem depender de estado compartilhado.

        Args:
            modules: Nomes de módulos (``pkg.mod``) ou objetos de módulo.
            force_reload: Recarrega módulos já importados (``importlib.reload``).

        Returns:
            A lista de módulos efetivamente importados.

        Raises:
            ModuleLoadError: Se algum módulo não puder ser importado.
        """
        nomes: list[str] = []
        for item in modules:
            nome = item.strip() if isinstance(item, str) else getattr(item, "__name__", "")
            if nome:
                nomes.append(nome)
        total = 0
        for nome in nomes:
            try:
                if force_reload and nome in sys.modules:
                    modulo = importlib.reload(sys.modules[nome])
                else:
                    modulo = importlib.import_module(nome)
            except Exception as exc:
                raise ModuleLoadError(
                    f"não foi possível importar o módulo {nome!r}: {exc}"
                ) from exc
            encontradas = [
                valor
                for _, valor in sorted(vars(modulo).items(), key=lambda item: item[0])
                if isinstance(valor, RegisteredTask)
                and getattr(valor.func, "__module__", "") == nome
            ]
            for registered in encontradas:
                self.register(registered, replace=True)
            total += len(encontradas)
        if nomes:
            LOGGER.debug(
                "módulos de task importados",
                extra={"modulos": ",".join(nomes), "tasks": total},
            )
        return nomes

    def __len__(self) -> int:
        """Quantidade de tasks registradas."""
        return len(self._tasks)

    def __contains__(self, name: object) -> bool:
        """Diz se o nome está registrado."""
        return name in self._tasks


#: Registro usado pelo decorador de módulo :func:`task`.
registry: Final[TaskRegistry] = TaskRegistry()


def task(
    func: TaskFunction | None = None,
    *,
    name: str | None = None,
    queue: str = "default",
    priority: int = 0,
    max_retries: int = 3,
    timeout: float | None = None,
    tags: Mapping[str, str] | None = None,
    replace: bool = False,
) -> Any:
    """Registra uma função como task do taskflow.

    Pode ser usado como ``@task`` ou ``@task(queue="alta", max_retries=5)``::

        @task(name="send_email", queue="emails", priority=10, max_retries=5)
        def send_email(to: str) -> str:
            return f"enviado para {to}"

    Args:
        func: Função decorada (preenchido quando usado como ``@task``).
        name: Nome lógico da task (padrão: módulo + nome qualificado).
        queue: Fila padrão.
        priority: Prioridade padrão (maior primeiro).
        max_retries: Retries padrão.
        timeout: Timeout padrão em segundos.
        tags: Metadados textuais livres.
        replace: Substitui uma task previamente registrada com o mesmo nome.

    Returns:
        Um :class:`RegisteredTask`, que também pode ser chamado como a função.
    """
    decorator = registry.task(
        name=name,
        queue=queue,
        priority=priority,
        max_retries=max_retries,
        timeout=timeout,
        tags=tags,
    )
    if func is None:
        return decorator
    registered = decorator(func)
    if replace:
        registry.register(registered, replace=True)
    return registered


def build_registry(
    modules: Sequence[str] | None = None, *, replace: bool = False
) -> TaskRegistry:
    """Cria um :class:`TaskRegistry` populado com os módulos indicados.

    Usado pela CLI e pelo dashboard para isolar o registro em cada processo.
    """
    local = TaskRegistry()
    if modules:
        local.load_modules(modules, force_reload=replace)
    return local