"""Conversão de payloads para JSON seguro, com mensagens de erro precisas.

O broker grava os payloads das tasks em um journal JSONL, portanto qualquer
valor devolvido por uma task precisa ser serializável. Este módulo centraliza a
conversão e recusa, com mensagem indicando o caminho exato do problema, tipos
que quebrariam o journal (funções, sockets, bytes não-UTF-8, ``NaN``...).
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence, Set
from dataclasses import fields, is_dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Any, Callable, Final
from uuid import UUID

DEFAULT_MAX_DEPTH: Final[int] = 32


class SerializationError(TypeError):
    """Payload não serializável, com o caminho do valor problemático.

    Atributos:
        path: Caminho do valor dentro da estrutura (``args[0].user.name``).
    """

    def __init__(self, message: str, *, path: str = "$") -> None:
        """Guarda a mensagem e o caminho do valor que não pôde ser convertido."""
        super().__init__(f"{message} (em {path})")
        self.path = path


def _describe(value: Any) -> str:
    """Devolve o nome do tipo de um valor, com o módulo para ficar inequívoco."""
    tp = type(value)
    return f"{tp.__module__}.{tp.__qualname__}"


def to_jsonable(
    value: Any,
    *,
    path: str = "$",
    max_depth: int = DEFAULT_MAX_DEPTH,
    _depth: int = 0,
    _seen: set[int] | None = None,
) -> Any:
    """Converte *value* em uma estrutura composta apenas por tipos JSON.

    Tipos aceitos: ``None``, ``bool``, ``int``, ``float`` finito, ``str``, bytes
    UTF-8, ``datetime``/``date``/``time``/``timedelta``, ``Decimal``, ``UUID``,
    ``Path``, ``Enum``, ``set``/``frozenset``, ``tuple``, ``list``, ``dict`` com
    chaves ``str`` e dataclasses.

    Args:
        value: Valor a converter.
        path: Caminho usado nas mensagens de erro.
        max_depth: Profundidade máxima aceita antes de recusar a estrutura.
        _depth: Profundidade atual (uso interno).
        _seen: Identificadores dos contêiner já visitados (detecção de ciclo).

    Returns:
        Uma estrutura JSON-safe.

    Raises:
        SerializationError: Para tipos não suportados, ciclos, profundidade
            excessiva, ``NaN``/``Infinity`` ou chaves de ``dict`` não textuais.
    """
    if _depth > max_depth:
        raise SerializationError(
            f"estrutura profunda demais (limite {max_depth} níveis)", path=path
        )
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SerializationError(
                f"float não finito ({value!r}) não é JSON válido", path=path
            )
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise SerializationError(
                f"Decimal não finito ({value!r}) não é JSON válido", path=path
            )
        return float(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SerializationError(
                f"bytes não decodificáveis em UTF-8 ({len(value)} bytes)", path=path
            ) from exc
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, PurePath):
        return str(value)
    if isinstance(value, Enum):
        return to_jsonable(value.value, path=path, max_depth=max_depth, _depth=_depth, _seen=_seen)

    seen = set() if _seen is None else _seen
    container_id = id(value)
    is_container = isinstance(value, (Mapping, Sequence, Set)) and not isinstance(
        value, (str, bytes, bytearray)
    )
    if is_container:
        if container_id in seen:
            raise SerializationError("ciclo de referência detectado", path=path)
        seen.add(container_id)
    try:
        if is_dataclass(value) and not isinstance(value, type):
            result: dict[str, Any] = {}
            for field_info in fields(value):
                result[field_info.name] = to_jsonable(
                    getattr(value, field_info.name),
                    path=f"{path}.{field_info.name}",
                    max_depth=max_depth,
                    _depth=_depth + 1,
                    _seen=seen,
                )
            return result
        if isinstance(value, Mapping):
            converted: dict[str, Any] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise SerializationError(
                        f"chave de dict precisa ser str, recebido {_describe(key)}",
                        path=f"{path}.<chave>",
                    )
                converted[key] = to_jsonable(
                    item,
                    path=f"{path}.{key}",
                    max_depth=max_depth,
                    _depth=_depth + 1,
                    _seen=seen,
                )
            return converted
        if isinstance(value, (set, frozenset, Set)):
            items = [
                to_jsonable(
                    item,
                    path=f"{path}[{index}]",
                    max_depth=max_depth,
                    _depth=_depth + 1,
                    _seen=seen,
                )
                for index, item in enumerate(value)
            ]
            return items
        if isinstance(value, Sequence):
            return [
                to_jsonable(
                    item,
                    path=f"{path}[{index}]",
                    max_depth=max_depth,
                    _depth=_depth + 1,
                    _seen=seen,
                )
                for index, item in enumerate(value)
            ]
        hook: Callable[[], Any] | None = getattr(value, "to_dict", None)
        if callable(hook):
            return to_jsonable(
                hook(), path=path, max_depth=max_depth, _depth=_depth + 1, _seen=seen
            )
        raise SerializationError(
            f"tipo não serializável: {_describe(value)} "
            "(aceitos: primitivos, datetime, Decimal, UUID, Path, Enum, list, tuple, "
            "set, dict, dataclass)",
            path=path,
        )
    finally:
        if is_container:
            seen.discard(container_id)


def dumps(value: Any, *, indent: int | None = None, sort_keys: bool = False) -> str:
    """Serializa *value* em JSON, convertendo antes para tipos JSON-safe.

    Raises:
        SerializationError: Se o valor não for serializável.
    """
    payload = to_jsonable(value)
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        indent=indent,
        sort_keys=sort_keys,
        separators=(",", ":") if indent is None else None,
    )


def loads(raw: str | bytes) -> Any:
    """Faz o parse de um documento JSON.

    Raises:
        SerializationError: Se o texto não for JSON válido.
    """
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SerializationError(f"JSON inválido na posição {exc.pos}: {exc.msg}") from exc