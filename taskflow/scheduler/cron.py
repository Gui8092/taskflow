"""Parser de expressões cron, implementado sem bibliotecas externas.

Formato aceito (5 campos, no estilo Vixie)::

    ┌─────────── minuto (0-59)
    │ ┌───────── hora (0-23)
    │ │ ┌─────── dia do mês (1-31)
    │ │ │ ┌───── mês (1-12 ou jan..dez)
    │ │ │ │ ┌─── dia da semana (0-6 ou dom..sab, 7 = domingo)
    │ │ │ │ │
    * * * * *

Cada campo aceita ``*``, ``*/n``, ``a``, ``a-b``, ``a-b/n`` e listas separadas por
vírgula (``0,30``). Comentários com ``#`` e espaços em volta são ignorados.

Quando **dia do mês** e **dia da semana** estão ambos restritos, o disparo ocorre
se qualquer um dos dois casar (semântica OU do Vixie, como no ``crontab`` padrão).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Sequence

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.scheduler")

#: Nome dos campos, na ordem, com o intervalo aceito.
FIELDS: Final[tuple[tuple[str, int, int], ...]] = (
    ("minuto", 0, 59),
    ("hora", 0, 23),
    ("dia do mês", 1, 31),
    ("mês", 1, 12),
    ("dia da semana", 0, 6),
)

#: Quantidade de campos esperados na expressão.
FIELD_COUNT: Final[int] = len(FIELDS)

#: Limite de busca da próxima execução, para expressões impossíveis falharem.
MAX_LOOKAHEAD_DAYS: Final[int] = 1500

_MONTH_NAMES: Final[dict[str, int]] = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}

_WEEKDAY_NAMES: Final[dict[str, int]] = {
    "dom": 0,
    "seg": 1,
    "ter": 2,
    "qua": 3,
    "qui": 4,
    "sex": 5,
    "sab": 6,
}

_MONTH_NAMES_EN: Final[dict[str, int]] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

_WEEKDAY_NAMES_EN: Final[dict[str, int]] = {
    "sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}


class CronError(ValueError):
    """Expressão cron inválida, com mensagem indicando o campo e o token culpados."""


def _format_fields(values: Sequence[int]) -> str:
    """Formata um conjunto de valores como expressão cron legível."""
    ordered = sorted(values)
    groups: list[tuple[int, int]] = []
    for value in ordered:
        if groups and value == groups[-1][1] + 1:
            groups[-1] = (groups[-1][0], value)
        else:
            groups.append((value, value))
    parts: list[str] = []
    for start, end in groups:
        parts.append(str(start) if start == end else f"{start}-{end}")
    return ",".join(parts)


@dataclass(frozen=True, slots=True)
class CronField:
    """Um campo da expressão já validado."""

    name: str
    values: frozenset[int]
    wildcard: bool

    def matches(self, value: int) -> bool:
        """Diz se o valor pertence ao conjunto aceito."""
        return value in self.values

    def __str__(self) -> str:
        """Representação canônica (usada por :meth:`CronExpression.__str__`)."""
        return "*" if self.wildcard else _format_fields(self.values)


class CronExpression:
    """Expressão cron de 5 campos, já validada e pronta para uso."""

    __slots__ = ("expression", "minutes", "hours", "days", "months", "weekdays")

    def __init__(
        self,
        expression: str,
        minutes: CronField,
        hours: CronField,
        days: CronField,
        months: CronField,
        weekdays: CronField,
    ) -> None:
        """Guarda os campos validados e a expressão original."""
        self.expression = expression
        self.minutes = minutes
        self.hours = hours
        self.days = days
        self.months = months
        self.weekdays = weekdays

    # ------------------------------------------------------------------ parsing

    @classmethod
    def parse(cls, text: str) -> CronExpression:
        """Faz o parse e a validação de uma expressão cron.

        Args:
            text: Expressão de 5 campos, opcionalmente com ``# comentário``.

        Returns:
            A expressão validada.

        Raises:
            CronError: Com mensagem indicando o campo e o token inválidos.
        """
        if not isinstance(text, str):
            raise CronError(f"esperado texto, recebido {type(text).__name__}")
        cleaned = text.split("#", 1)[0].strip()
        if not cleaned:
            raise CronError(
                "expressão cron vazia; esperado 5 campos no formato "
                "'minuto hora dia-do-mês mês dia-da-semana' (ex.: */5 * * * *)"
            )
        tokens = cleaned.split()
        if len(tokens) != FIELD_COUNT:
            raise CronError(
                f"expressão {text.strip()!r} tem {len(tokens)} campo(s), esperado {FIELD_COUNT} campos. "
                "Formato: 'minuto hora dia-do-mês mês dia-da-semana' (ex.: 30 9 * * 1-5)"
            )
        parsed = [
            cls._parse_field(token, index) for index, token in enumerate(tokens)
        ]
        return cls(
            expression=cleaned,
            minutes=parsed[0],
            hours=parsed[1],
            days=parsed[2],
            months=parsed[3],
            weekdays=parsed[4],
        )

    @staticmethod
    def _parse_field(token: str, index: int) -> CronField:
        """Valida um único campo da expressão.

        Raises:
            CronError: Se o token estiver fora da sintaxe ou dos limites do campo.
        """
        name, minimum, maximum = FIELDS[index]
        # No campo de dia da semana o crontab aceita também 7 (domingo).
        limite = 7 if index == 4 else maximum
        aliases: dict[str, int] = {}
        if index == 3:
            aliases = {**_MONTH_NAMES, **_MONTH_NAMES_EN}
        elif index == 4:
            aliases = _WEEKDAY_NAMES_EN | _WEEKDAY_NAMES

        def fail(reason: str) -> CronError:
            """Monta o erro de campo com o trecho esperado."""
            intervalo = f"{minimum} a {limite}"
            if index == 4:
                intervalo = "0 a 6 (7 também é domingo)"
            return CronError(
                f"campo {name!r}: {reason} (em {token!r}); "
                "aceito '*', '*/n', 'a', 'a-b', 'a-b/n' ou lista 'a,b,c', "
                f"valores de {intervalo}"
            )

        def to_int(token: str) -> int:
            """Converte token numérico ou apelido em inteiro, com erro do campo."""
            return _to_int(token, name, minimum, maximum, aliases)

        raw = token.strip()
        if not raw:
            raise fail("token vazio")
        values: set[int] = set()
        for part in raw.split(","):
            piece = part.strip()
            if not piece:
                raise fail("lista contém item vazio")
            step = 1
            if "/" in piece:
                piece, _, step_text = piece.partition("/")
                piece = piece.strip()
                step_text = step_text.strip()
                if not step_text.isdigit():
                    raise fail(f"passo inválido {step_text!r} (esperado inteiro positivo)")
                step = int(step_text)
                if step < 1:
                    raise fail(f"passo {step} inválido (passo precisa ser >= 1)")
            if piece == "*":
                start, end = minimum, limite
            elif "-" in piece[1:]:
                start_text, _, end_text = piece.partition("-")
                start = to_int(start_text)
                end = to_int(end_text)
                if start > end:
                    raise fail(f"intervalo invertido {start}-{end}")
            else:
                start = end = to_int(piece)
            if start < minimum or end > limite:
                span = str(start) if start == end else f"{start}-{end}"
                raise fail(f"valor {span} fora do intervalo {minimum}-{limite}")
            values.update(range(start, end + 1, step))
        if index == 4:
            values = {0 if value == 7 else value for value in values}  # 7 também é domingo
        if not values:
            raise fail("não restou nenhum valor válido")
        return CronField(name=name, values=frozenset(values), wildcard=raw == "*")

    # ------------------------------------------------------------------ avaliação

    def _day_matches(self, moment: datetime) -> bool:
        """Aplica a regra do dia (OU entre dia do mês e dia da semana, como no Vixie)."""
        by_day_of_month = self.days.matches(moment.day)
        by_weekday = self.weekdays.matches((moment.weekday() + 1) % 7)
        if not self.days.wildcard and not self.weekdays.wildcard:
            return by_day_of_month or by_weekday
        if not self.days.wildcard:
            return by_day_of_month
        if not self.weekdays.wildcard:
            return by_weekday
        return True

    def matches(self, moment: datetime) -> bool:
        """Diz se a expressão dispara exatamente no instante *moment* (segundos ignorados)."""
        return (
            self.minutes.matches(moment.minute)
            and self.hours.matches(moment.hour)
            and self.months.matches(moment.month)
            and self._day_matches(moment)
        )

    def next_after(self, moment: datetime, *, limit_days: int = MAX_LOOKAHEAD_DAYS) -> datetime:
        """Devolve o primeiro instante estritamente posterior a *moment* que casa.

        Args:
            moment: Instante de referência (com ou sem ``tzinfo``).
            limit_days: Teto de busca; ao estourar, levanta :class:`CronError`
                (acontece com datas impossíveis, como ``0 0 30 2 *``).

        Returns:
            O instante da próxima execução, com a mesma informação de fuso de *moment*.

        Raises:
            CronError: Se nenhuma execução existir dentro do limite.
        """
        current = moment.replace(second=0, microsecond=0) + timedelta(minutes=1)
        limit = moment + timedelta(days=limit_days)
        while current <= limit:
            if not self.months.matches(current.month):
                current = _next_month(current)
                continue
            if not self._day_matches(current):
                current = _next_day(current)
                continue
            if not self.hours.matches(current.hour):
                current = _next_hour(current)
                continue
            if self.minutes.matches(current.minute):
                return current
            later = [value for value in self.minutes.values if value > current.minute]
            if later:
                current = current.replace(minute=min(later))
                continue
            current = _next_hour(current)
        raise CronError(
            f"a expressão {self.expression!r} não tem execução nos próximos {limit_days} dias "
            f"a partir de {moment.isoformat()}"
        )

    # ------------------------------------------------------------------ apresentação

    def describe(self) -> str:
        """Descreve a expressão em português, campo a campo."""
        return (
            f"{self.expression!r}: minutos={self.minutes}, horas={self.hours}, "
            f"dias={self.days}, meses={self.months}, semana={self.weekdays}"
        )

    def __str__(self) -> str:
        """Expressão canônica (nomes viram números)."""
        return " ".join(
            str(field) for field in (self.minutes, self.hours, self.days, self.months, self.weekdays)
        )

    def __repr__(self) -> str:
        """Representação útil em depuração."""
        return f"CronExpression({self.expression!r})"

    def __eq__(self, other: object) -> bool:
        """Compara pela expressão canônica."""
        if isinstance(other, CronExpression):
            return str(self) == str(other)
        return NotImplemented

    def __hash__(self) -> int:
        """Hash pela expressão canônica."""
        return hash(str(self))


def _to_int(
    token: str,
    field_name: str,
    minimum: int,
    maximum: int,
    aliases: dict[str, int],
) -> int:
    """Converte um token numérico ou apelido de nome em inteiro.

    Raises:
        CronError: Se o token não for número nem apelido conhecido.
    """
    candidate = token.strip()
    if not candidate:
        raise CronError(f"campo {field_name!r}: token vazio")
    alias = aliases.get(candidate.lower())
    if alias is not None:
        return alias
    try:
        return int(candidate)
    except ValueError as exc:
        accepted = ", ".join(sorted(aliases)) or "somente números"
        raise CronError(
            f"campo {field_name!r}: token inválido {candidate!r} "
            f"(esperado número de {minimum} a {maximum}, ou nome: {accepted})"
        ) from exc


def _next_month(moment: datetime) -> datetime:
    """Avança para o primeiro minuto do mês seguinte."""
    year = moment.year + (1 if moment.month == 12 else 0)
    month = 1 if moment.month == 12 else moment.month + 1
    return moment.replace(year=year, month=month, day=1, hour=0, minute=0)


def _next_day(moment: datetime) -> datetime:
    """Avança para o primeiro minuto do dia seguinte."""
    return (moment + timedelta(days=1)).replace(hour=0, minute=0)


def _next_hour(moment: datetime) -> datetime:
    """Avança para o primeiro minuto da hora seguinte."""
    return (moment + timedelta(hours=1)).replace(minute=0)


def parse_cron(text: str) -> CronExpression:
    """Atalho funcional para :meth:`CronExpression.parse`."""
    return CronExpression.parse(text)


def next_runs(expression: str | CronExpression, count: int = 5, *, start: datetime | None = None) -> list[datetime]:
    """Devolve as próximas *count* execuções de uma expressão cron.

    Args:
        expression: Texto da expressão ou objeto já validado.
        count: Quantidade de instantes.
        start: Instante inicial (padrão: agora, arredondado para o minuto).

    Raises:
        CronError: Se a expressão for inválida ou não tiver execuções futuras.
    """
    cron = expression if isinstance(expression, CronExpression) else CronExpression.parse(expression)
    if count < 1:
        raise CronError(f"count precisa ser >= 1 (recebido {count})")
    reference = start if start is not None else datetime.now().replace(second=0, microsecond=0)
    moments: list[datetime] = []
    cursor = reference
    for _ in range(count):
        cursor = cron.next_after(cursor)
        moments.append(cursor)
    return moments