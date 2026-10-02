"""Política de retry: backoff exponencial com jitter."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Final

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.retry")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Calcula quanto tempo esperar antes da próxima tentativa.

    O modelo é *full jitter*: a cada tentativa o teto cresce exponencialmente
    (``base * 2 ** (tentativa - 1)``, limitado por ``cap``) e o atraso real é
    sorteado uniformemente em ``[0, teto]``. Isso espalha o estouro de tasks que
    falharam juntas, evitando que todas voltem no mesmo instante.

    Sem jitter (``jitter=False``) o atraso é exatamente o teto, o que deixa os
    testes determinísticos.
    """

    base: float = 0.5
    cap: float = 30.0
    jitter: bool = True
    max_retries: int = 3

    def ceiling_for(self, attempt: int) -> float:
        """Devolve o teto do backoff para a tentativa informada (1 = primeira falha)."""
        if attempt < 1:
            return 0.0
        return min(self.cap, self.base * (2 ** (attempt - 1)))

    def delay_for(self, attempt: int, *, rng: random.Random | None = None) -> float:
        """Devolve o atraso, em segundos, antes da próxima tentativa.

        Args:
            attempt: Número da tentativa que acabou de falhar (1 para a primeira).
            rng: Gerador aleatório injetável (útil em testes determinísticos).

        Returns:
            Segundos a esperar, sempre ``>= 0`` e nunca maior que o teto.
        """
        ceiling = self.ceiling_for(attempt)
        if ceiling <= 0:
            return 0.0
        if not self.jitter:
            return ceiling
        return (rng or random).uniform(0.0, ceiling)

    def eta_for(self, attempt: int, *, now: float | None = None, rng: random.Random | None = None) -> float:
        """Devolve o instante (epoch) em que a próxima tentativa deve acontecer."""
        moment = time.time() if now is None else now
        return moment + self.delay_for(attempt, rng=rng)

    def should_retry(
        self,
        *,
        attempt: int,
        max_retries: int,
        permanent: bool = False,
    ) -> bool:
        """Diz se ainda há orçamento de retry.

        ``attempt`` é o número de execuções já feitas (incrementado no ``fetch``), e
        ``max_retries`` é a quantidade de *retries* permitidos — ou seja, o total de
        execuções pode chegar a ``max_retries + 1``.
        """
        if permanent:
            return False
        return attempt <= max_retries

    @classmethod
    def from_config(cls, config: object, *, max_retries: int | None = None) -> RetryPolicy:
        """Cria a política a partir de um :class:`~taskflow.core.config.Config`.

        Args:
            config: Objeto de configuração com ``retry_base``, ``retry_cap``,
                ``retry_jitter`` e ``default_max_retries``.
            max_retries: Sobrescreve ``config.default_max_retries``.
        """
        return cls(
            base=float(getattr(config, "retry_base")),
            cap=float(getattr(config, "retry_cap")),
            jitter=bool(getattr(config, "retry_jitter")),
            max_retries=int(
                max_retries if max_retries is not None else getattr(config, "default_max_retries")
            ),
        )

    def describe(self) -> str:
        """Resumo legível da política, usado em logs e na CLI."""
        mode = "full-jitter" if self.jitter else "determinístico"
        return (
            f"base={self.base}s cap={self.cap}s retries={self.max_retries} ({mode})"
        )