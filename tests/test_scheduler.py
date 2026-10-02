"""Testes do parser cron e do agendador periódico."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from taskflow.core.broker import Broker
from taskflow.core.registry import TaskRegistry
from taskflow.core.states import TaskState
from taskflow.scheduler.cron import CronError, CronExpression, next_runs, parse_cron
from taskflow.scheduler.scheduler import (
    Schedule,
    Scheduler,
    SchedulerError,
    load_schedules,
)


# ------------------------------------------------------------------ expressões válidas


@pytest.mark.parametrize(
    ("expressao", "minutos", "horas", "dias", "meses", "semana"),
    [
        ("* * * * *", 60, 24, 31, 12, 7),
        ("*/5 * * * *", 12, 24, 31, 12, 7),
        ("*/15 * * * *", 4, 24, 31, 12, 7),
        ("0 9 * * *", 1, 1, 31, 12, 7),
        ("30 2 1 * *", 1, 1, 1, 12, 7),
        ("0 0 1 1 *", 1, 1, 1, 1, 7),
        ("15,45 3-5 * * *", 2, 3, 31, 12, 7),
        ("0 9-17/4 * * *", 1, 3, 31, 12, 7),
        ("0 0 * * 1-5", 1, 1, 31, 12, 5),
        ("0 0 * JAN *", 1, 1, 31, 1, 7),
        ("0 0 * * MON-FRI", 1, 1, 31, 12, 5),
        ("5 4 * * sun", 1, 1, 31, 12, 1),
        ("0 12 * * 7", 1, 1, 31, 12, 1),
        ("*/30 0-23/6 1,15 */3 *", 2, 4, 2, 4, 7),
    ],
)
def test_parse_de_expressoes_validas(
    expressao: str, minutos: int, horas: int, dias: int, meses: int, semana: int
) -> None:
    """Cada sintaxe aceita produz exatamente a quantidade de valores esperada."""
    cron = CronExpression.parse(expressao)

    assert len(cron.minutes.values) == minutos
    assert len(cron.hours.values) == horas
    assert len(cron.days.values) == dias
    assert len(cron.months.values) == meses
    assert len(cron.weekdays.values) == semana
    assert cron.expression == expressao
    assert cron.describe().startswith(repr(expressao))


def test_comentario_e_espacos_sao_ignorados() -> None:
    """Comentários com ``#`` e espaços extras não quebram o parse."""
    cron = CronExpression.parse("  */10   *  * * *   # a cada dez minutos ")

    assert cron.minutes.values == frozenset({0, 10, 20, 30, 40, 50})
    assert cron.expression == "*/10   *  * * *"


def test_representacao_canonica_e_comparacao() -> None:
    """Nomes viram números e expressões equivalentes comparam iguais."""
    assert str(CronExpression.parse("0 0 * JAN MON")) == "0 0 * 1 1"
    assert CronExpression.parse("0 0 * JAN MON") == CronExpression.parse("0 0 * 1 1")
    assert repr(CronExpression.parse("* * * * *")) == "CronExpression('* * * * *')"
    assert parse_cron("*/5 * * * *") == CronExpression.parse("0,5,10,15,20,25,30,35,40,45,50,55 * * * *")


# ------------------------------------------------------------------ expressões inválidas


@pytest.mark.parametrize(
    ("expressao", "campo"),
    [
        ("", "vazia"),
        ("   ", "vazia"),
        ("* * * *", "5 campos"),
        ("* * * * * *", "6 campo"),
        ("60 * * * *", "'minuto'"),
        ("* 24 * * *", "'hora'"),
        ("* * 32 * *", "'dia do mês'"),
        ("* * 0 * *", "'dia do mês'"),
        ("* * * 13 *", "'mês'"),
        ("* * * * 7 *", "6 campo"),
        ("*/0 * * * *", "'minuto'"),
        ("*/x * * * *", "'minuto'"),
        ("5-1 * * * *", "'minuto'"),
        ("abc * * * *", "'minuto'"),
        ("1,,2 * * * *", "'minuto'"),
        ("0 0 * * FOO", "'dia da semana'"),
        ("0 0 * * 8", "'dia da semana'"),
    ],
)
def test_expressoes_invalidas_falham_com_mensagem_clara(expressao: str, campo: str) -> None:
    """Cada expressão inválida aponta o campo (ou o motivo) na mensagem."""
    with pytest.raises(CronError) as erro:
        CronExpression.parse(expressao)

    mensagem = str(erro.value)
    assert campo in mensagem, mensagem
    assert "cron" in mensagem.lower() or "campo" in mensagem.lower()


def test_next_after_sem_execucao_falha() -> None:
    """Datas impossíveis (30 de fevereiro) levantam erro de busca."""
    cron = CronExpression.parse("0 0 30 2 *")

    with pytest.raises(CronError) as erro:
        cron.next_after(datetime(2024, 1, 1, 12, 0))

    assert "próximos" in str(erro.value)


# ------------------------------------------------------------------ avaliação


def test_matches_respeita_cada_campo() -> None:
    """``matches`` só aceita instantes dentro de todos os limites."""
    cron = CronExpression.parse("*/15 9-17 * * MON-FRI")

    assert cron.matches(datetime(2024, 3, 5, 9, 15)) is True  # terça
    assert cron.matches(datetime(2024, 3, 8, 17, 45)) is True  # sexta
    assert cron.matches(datetime(2024, 3, 5, 9, 16)) is False
    assert cron.matches(datetime(2024, 3, 5, 8, 15)) is False
    assert cron.matches(datetime(2024, 3, 9, 9, 15)) is False  # sábado


def test_dia_do_mes_e_dia_da_semana_usam_semantica_ou() -> None:
    """Com os dois campos restritos, dispara se qualquer um casar (regra do Vixie)."""
    cron = CronExpression.parse("0 0 1 * 5")  # dia 1 ou sexta-feira

    assert cron.matches(datetime(2024, 3, 1, 0, 0)) is True  # dia 1 (sexta)
    assert cron.matches(datetime(2024, 3, 8, 0, 0)) is True  # dia 8 (sexta)
    assert cron.matches(datetime(2024, 3, 2, 0, 0)) is False  # sábado, dia 2
    assert cron.matches(datetime(2024, 3, 4, 0, 0)) is False  # segunda, dia 4


def test_next_after_encontra_a_proxima_execucao() -> None:
    """``next_after`` devolve o primeiro minuto válido depois do instante dado."""
    cron = CronExpression.parse("*/5 * * * *")
    inicio = datetime(2024, 3, 5, 9, 2, 30)

    assert cron.next_after(inicio) == datetime(2024, 3, 5, 9, 5)
    assert cron.next_after(datetime(2024, 3, 5, 9, 4, 59)) == datetime(2024, 3, 5, 9, 5)
    assert cron.next_after(datetime(2024, 3, 5, 9, 55, 0)) == datetime(2024, 3, 5, 10, 0)


def test_next_after_faz_rollover_de_dia_e_mes() -> None:
    """A busca salta de dia, mês e ano corretamente."""
    assert CronExpression.parse("0 9 * * *").next_after(datetime(2024, 3, 5, 9, 30)) == datetime(
        2024, 3, 6, 9, 0
    )
    assert CronExpression.parse("0 0 1 * *").next_after(datetime(2024, 1, 15)) == datetime(
        2024, 2, 1
    )
    assert CronExpression.parse("0 0 1 1 *").next_after(datetime(2024, 6, 1)) == datetime(
        2025, 1, 1
    )
    assert CronExpression.parse("59 23 31 12 *").next_after(datetime(2024, 12, 31, 23, 59)) == (
        datetime(2025, 12, 31, 23, 59)
    )


def test_next_after_preserva_fuso_e_ignora_segundos() -> None:
    """O instante devolvido mantém o fuso e nunca carrega segundos."""
    com_fuso = datetime(2024, 3, 5, 9, 0, 45, 123, tzinfo=datetime.now().astimezone().tzinfo)
    resultado = CronExpression.parse("30 9 * * *").next_after(com_fuso)

    assert resultado.hour == 9 and resultado.minute == 30
    assert resultado.second == 0 and resultado.microsecond == 0
    assert resultado.tzinfo == com_fuso.tzinfo


def test_next_runs_lista_proximas_execucoes() -> None:
    """O atalho ``next_runs`` devolve N instantes em ordem crescente."""
    instantes = next_runs("*/20 * * * *", 3, start=datetime(2024, 3, 5, 9, 0))

    assert instantes == [
        datetime(2024, 3, 5, 9, 20),
        datetime(2024, 3, 5, 9, 40),
        datetime(2024, 3, 5, 10, 0),
    ]
    with pytest.raises(CronError):
        next_runs("* * * * *", 0)


# ------------------------------------------------------------------ agendador


class RelogioFake:
    """Relógio controlado por testes, para não esperar de verdade."""

    def __init__(self, inicio: datetime) -> None:
        """Começa no instante informado."""
        self.agora = inicio

    def __call__(self) -> float:
        """Devolve o instante atual em epoch."""
        return self.agora.timestamp()

    def avancar(self, segundos: float) -> None:
        """Move o relógio para a frente."""
        self.agora = self.agora + timedelta(seconds=segundos)


async def test_scheduler_dispara_task_no_horario(
    broker: Broker, registry: TaskRegistry
) -> None:
    """O loop dispara a task quando o relógio alcança o minuto da expressão."""
    relogio = RelogioFake(datetime(2024, 3, 5, 9, 0, 5))

    async def dormir(_segundos: float) -> None:
        """Avança o relógio falso em vez de esperar."""
        relogio.avancar(0.5)

    agendador = Scheduler(broker, broker.config, clock=relogio, sleep=dormir, max_sleep=0.5)
    agendador.add_task("ok", "*/5 * * * *", "ana")

    assert await agendador.tick() == 0  # ainda não chegou o horário
    relogio.avancar(300)  # 9:05:05

    assert await agendador.tick() == 1
    assert broker.stats().total == 1
    tarefa = broker.list_tasks()[0]
    assert tarefa.state is TaskState.PENDING
    assert tarefa.args == ["ana"]
    assert tarefa.tags["scheduled"] == "*/5 * * * *"

    await asyncio.sleep(0)
    assert agendador.schedules()[0].run_count == 1


async def test_scheduler_nao_faz_catch_up_de_execucoes_perdidas(
    broker: Broker, registry: TaskRegistry
) -> None:
    """Se o processo ficou parado, o agendador dispara uma vez — não uma por minuto perdido."""
    relogio = RelogioFake(datetime(2024, 3, 5, 9, 0))
    agendador = Scheduler(broker, broker.config, clock=relogio, max_sleep=1.0)
    agendador.add_task("ok", "* * * * *", "perdida")
    agendador.due()  # primeiro cálculo do horário
    relogio.avancar(600)  # dez minutos de tardez

    assert await agendador.tick() == 1
    assert broker.stats().total == 1


async def test_scheduler_reusa_metadados_da_task(
    broker: Broker, registry: TaskRegistry
) -> None:
    """``add_task`` herda fila, prioridade e retries da task registrada."""
    agendador = Scheduler(broker, broker.config)
    schedule = agendador.add_task(registry.get("flaky"), "*/5 * * * *", 1)

    assert schedule.queue == "demo"
    assert schedule.max_retries == 5
    assert schedule.args == [1]
    assert schedule.expression == "*/5 * * * *"


async def test_scheduler_rejeita_nome_duplicado_e_remove(
    broker: Broker, registry: TaskRegistry
) -> None:
    """Nomes duplicados são recusados e ``remove`` desregistra."""
    agendador = Scheduler(broker, broker.config)
    agendador.add_task("ok", "*/5 * * * *", "x")

    with pytest.raises(SchedulerError):
        agendador.add_task("ok", "*/10 * * * *")

    assert agendador.remove("ok") is True
    assert agendador.remove("ok") is False
    assert agendador.schedules() == []


async def test_scheduler_start_e_stop(broker: Broker, registry: TaskRegistry) -> None:
    """O loop sobe e para limpamente em background."""
    agendador = Scheduler(broker, broker.config, max_sleep=0.01)
    agendador.add_task("ok", "*/5 * * * *", "x")

    await agendador.start()
    assert agendador.running is True
    await asyncio.sleep(0.03)
    await agendador.stop()

    assert agendador.running is False


async def test_next_runs_ordenado_e_com_estado(
    broker: Broker, registry: TaskRegistry
) -> None:
    """``next_runs`` devolve os próximos disparos ordenados e com os metadados."""
    agendador = Scheduler(broker, broker.config, clock=RelogioFake(datetime(2024, 3, 5, 9, 0)))
    agendador.add_task("ok", "*/30 * * * *", "depois")
    agendador.add_task("alta", "*/10 * * * *", 1, queue="alta")

    lista = agendador.next_runs()

    assert [item["name"] for item in lista] == ["alta", "ok"]
    assert lista[0]["cron"] == "*/10 * * * *"
    assert lista[0]["next_run_at"] is not None
    assert datetime.fromtimestamp(lista[0]["next_run_at"]).minute == 10


async def test_due_calcula_o_primeiro_horario(
    broker: Broker, registry: TaskRegistry
) -> None:
    """``due`` calcula o primeiro horário e depois só devolve os vencidos."""
    relogio = RelogioFake(datetime(2024, 3, 5, 9, 0))
    agendador = Scheduler(broker, broker.config, clock=relogio)
    schedule = agendador.add_task("ok", "*/15 * * * *", "x")

    assert agendador.due() == []  # apenas calculou o próximo horário
    assert datetime.fromtimestamp(schedule.next_run_at or 0).minute == 15

    relogio.avancar(15 * 60)
    assert [item.name for item in agendador.due()] == ["ok"]


async def test_load_schedules_por_dicionarios(
    broker: Broker, registry: TaskRegistry
) -> None:
    """``load_schedules`` cria schedules a partir de dicionários de configuração."""
    relogio = RelogioFake(datetime(2024, 3, 5, 9, 0))
    agendador = Scheduler(broker, broker.config, clock=relogio)

    criados = load_schedules(
        agendador,
        [
            {"name": "ok", "cron": "*/5 * * * *", "args": ["ana"], "priority": 4},
            {"name": "alta", "cron": "0 * * * *", "kwargs": {"indice": 1}},
        ],
    )

    assert len(criados) == 2
    agendador.due()  # calcula o primeiro horário de cada schedule
    relogio.avancar(300)
    assert await agendador.tick() == 1
    assert broker.list_tasks()[0].priority == 4

    with pytest.raises(KeyError):
        load_schedules(agendador, [{"cron": "* * * * *"}])


async def test_scheduler_contabiliza_erro_ao_enfileirar(
    broker: Broker, registry: TaskRegistry
) -> None:
    """Se o registro falhar ao enfileirar, o erro é contabilizado e o schedule continua."""
    relogio = RelogioFake(datetime(2024, 3, 5, 9, 0, 1))
    agendador = Scheduler(broker, broker.config, clock=relogio)
    schedule = Schedule(name="inexistente", cron=CronExpression.parse("* * * * *"))
    agendador.add(schedule)

    await agendador.tick()  # apenas calcula o próximo horário (9:01)
    relogio.avancar(120)  # 9:02, horário vencido

    assert await agendador.tick() == 0
    assert schedule.error_count == 1
    assert schedule.run_count == 0
    assert schedule.next_run_at is not None
    assert schedule.last_task_id is None