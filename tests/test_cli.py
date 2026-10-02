"""Testes da CLI: submit, status, tasks, monitor, worker, dlq, cron e dashboard."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from taskflow.cli.main import EXIT_ERROR, EXIT_OK, EXIT_USAGE, format_duration, main, render_table
from taskflow.core.broker import Broker
from taskflow.core.config import Config
from taskflow.core.registry import TaskRegistry
from taskflow.core.states import TaskState
from taskflow.dashboard.app import build_dashboard_html, build_state_snapshot, create_app

#: Argumentos base: diretório de estado isolado e sem cores (saída determinística).
BASE = ("--no-color", "--log-level", "WARNING")


def rodar(argv: list[str], saida: io.StringIO | None = None) -> tuple[int, str]:
    """Executa a CLI capturando a saída."""
    buffer = saida or io.StringIO()
    codigo = main([*BASE, *argv], out=buffer, err=io.StringIO())
    return codigo, buffer.getvalue()


def test_submit_enfileira_e_imprime_id(data_dir: Path) -> None:
    """``submit`` grava a task no journal e informa o id gerado."""
    codigo, saida = rodar(["--data-dir", str(data_dir), "--modules", "demo_tasks", "submit", "demo.ok", "--args", '["ana"]'])

    assert codigo == EXIT_OK
    assert "task enfileirada" in saida
    assert "nome      demo.ok" in saida

    payload = json.loads((data_dir / "journal.jsonl").read_text(encoding="utf-8").strip())
    assert payload["op"] == "enqueue"
    assert payload["task"]["name"] == "demo.ok"
    assert payload["task"]["args"] == ["ana"]

    _, saida_status = rodar(["--data-dir", str(data_dir), "status", "--json"])
    tarefas = json.loads(saida_status)
    assert tarefas[0]["state"] == TaskState.PENDING.value


def test_submit_rejeita_json_invalido(data_dir: Path) -> None:
    """JSON malformado no ``--args`` sai com código de uso, sem stacktrace."""
    buffer = io.StringIO()
    erros = io.StringIO()
    codigo = main(
        [*BASE, "--data-dir", str(data_dir), "--modules", "demo_tasks", "submit", "demo.ok", "--args", "{aosd"],
        out=buffer,
        err=erros,
    )

    assert codigo == EXIT_USAGE
    assert "não é um JSON válido" in erros.getvalue()


def test_submit_com_task_desconhecida(data_dir: Path) -> None:
    """Nome de task inexistente sai com erro e código 1."""
    erros = io.StringIO()
    codigo = main(
        [*BASE, "--data-dir", str(data_dir), "submit", "nao_existe"],
        out=io.StringIO(),
        err=erros,
    )

    assert codigo == EXIT_ERROR
    assert "task desconhecida" in erros.getvalue()


def test_status_de_uma_task(data_dir: Path) -> None:
    """``status <id>`` mostra o detalhe completo da task."""
    _, saida_submit = rodar(
        ["--data-dir", str(data_dir), "--modules", "demo_tasks", "submit", "demo.ok"]
    )
    task_id = saida_submit.split("id        ")[1].split()[0]

    codigo, saida = rodar(["--data-dir", str(data_dir), "status", task_id])

    assert codigo == EXIT_ERROR  # ainda está pendente, não é sucesso
    assert "demo.ok" in saida
    assert "estado" in saida and "pendente" in saida
    assert "tentativas" in saida


def test_status_resumo_lista_tabelas(data_dir: Path) -> None:
    """``status`` sem id mostra a tabela de filas e a de tasks."""
    rodar(["--data-dir", str(data_dir), "--modules", "demo_tasks", "submit", "demo.ok"])

    codigo, saida = rodar(["--data-dir", str(data_dir), "status", "--queue", "default"])

    assert codigo == EXIT_OK
    assert "FILA" in saida and "PEND" in saida
    assert "tasks (1)" in saida
    assert "ESTADO" in saida


def test_status_por_prefixo_do_id(data_dir: Path) -> None:
    """Aceita o prefixo do id, como ``git rev-parse`` faz."""
    _, saida_submit = rodar(
        ["--data-dir", str(data_dir), "--modules", "demo_tasks", "submit", "demo.ok"]
    )
    task_id = saida_submit.split("id        ")[1].split()[0]

    _, saida = rodar(["--data-dir", str(data_dir), "status", task_id[:8]])

    assert "demo.ok" in saida


def test_tasks_lista_registradas(data_dir: Path) -> None:
    """``tasks`` mostra as tasks do módulo importado via ``--modules``."""
    codigo, saida = rodar(["--data-dir", str(data_dir), "--modules", "demo_tasks", "tasks"])

    assert codigo == EXIT_OK
    assert "demo.flaky" in saida
    assert "demo.lenta" in saida
    assert "ASSINATURA" in saida


def test_monitor_once_e_deterministico(data_dir: Path) -> None:
    """``monitor --once`` imprime um quadro e sai, sem sequências ANSI de reposicionamento."""
    rodar(["--data-dir", str(data_dir), "--modules", "demo_tasks", "submit", "demo.ok"])

    codigo, saida = rodar(["--data-dir", str(data_dir), "monitor", "--once"])

    assert codigo == EXIT_OK
    assert "monitor" in saida
    assert "PEND" in saida
    assert "\033[" not in saida


def test_worker_drain_processa_a_fila(data_dir: Path) -> None:
    """``worker --drain`` executa as tasks enfileiradas e encerra sozinho."""
    rodar(["--data-dir", str(data_dir), "--modules", "demo_tasks", "submit", "demo.ok"])
    rodar(["--data-dir", str(data_dir), "--modules", "demo_tasks", "submit", "demo.echo"])

    codigo, saida = rodar(
        [
            "--data-dir",
            str(data_dir),
            "--modules",
            "demo_tasks",
            "worker",
            "-c",
            "2",
            "--queue",
            "default",
            "--drain",
        ]
    )

    assert codigo == EXIT_OK
    assert "2 worker(s)" in saida
    assert "resumo: 2 ok" in saida

    _, saida_status = rodar(["--data-dir", str(data_dir), "status", "--json"])
    estados = {tarefa["state"] for tarefa in json.loads(saida_status)}
    assert estados == {TaskState.SUCCESS.value}


def test_worker_max_idle_encerra_sem_trabalho(data_dir: Path) -> None:
    """``--max-idle 0`` encerra imediatamente quando não há nada para fazer."""
    codigo, saida = rodar(
        ["--data-dir", str(data_dir), "worker", "-c", "1", "--max-idle", "0"]
    )

    assert codigo == EXIT_OK
    assert "encerrando" in saida


def test_dlq_list_e_requeue(data_dir: Path) -> None:
    """``dlq list`` mostra a fila de mortos e ``dlq requeue`` devolve a task."""
    _, saida = rodar(
        [
            "--data-dir",
            str(data_dir),
            "--modules",
            "demo_tasks",
            "submit",
            "demo.boom",
            "--queue",
            "demo",
            "--max-retries",
            "0",
        ]
    )
    task_id = saida.split("id        ")[1].split()[0]
    rodar(
        [
            "--data-dir",
            str(data_dir),
            "--modules",
            "demo_tasks",
            "worker",
            "-c",
            "1",
            "--queue",
            "demo",
            "--drain",
        ]
    )

    codigo, listagem = rodar(["--data-dir", str(data_dir), "dlq", "list"])
    assert codigo == EXIT_OK
    assert "dead letter queue" in listagem
    assert "tasks mortas (1)" in listagem

    codigo, reenfileirada = rodar(["--data-dir", str(data_dir), "dlq", "requeue", task_id])
    assert codigo == EXIT_OK
    assert "reenfileirada" in reenfileirada

    _, depois = rodar(["--data-dir", str(data_dir), "dlq", "list"])
    assert "tasks mortas (0)" in depois


def test_dlq_purge(data_dir: Path) -> None:
    """``dlq purge`` remove as tasks mortas do ledger."""
    rodar(
        [
            "--data-dir",
            str(data_dir),
            "--modules",
            "demo_tasks",
            "submit",
            "demo.boom",
            "--queue",
            "demo",
            "--max-retries",
            "0",
        ]
    )
    rodar(
        [
            "--data-dir",
            str(data_dir),
            "--modules",
            "demo_tasks",
            "worker",
            "-c",
            "1",
            "--queue",
            "demo",
            "--drain",
        ]
    )

    codigo, saida = rodar(["--data-dir", str(data_dir), "dlq", "purge"])

    assert codigo == EXIT_OK
    assert "1 task(s) removida" in saida


def test_cron_next_e_check(data_dir: Path) -> None:
    """``cron next`` lista as execuções futuras e ``cron check`` valida a expressão."""
    codigo, saida = rodar(["--data-dir", str(data_dir), "cron", "next", "*/5 * * * *", "--count", "3"])

    assert codigo == EXIT_OK
    assert "próximas 3 execuções" in saida
    assert saida.count("em ") >= 3

    codigo, validada = rodar(["--data-dir", str(data_dir), "cron", "check", "*/5 * * * *"])
    assert codigo == EXIT_OK
    assert "expressão válida" in validada


def test_cron_invalido_sai_com_erro(data_dir: Path) -> None:
    """Expressão cron inválida sai com código 1 e mensagem do campo."""
    erros = io.StringIO()
    codigo = main(
        [*BASE, "--data-dir", str(data_dir), "cron", "check", "99 * * * *"],
        out=io.StringIO(),
        err=erros,
    )

    assert codigo == EXIT_ERROR
    assert "'minuto'" in erros.getvalue()


def test_sem_subcomando_sai_com_codigo_de_uso(data_dir: Path) -> None:
    """Chamar a CLI sem subcomando explica o uso."""
    erros = io.StringIO()
    codigo = main([*BASE, "--data-dir", str(data_dir)], out=io.StringIO(), err=erros)

    assert codigo == EXIT_USAGE
    assert "subcomando" in erros.getvalue()


def test_help_encerra_com_codigo_zero() -> None:
    """``--help`` funciona normalmente."""
    with pytest.raises(SystemExit) as saida:
        main(["--help"])
    assert saida.value.code == 0


def test_subcomando_desconhecido_encerra_com_dois(data_dir: Path) -> None:
    """Subcomando inexistente sai com o código 2 do argparse."""
    with pytest.raises(SystemExit) as saida:
        main(["--data-dir", str(data_dir), "inexistente"])
    assert saida.value.code == EXIT_USAGE


def test_formatadores_de_tabela_e_duracao() -> None:
    """Os formatadores da CLI produzem strings estáveis (base do ``monitor --once``)."""
    tabela = render_table(["A", "B"], [["x", "1"], ["yy", "22"]], aligns=["l", "r"])

    assert tabela.splitlines()[0] == "  +---+---+"
    assert tabela.splitlines()[1].startswith("  A")
    assert tabela.splitlines()[3] == "  x   1"
    assert tabela.splitlines()[-1] == tabela.splitlines()[0]

    assert format_duration(None) == "-"
    assert format_duration(142) == "142ms"
    assert format_duration(8100) == "8.1s"
    assert format_duration(123000) == "2m03s"


# ------------------------------------------------------------------ dashboard


def test_html_do_dashboard_e_autossuficiente() -> None:
    """A página do dashboard não depende de CDN nem de build."""
    html = build_dashboard_html()

    assert html.startswith("<!doctype html>")
    assert "http://" not in html and "https://" not in html
    assert "<script" in html and "WebSocket" in html
    assert "taskflow" in html
    assert "prefers-color-scheme" in html
    assert "prefers-reduced-motion" in html


async def test_snapshot_do_dashboard_contem_o_que_a_ui_precisa(
    broker: Broker
) -> None:
    """``build_state_snapshot`` entrega stats, tasks, DLQ e eventos."""
    await broker.submit("ok", "ana")
    executando = await broker.fetch("default", timeout=1.0)
    assert executando is not None
    await broker.ack(executando, "fim")

    snapshot = build_state_snapshot(broker)

    assert snapshot["stats"]["succeeded"] == 1
    assert len(snapshot["tasks"]) == 1
    assert snapshot["tasks"][0]["state"] == TaskState.SUCCESS.value
    assert snapshot["dlq"] == []
    assert {evento["type"] for evento in snapshot["events"]} >= {"enqueued", "started", "success"}
    assert json.dumps(snapshot)  # serializável para o WebSocket


def test_create_app_expoe_as_rotas_esperadas(config: Config, registry: TaskRegistry) -> None:
    """A aplicação do dashboard declara as rotas de HTML, API e WebSocket."""
    app = create_app(config=config, registry=registry, write=False)
    rotas = {getattr(rota, "path", "") for rota in app.routes}

    assert "/" in rotas
    assert "/api/state" in rotas
    assert "/api/tasks" in rotas
    assert "/api/dlq" in rotas
    assert "/api/dlq/{task_id}/requeue" in rotas
    assert "/health" in rotas
    assert "/ws" in rotas