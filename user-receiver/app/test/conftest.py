# tests/conftest.py
import asyncio
import json
import types
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock
import importlib
import os

import aio_pika

# pytest-asyncio requires this for legacy loop policy in some environments
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def app_module(monkeypatch):
    """
    Importa (o recarga) el módulo main del orquestador (user-receiver/app/main.py)
    pero parchea aio_pika.connect_robust para que no conecte a Rabbit real,
    y provee un 'exchange' mock que registra llamadas a publish.
    Retorna el módulo ya importado (main).
    """

    # Crear un mock exchange que tenga método async 'publish'
    mock_exchange = AsyncMock(name="exchange")
    # el método publish será awaitable (AsyncMock) — lo dejamos así para inspección
    mock_exchange.publish = AsyncMock(name="publish")

    # Crear un objeto channel stub con declare_exchange que devuelve mock_exchange
    async def fake_declare_exchange(name, type_, durable=True):
        return mock_exchange

    mock_channel = AsyncMock(name="channel")
    mock_channel.declare_exchange = AsyncMock(side_effect=fake_declare_exchange)

    # Crear una conexión stub cuya .channel() devuelve mock_channel
    async def fake_channel():
        return mock_channel

    mock_connection = AsyncMock(name="connection")
    mock_connection.channel = AsyncMock(return_value=mock_channel)

    # parchear aio_pika.connect_robust para que devuelva mock_connection
    async def fake_connect_robust(url):
        return mock_connection

    monkeypatch.setattr(aio_pika, "connect_robust", fake_connect_robust)

    # Importar (o recargar) el módulo main (orchestrator). Ajusta import path según tu proyecto.
    # Asumimos que el módulo está en user-receiver.app.main
    # Si la ruta difiere, ajusta aquí.
    module_name = "user-receiver.app.main".replace("-", "_")  # just in case; actual import below
    # import by file path: easier to import with package path used in your project.
    # We'll attempt to import 'user_receiver.app.main' or 'user_receiver.main' etc.
    # Try common names:
    candidates = [
        "user_receiver.app.main",
        "user_receiver.main",
        "user_receiver.app_main",
        "app.main",  # fallback if you placed differently
        "main"
    ]
    main = None
    for name in candidates:
        try:
            if name in importlib.sys.modules:
                importlib.reload(importlib.import_module(name))
            main = importlib.import_module(name)
            break
        except Exception:
            main = None
            continue

    if main is None:
        # Try to import by path relative to tests (common structure)
        try:
            main = importlib.import_module("user_receiver.app.main")
        except Exception as exc:
            raise ImportError(
                "No pude importar el módulo main del orquestador. Ajusta 'candidates' en conftest.py "
                "para que coincida con tu layout de paquetes. Error: " + str(exc)
            )

    # Wait a tick — when the test client triggers startup, our patched connect_robust will be used
    # Expose mocks so tests can inspect calls:
    main._test_mock = types.SimpleNamespace(
        mock_exchange=mock_exchange,
        mock_channel=mock_channel,
        mock_connection=mock_connection
    )

    yield main

    # cleanup: try to call shutdown if present
    try:
        if hasattr(main, "shutdown"):
            await main.shutdown()
    except Exception:
        pass
