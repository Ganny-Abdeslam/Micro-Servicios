# tests/conftest.py
import asyncio
import json
import types
import pytest
from unittest.mock import AsyncMock, Mock
import importlib

import aio_pika

@pytest.fixture(scope="session")
def event_loop():
    """Ensure pytest-asyncio uses a fresh event loop for the session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def orchestrator_module(tmp_path, monkeypatch):
    """
    Patch aio_pika.connect_robust to return mocked connection/channel/exchanges/queue.
    Import the target module (user-receiver/app/main.py) dynamically and start start_consumer
    in background. Expose:
      - module: the imported module
      - mocks: dict with mock objects (source_ex, target_ex, channel, queue)
      - captured_handler: the callback that will be bound via queue.consume
    """
    # Prepare mocks
    mock_source_ex = AsyncMock(name="source_exchange")
    mock_target_ex = AsyncMock(name="target_exchange")
    # publish is async
    mock_target_ex.publish = AsyncMock(name="publish")

    mock_queue = AsyncMock(name="queue")
    # We'll capture the handler passed to queue.consume here
    captured = {"handler": None}

    async def fake_consume(callback, no_ack=False):
        # store callback so tests can call it later
        captured["handler"] = callback
        return None

    mock_queue.consume = AsyncMock(side_effect=fake_consume)
    mock_queue.bind = AsyncMock(name="bind")

    mock_channel = AsyncMock(name="channel")
    # declare_exchange should return source_ex for SOURCE_EXCHANGE and target_ex for TARGET_EXCHANGE
    async def fake_declare_exchange(name, type_, durable=True):
        if name == "user.events":
            return mock_source_ex
        if name == "messaging.events":
            return mock_target_ex
        # default return a fresh AsyncMock
        m = AsyncMock(name=f"exchange_{name}")
        m.publish = AsyncMock()
        return m

    mock_channel.declare_exchange = AsyncMock(side_effect=fake_declare_exchange)
    mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
    mock_channel.set_qos = AsyncMock()
    mock_channel.close = AsyncMock()

    mock_connection = AsyncMock(name="connection")
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_connection.close = AsyncMock()

    async def fake_connect_robust(url):
        return mock_connection

    monkeypatch.setattr(aio_pika, "connect_robust", fake_connect_robust)

    # Import target module. Try common import paths; if not, load by file.
    candidates = [
        "user_receiver.app.main",
        "user_receiver.main",
        "user_receiver.app_main",
        "user_receiver.app.main",  # duplicate but harmless
        "app.main",
        "main",
    ]
    module = None
    for name in candidates:
        try:
            module = importlib.import_module(name)
            importlib.reload(module)
            break
        except Exception:
            module = None
            continue

    if module is None:
        # Fallback: try to import by path relative to working dir
        # This will usually work if tests are run from repo root and file path exists.
        try:
            spec = importlib.util.spec_from_file_location("orchestrator_main", "user-receiver/app/main.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as exc:
            raise ImportError(
                "No pude importar el módulo 'main'. Ajusta los candidatos en conftest.py. Error: " + str(exc)
            )

    # Start the consumer in background
    task = asyncio.create_task(module.start_consumer())

    # Wait a little so start_consumer reaches queue.consume and captured handler is set
    for _ in range(20):
        await asyncio.sleep(0.05)
        if captured["handler"] is not None:
            break
    else:
        # if handler not captured yet, cancel task and raise
        task.cancel()
        await asyncio.sleep(0)  # allow cancel
        raise RuntimeError("No se capturó la función handler desde queue.consume. start_consumer no inicializó correctamente.")

    # Expose useful objects for tests
    yield types.SimpleNamespace(
        module=module,
        mocks={
            "source_ex": mock_source_ex,
            "target_ex": mock_target_ex,
            "channel": mock_channel,
            "queue": mock_queue,
            "connection": mock_connection,
        },
        captured_handler=captured
    )

    # Teardown: cancel background task and close mocked connection/channel
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
