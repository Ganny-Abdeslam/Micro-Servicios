# tests/test_orchestrator_consumer.py
import pytest
import json
import asyncio
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_handle_publishes_message_payload(orchestrator_module):
    module = orchestrator_module.module
    mocks = orchestrator_module.mocks
    handler = orchestrator_module.captured_handler["handler"]
    assert handler is not None

    # Construct a sample event that matches your Go publisher structure
    event = {
        "action": "user.registered",
        "user": {
            "id": 42,
            "name": "Juan",
            "lastName": "Pérez",
            "email": "juan.perez@example.com",
            "phone": "+573001112233"
        },
        "timestamp": "2025-09-28T12:00:00Z"
    }
    body = json.dumps(event, ensure_ascii=False).encode("utf-8")

    # Build a DummyMessage with .body and .process() async context manager
    class DummyMessage:
        def __init__(self, body):
            self.body = body

        def process(self, requeue=False):
            class Ctx:
                async def __aenter__(self_inner):
                    return None
                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False
            return Ctx()

    dummy = DummyMessage(body)

    # Call handler
    await handler(dummy)

    # Assert target_ex.publish was called once
    target_ex = mocks["target_ex"]
    assert target_ex.publish.await_count == 1

    # Inspect the published message payload
    publish_call = target_ex.publish.await_args_list[0]
    published_msg = publish_call.args[0]  # aio_pika.Message
    rk = publish_call.kwargs.get("routing_key") or (publish_call.args[1] if len(publish_call.args) > 1 else None)

    # message body is bytes -> decode -> json
    published_payload = json.loads(published_msg.body.decode("utf-8"))

    # Check fields
    assert published_payload["email"] == "juan.perez@example.com"
    assert "affair" in published_payload and published_payload["affair"] == "Registro completado"
    assert "body" in published_payload and "Juan Pérez" in published_payload["body"]
    assert published_payload["number"] == "+573001112233"
    assert published_payload["meta"]["id"] == 42
    assert published_payload["meta"]["action"] == "user.registered"

@pytest.mark.asyncio
async def test_handle_with_missing_fields_does_not_crash(orchestrator_module):
    module = orchestrator_module.module
    mocks = orchestrator_module.mocks
    handler = orchestrator_module.captured_handler["handler"]
    assert handler is not None

    # Event missing user.email and phone
    event = {
        "action": "user.login",
        "user": {
            "id": 7,
            "name": "Ana",
            "lastName": ""  # empty last name
        },
        "timestamp": "2025-09-28T12:05:00Z"
    }
    body = json.dumps(event).encode("utf-8")

    class DummyMessage:
        def __init__(self, body):
            self.body = body

        def process(self, requeue=False):
            class Ctx:
                async def __aenter__(self_inner):
                    return None
                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False
            return Ctx()

    dummy = DummyMessage(body)

    # Call handler - should not raise
    await handler(dummy)

    # Should still publish a message (with empty email/phone)
    target_ex = mocks["target_ex"]
    assert target_ex.publish.await_count >= 1
    published_msg = target_ex.publish.await_args_list[-1].args[0]
    payload = json.loads(published_msg.body.decode("utf-8"))

    # email and number may be None or empty
    assert "email" in payload
    assert "number" in payload
    # subject must map to login subject
    assert payload["affair"] == "Notificación de autenticación"
    # body must contain the name even if last name missing
    assert "Ana" in payload["body"]
