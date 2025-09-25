# tests/test_orchestrator.py
import pytest
import json
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_post_users_valid_publishes(app_module):
    main = app_module
    app = main.app  # FastAPI app

    # valid payload (usa "número" y "acción" con tilde)
    payload = {
        "nombre": "María",
        "apellido": "Gómez",
        "correo": "maria.gomez@example.com",
        "número": "+573001112233",
        "tipo": "cliente",
        "acción": "registrar"
    }

    async with AsyncClient(app=app, base_url="http://testserver") as ac:
        resp = await ac.post("/users", json=payload)

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "published"
    assert body["para"] == payload["correo"] or body.get("para") == payload["correo"] or body.get("correo") == payload["correo"] or body.get("para") == payload["correo"]

    # Ahora verificar que exchange.publish fue llamado y con el mensaje correcto
    mock_exchange = main._test_mock.mock_exchange
    # publish debe haber sido awaitable al menos 1 vez
    assert mock_exchange.publish.await_count == 1

    # Examinar argumento: el primer call tiene args (msg, routing_key=...)
    call_args = mock_exchange.publish.await_args_list[0]
    publish_msg = call_args.args[0]  # aio_pika.Message instance passed by main
    # extraer body bytes -> decode y parse JSON
    body_bytes = publish_msg.body
    published = json.loads(body_bytes.decode("utf-8"))
    # Verificar estructura
    assert "para" in published
    assert published["para"] == payload["correo"]
    assert "asunto" in published
    assert "cuerpo" in published
    assert "meta" in published
    assert published["meta"]["nombre"] == payload["nombre"]
    assert published["meta"]["apellido"] == payload["apellido"]
    assert published["meta"]["correo"] == payload["correo"]
    assert published["meta"]["número"] == payload["número"]
    assert published["meta"]["acción"] == payload["acción"]

@pytest.mark.asyncio
async def test_post_users_invalid_returns_400(app_module):
    main = app_module
    app = main.app

    # invalid missing required field 'correo'
    payload = {
        "nombre": "José",
        "apellido": "Perez",
        "número": "+57...",
        "tipo": "cliente",
        "acción": "registrar"
    }

    from httpx import AsyncClient
    async with AsyncClient(app=app, base_url="http://testserver") as ac:
        resp = await ac.post("/users", json=payload)

    assert resp.status_code == 400
    data = resp.json()
    assert "error" in data["detail"] or "payload inválido" in str(data["detail"])
