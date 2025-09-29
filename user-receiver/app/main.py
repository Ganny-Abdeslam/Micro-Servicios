# user-receiver/app/main.py
import os
import json
import asyncio
from datetime import datetime
from typing import Dict, Any
import logging

import aio_pika

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator_consumer")

# Config desde env (ajusta nombres si lo prefieres)
RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbit:5672/")
SOURCE_EXCHANGE = os.getenv("SOURCE_EXCHANGE", "user.events")   # exchange donde publica user-service
SOURCE_BINDING = os.getenv("SOURCE_BINDING", "user.#")         # binding key para recibir eventos user.*
TARGET_EXCHANGE = os.getenv("EXCHANGE", "messaging.events")    # exchange al que publicamos el correo listo
TARGET_ROUTING_KEY = os.getenv("ROUTING_KEY", "messaging.send")

QUEUE_NAME = os.getenv("QUEUE_NAME", "orchestrator.user.events")

# Mapeo acción -> asunto y cuerpo (puedes extender)
SUBJECTS: Dict[str, str] = {
    "user.registered": "Registro completado",
    "user.login": "Notificación de autenticación",
    "user.recovery.link": "Solicitud de recuperación de claves",
    "user.password.updated": "Actualización de claves realizada",
}

BODIES: Dict[str, str] = {
    "user.registered": (
        "Hola {full_name},\n\n"
        "Gracias por registrarte en nuestro sistema. Tu cuenta ha sido creada correctamente.\n\n"
        "Si tienes alguna duda, responde a este correo.\n\n"
        "Saludos,\nEl equipo"
    ),
    "user.login": (
        "Hola {full_name},\n\n"
        "Se ha detectado una autenticación en tu cuenta. Si fuiste tú, ignora este mensaje. "
        "Si no reconoces esta actividad, por favor contacta soporte inmediatamente.\n\n"
        "Saludos,\nEl equipo"
    ),
    "user.recovery.link": (
        "Hola {full_name},\n\n"
        "Hemos recibido una solicitud para recuperar tu contraseña. Si fuiste tú, sigue las instrucciones "
        "en la plataforma para restablecerla. Si no solicitaste esto, ignora el mensaje.\n\n"
        "Saludos,\nEl equipo"
    ),
    "user.password.updated": (
        "Hola {full_name},\n\n"
        "Te confirmamos que la contraseña asociada a tu cuenta ha sido actualizada correctamente. "
        "Si no realizaste este cambio, contacta soporte de inmediato.\n\n"
        "Saludos,\nEl equipo"
    ),
}

def render_subject(action: str) -> str:
    return SUBJECTS.get(action, f"Notificación: {action}")

def render_body(action: str, full_name: str) -> str:
    tpl = BODIES.get(action)
    if tpl:
        return tpl.format(full_name=full_name)
    return f"Hola {full_name},\n\nSe ha producido la acción: {action}.\n\nSaludos,\nEl equipo"

async def connect_with_retry(url: str, max_retries: int = 10, delay_seconds: int = 3) -> aio_pika.RobustConnection:
    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info("🔁 Intento %d: conectando a RabbitMQ en %s", attempt, url)
            connection = await aio_pika.connect_robust(url)
            logger.info("✅ Conectado a RabbitMQ en el intento %d", attempt)
            return connection
        except Exception as e:
            logger.warning("⚠️  Fallo intento %d de conexión a RabbitMQ: %s", attempt, e)
            last_exception = e
            await asyncio.sleep(delay_seconds)

    logger.error("❌ No se pudo conectar a RabbitMQ después de %d intentos", max_retries)
    raise last_exception

async def start_consumer():
    logger.info("Conectando a RabbitMQ %s", RABBIT_URL)
    connection = await connect_with_retry(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    # Declarar exchange origen (topic) - asegúrate que el publisher use el mismo exchange
    source_ex = await channel.declare_exchange(SOURCE_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    # Declarar exchange destino donde publicar el correo listo
    target_ex = await channel.declare_exchange(TARGET_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)

    # Declarar cola y bind
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(source_ex, SOURCE_BINDING)

    logger.info("Esperando mensajes en queue=%s binding=%s", QUEUE_NAME, SOURCE_BINDING)

    async def handle(message: aio_pika.IncomingMessage):
        async with message.process(requeue=False):
            try:
                raw = message.body.decode("utf-8")
                event = json.loads(raw)
                logger.info("Evento recibido: %s", event.get("action"))

                # Estructura esperada: { "action": "...", "user": { id, name, lastName, email, phone }, "timestamp": ... }
                action = event.get("action") or event.get("accion") or event.get("type")
                user_obj = event.get("user") or event.get("usuario") or {}

                # seguridad: normalizar nombres de campo del publisher Go
                name = user_obj.get("name") or user_obj.get("nombre") or user_obj.get("firstName") or ""
                last_name = user_obj.get("lastName") or user_obj.get("apellido") or user_obj.get("last_name") or ""
                email = user_obj.get("email") or user_obj.get("correo")
                phone = user_obj.get("phone") or user_obj.get("phoneNumber") or user_obj.get("telefono") or ""

                full_name = f"{name} {last_name}".strip()

                # Construir email listo
                subject = render_subject(action or "unknown")
                body = render_body(action or "unknown", full_name)
                message_payload = {
                    "email": email,
                    "affair": subject,
                    "body": body,
                    "number": phone,
                    "meta": {
                        "id": user_obj.get("id"),
                        "name": name,
                        "lastName": last_name,
                        "email": email,
                        "phone": phone,
                        "action": action,
                        "timestamp": event.get("timestamp") or datetime.utcnow().isoformat() + "Z",
                        "receivedAt": datetime.utcnow().isoformat() + "Z"
                    }
                }

                # publicar en exchange destino con routing key TARGET_ROUTING_KEY
                msg = aio_pika.Message(
                    body=json.dumps(message_payload, ensure_ascii=False).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                )
                await target_ex.publish(msg, routing_key=TARGET_ROUTING_KEY)
                logger.info("Publicado mensaje a %s (routing=%s) para=%s action=%s", TARGET_EXCHANGE, TARGET_ROUTING_KEY, email, action)
            except Exception as e:
                logger.exception("Error procesando evento: %s", e)
                # si hay error grave, no reenviamos (evita loops), pero podrías ch.nack con requeue=True
                return

    await queue.consume(handle, no_ack=False)
    # Mantener corriendo
    try:
        await asyncio.Future()
    finally:
        await channel.close()
        await connection.close()

if __name__ == "__main__":
    asyncio.run(start_consumer())
