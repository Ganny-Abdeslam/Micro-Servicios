# user-receiver/app/main.py
import os
import json
from datetime import datetime
from typing import Literal, Dict

import aio_pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, Field
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator")

# Config desde env
RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbit:5672/")
EXCHANGE = os.getenv("EXCHANGE", "messaging.events")   # exchange donde publicar el correo listo
ROUTING_KEY = os.getenv("ROUTING_KEY", "messaging.send")  # routing key para que messaging lo consuma

# Acciones permitidas
ActionLiteral = Literal["registrar", "autenticacion", "recuperacion_claves", "actualizacion_claves"]

class UserIn(BaseModel):
    nombre: str
    apellido: str
    correo: EmailStr
    numero: str = Field(..., alias="número")
    tipo: str
    accion: ActionLiteral = Field(..., alias="acción")

    class Config:
        allow_population_by_field_name = True
        extra = "allow"

SUBJECTS: Dict[str, str] = {
    "registrar": "Registro completado",
    "autenticacion": "Notificación de autenticación",
    "recuperacion_claves": "Solicitud de recuperación de claves",
    "actualizacion_claves": "Actualización de claves realizada",
}

BODIES: Dict[str, str] = {
    "registrar": (
        "Hola {full_name},\n\n"
        "Gracias por registrarte en nuestro sistema. Tu cuenta ha sido creada correctamente.\n\n"
        "Si tienes alguna duda, responde a este correo.\n\n"
        "Saludos,\nEl equipo"
    ),
    "autenticacion": (
        "Hola {full_name},\n\n"
        "Se ha detectado una autenticación en tu cuenta. Si fuiste tú, ignora este mensaje. "
        "Si no reconoces esta actividad, por favor contacta soporte inmediatamente.\n\n"
        "Saludos,\nEl equipo"
    ),
    "recuperacion_claves": (
        "Hola {full_name},\n\n"
        "Hemos recibido una solicitud para recuperar tu contraseña. Si fuiste tú, sigue las instrucciones "
        "en la plataforma para restablecerla. Si no solicitaste esto, ignora el mensaje.\n\n"
        "Saludos,\nEl equipo"
    ),
    "actualizacion_claves": (
        "Hola {full_name},\n\n"
        "Te confirmamos que la contraseña asociada a tu cuenta ha sido actualizada correctamente. "
        "Si no realizaste este cambio, contacta soporte de inmediato.\n\n"
        "Saludos,\nEl equipo"
    ),
}

app = FastAPI(title="orchestrator")

# RabbitMQ globals
rabbit_conn: aio_pika.RobustConnection | None = None
rabbit_channel: aio_pika.RobustChannel | None = None
exchange: aio_pika.Exchange | None = None

@app.on_event("startup")
async def startup():
    global rabbit_conn, rabbit_channel, exchange
    logger.info("Conectando a RabbitMQ en %s", RABBIT_URL)
    rabbit_conn = await aio_pika.connect_robust(RABBIT_URL)
    rabbit_channel = await rabbit_conn.channel()
    exchange = await rabbit_channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    logger.info("Exchange declarado: %s (rk=%s)", EXCHANGE, ROUTING_KEY)

@app.on_event("shutdown")
async def shutdown():
    global rabbit_channel, rabbit_conn
    logger.info("Cerrando conexión RabbitMQ")
    if rabbit_channel:
        await rabbit_channel.close()
    if rabbit_conn:
        await rabbit_conn.close()

def render_subject(action: str) -> str:
    return SUBJECTS.get(action, f"Notificación: {action}")

def render_body(action: str, full_name: str) -> str:
    template = BODIES.get(action)
    if template:
        return template.format(full_name=full_name)
    return f"Hola {full_name},\n\nSe ha producido la acción: {action}.\n\nSaludos,\nEl equipo"

@app.post("/users", status_code=202)
async def receive_user(payload: dict):
    try:
        user = UserIn.model_validate(payload)
    except Exception as exc:
        logger.warning("Payload inválido: %s", exc)
        raise HTTPException(status_code=400, detail={"error": "payload inválido", "details": str(exc)})

    # Construir datos
    full_name = f"{user.nombre} {user.apellido}"
    action_alias = user.model_dump(by_alias=True).get("acción")
    para = str(user.correo)
    asunto = render_subject(action_alias)
    cuerpo = render_body(action_alias, full_name)

    message = {
        "para": para,
        "asunto": asunto,
        "cuerpo": cuerpo,
        "meta": {
            "nombre": user.nombre,
            "apellido": user.apellido,
            "correo": str(user.correo),
            "número": user.model_dump(by_alias=True).get("número"),
            "tipo": user.tipo,
            "acción": action_alias,
            "recibidoAt": datetime.utcnow().isoformat() + "Z"
        }
    }

    # Publicar mensaje listo para envío
    try:
        if not exchange:
            raise RuntimeError("Exchange no disponible")
        msg = aio_pika.Message(
            body=json.dumps(message, ensure_ascii=False).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await exchange.publish(msg, routing_key=ROUTING_KEY)
        logger.info("Publicado correo listo -> para=%s asunto=%s action=%s", para, asunto, action_alias)
        return {"status": "published", "para": para, "asunto": asunto}
    except Exception as e:
        logger.exception("Error publicando en RabbitMQ: %s", e)
        raise HTTPException(status_code=500, detail="error publicando en broker")
