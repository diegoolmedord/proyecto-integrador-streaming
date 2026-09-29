import random
import uuid
from datetime import datetime, timezone, timedelta


MERCHANTS = [
    "merchant_alpha",
    "merchant_beta",
    "merchant_gamma"
]

STATUSES = [
    "CONFIRMED",
    "CONFIRMED",
    "CONFIRMED",
    "PENDING",
    "REJECTED"
]

# Guarda los últimos eventos generados para poder
# reutilizarlos correctamente cuando se inyecta un duplicado.
_recent_events = {}


def generate_payment_event(
    merchant_id=None,
    is_duplicate=False,
    duplicate_event_id=None,
    is_late=False,
    **kwargs
):
    """
    Genera un evento de pago para el pipeline de streaming.

    Parámetros:
        merchant_id:
            Identificador del comercio.

        is_duplicate:
            Si es True, reutiliza un evento existente.

        duplicate_event_id:
            ID del evento que debe duplicarse.

        is_late:
            Si es True, genera un evento cuyo event_time
            está 120 segundos atrasado.

    Retorna:
        Diccionario con el evento de pago.
    """

    # ---------------------------------------------------------
    # DUPLICADO
    # ---------------------------------------------------------
    if is_duplicate and duplicate_event_id:
        original_event = _recent_events.get(duplicate_event_id)

        if original_event:
            return dict(original_event)

    # ---------------------------------------------------------
    # MERCHANT
    # ---------------------------------------------------------
    if not merchant_id:
        merchant_id = random.choice(MERCHANTS)

    # ---------------------------------------------------------
    # TIMESTAMP
    # ---------------------------------------------------------
    custom_time = (
        kwargs.get("custom_timestamp")
        or kwargs.get("event_time")
    )

    if isinstance(custom_time, datetime):
        event_time = custom_time

    else:
        event_time = datetime.now(timezone.utc)

        # Simular evento tardío:
        # el evento llega ahora, pero su event_time
        # corresponde a 120 segundos atrás.
        if is_late:
            event_time = event_time - timedelta(seconds=120)

    # ---------------------------------------------------------
    # EVENT ID
    # ---------------------------------------------------------
    event_id = str(uuid.uuid4())

    # ---------------------------------------------------------
    # EVENTO
    # ---------------------------------------------------------
    event = {
        "event_id": event_id,
        "event_time": event_time.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "merchant_id": merchant_id,
        "key": merchant_id,
        "payload": {
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(STATUSES),
            "currency": "USD"
        }
    }

    # Guardar para futuras simulaciones de duplicados.
    _recent_events[event_id] = dict(event)

    # Mantener solamente los últimos 50 eventos.
    if len(_recent_events) > 50:
        oldest_event_id = next(iter(_recent_events))
        del _recent_events[oldest_event_id]

    return event
