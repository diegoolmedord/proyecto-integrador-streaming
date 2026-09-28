import random
import uuid
from datetime import datetime, timezone

_last_generated_event = None
MERCHANTS = ["merchant_alpha", "merchant_beta", "merchant_gamma"]
STATUSES = ["CONFIRMED", "CONFIRMED", "CONFIRMED", "PENDING", "REJECTED"]

def generate_payment_event(merchant_id=None, is_duplicate=False, **kwargs):
    global _last_generated_event

    if is_duplicate and _last_generated_event:
        dup_event = dict(_last_generated_event)
        if "duplicate_event_id" in kwargs:
            dup_event["event_id"] = kwargs["duplicate_event_id"]
        return dup_event

    if not merchant_id:
        merchant_id = random.choice(MERCHANTS)

    # Manejar marcas de tiempo personalizadas (como eventos tardíos) si se envían
    custom_time = kwargs.get("custom_timestamp") or kwargs.get("event_time")
    if isinstance(custom_time, datetime):
        now_utc = custom_time
    else:
        now_utc = datetime.now(timezone.utc)

    event_id = kwargs.get("duplicate_event_id") or str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "event_time": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "key": merchant_id,
        "payload": {
            "merchant_id": merchant_id,
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(STATUSES),
            "currency": "USD"
        }
    }

    _last_generated_event = event
    return event