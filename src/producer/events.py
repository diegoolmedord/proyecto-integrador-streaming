import json
import random
import uuid
from datetime import datetime, timezone, timedelta

def generate_payment_event(merchant_id: str, is_duplicate: bool = False, duplicate_event_id: str = None, is_late: bool = False) -> dict:
    """
    Genera un evento de pago cumpliendo el contrato de dominio.
    Soporta inyección deliberada de duplicados y eventos tardíos.
    """
    now_utc = datetime.now(timezone.utc)
    
    # Manejo del tiempo de evento (Event Time)
    if is_late:
        # Evento tardío: timestamp retrasado 150 segundos en el pasado
        event_time = now_utc - timedelta(seconds=150)
    else:
        event_time = now_utc
        
    # Manejo de la identidad (event_id)
    if is_duplicate and duplicate_event_id:
        event_id = duplicate_event_id
    else:
        event_id = f"evt_{uuid.uuid4().hex[:12]}"

    event = {
        "schema_version": "1.0",
        "event_id": event_id,
        "key": merchant_id,  # Clave de negocio y de particionamiento
        "event_time": event_time.isoformat(),
        "emitted_at": now_utc.isoformat(),
        "payload": {
            "merchant_id": merchant_id,
            "amount": round(random.uniform(10.0, 500.0), 2),
            "currency": "USD",
            "status": random.choice(["CONFIRMED", "CONFIRMED", "CONFIRMED", "REJECTED"]) # 75% confirmados
        }
    }
    return event