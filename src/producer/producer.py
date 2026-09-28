import json
import time
import random
from kafka import KafkaProducer
from events import generate_payment_event

MERCHANTS = ["merchant_alpha", "merchant_beta", "merchant_gamma"]

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("🚀 Productor iniciado. Enviando eventos a 'events.v1'...")
    
    recent_event_ids = []
    count = 0

    try:
        while True:
            merchant_id = random.choice(MERCHANTS)
            
            # Simulador de probabilidades de anomalías
            rand_val = random.random()
            
            is_duplicate = False
            dup_id = None
            is_late = False

            if rand_val < 0.10 and recent_event_ids:
                # 10% probabilidad de inyectar un DUPLICADO
                is_duplicate = True
                dup_id = random.choice(recent_event_ids)
                print(f"⚠️ Inyectando DUPLICADO para event_id: {dup_id}")
            elif rand_val > 0.85:
                # 15% probabilidad de inyectar un EVENTO TARDÍO (LATE)
                is_late = True
                print(f"🐢 Inyectando EVENTO TARDÍO (>120s de atraso) para {merchant_id}")

            event = generate_payment_event(
                merchant_id=merchant_id, 
                is_duplicate=is_duplicate, 
                duplicate_event_id=dup_id, 
                is_late=is_late
            )

            if not is_duplicate:
                recent_event_ids.append(event["event_id"])
                if len(recent_event_ids) > 50:
                    recent_event_ids.pop(0)

            # Enviar a Kafka usand `key` para el particionado
            producer.send('events.v1', key=event["key"], value=event)
            count += 1
            
            print(f"[{count}] Enviado: {event['event_id']} | Key: {event['key']} | Time: {event['event_time']}")
            time.sleep(1) # Emitir 1 evento por segundo
            
    except KeyboardInterrupt:
        print("\n🛑 Productor detenido.")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()