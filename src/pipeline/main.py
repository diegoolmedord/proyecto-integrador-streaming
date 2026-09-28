import json
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AccumulationMode
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka  # <--- IMPORTACIÓN DIRECTA

logging.basicConfig(level=logging.INFO)

# --- 1. PARSEO Y ASIGNACIÓN DE EVENT TIME ---
class ParseAndTimestampDoFn(beam.DoFn):
    """
    Parsea el JSON y asigna el event_time del dominio como timestamp de Beam.
    Eventos corruptos se ignoran o se envían a un registro de errores.
    """
    def process(self, element):
        try:
            # Si el mensaje viene directamente de Kafka o un mock
            payload_str = element[1].decode('utf-8') if isinstance(element, tuple) else element
            data = json.loads(payload_str)
            
            # Convertir event_time ISO a Unix Timestamp (segundos)
            dt = datetime.fromisoformat(data["event_time"].replace("Z", "+00:00"))
            timestamp = dt.timestamp()
            
            # Emitir tupla (key, event_data) asociada a su Event Time real
            yield TimestampedValue((data["key"], data), timestamp)
        except Exception as e:
            logging.error(f"Error parseando evento: {e}")

# --- 2. DEDUPLICACIÓN Y AGREGACIÓN INCREMENTAL ---
class AggregatePaymentsFn(beam.CombineFn):
    """
    CombineFn personalizado para deduplicar por event_id dentro de la ventana
    y calcular los totales del comercio.
    """
    def create_accumulator(self):
        # Estado inicial del acumulador para una ventana/clave
        return {
            "seen_event_ids": set(),
            "total_amount": 0.0,
            "count": 0,
            "confirmed_count": 0
        }

    def add_input(self, accumulator, input_element):
        key, event = input_element
        event_id = event["event_id"]

        # DEDUPLICACIÓN: Si el event_id ya fue procesado en esta ventana, se ignora
        if event_id in accumulator["seen_event_ids"]:
            logging.info(f"⚡ DEDUPLICADO FILTRADO EN BEAM: {event_id} para {key}")
            return accumulator

        # Registrar el ID del evento para no repetirlo
        accumulator["seen_event_ids"].add(event_id)
        
        # Agregación de negocio
        payload = event["payload"]
        accumulator["count"] += 1
        if payload.get("status") == "CONFIRMED":
            accumulator["total_amount"] += payload.get("amount", 0.0)
            accumulator["confirmed_count"] += 1

        return accumulator

    def merge_accumulators(self, accumulators):
        # Combinar acumuladores parciales (para paralelismo)
        merged = self.create_accumulator()
        for accum in accumulators:
            for evt_id in accum["seen_event_ids"]:
                if evt_id not in merged["seen_event_ids"]:
                    merged["seen_event_ids"].add(evt_id)
            merged["count"] += accum["count"]
            merged["confirmed_count"] += accum["confirmed_count"]
            merged["total_amount"] += accum["total_amount"]
        return merged

    def extract_output(self, accumulator):
        return {
            "total_amount": round(accumulator["total_amount"], 2),
            "total_events": accumulator["count"],
            "confirmed_events": accumulator["confirmed_count"]
        }

# --- 3. FORMATO DE SALIDA IDEMPOTENTE (UPSERT) ---
def format_idempotent_output(element, window=beam.DoFn.WindowParam, pane_info=beam.DoFn.PaneInfoParam):
    """
    Estructura el resultado final asignando una clave determinista para garantias de UPSERT.
    idempotency_key = merchant_id|window_start|window_end
    """
    merchant_id, metrics = element
    
    start_str = window.start.to_utc_datetime().isoformat()
    end_str = window.end.to_utc_datetime().isoformat()
    
    # Clave de idempotencia única por ventana y entidad
    idempotency_key = f"{merchant_id}|{start_str}|{end_str}"
    
    output_record = {
        "idempotency_key": idempotency_key,
        "merchant_id": merchant_id,
        "window_start": start_str,
        "window_end": end_str,
        "pane_index": pane_info.index,
        "is_final": pane_info.is_last,
        "metrics": metrics
    }
    
    # Formato para Kafka: Tupla (Key, Value)
    return (idempotency_key, json.dumps(output_record))


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            # 1. Lectura desde Kafka (Uso directo de ReadFromKafka)
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'beam-streaming-group',
                    'auto.offset.reset': 'latest'
                },
                topics=['events.v1']
            )
            
            # 2. Parsear JSON y asignar Event Time real
            | "ParseAndTimestamp" >> beam.ParDo(ParseAndTimestampDoFn())
            
            # 3. Asignar Ventanas Fijas de 60s con Allowed Lateness de 120s
            | "FixedWindows" >> beam.WindowInto(
                FixedWindows(60),
                allowed_lateness=120,
                trigger=AfterWatermark(late=AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING
            )
            
            # 4. Agregación e Inmunización ante Duplicados
            | "AggregatePayments" >> beam.CombinePerKey(AggregatePaymentsFn())
            
            # 5. Formatear con Clave de Idempotencia
            | "FormatOutput" >> beam.Map(format_idempotent_output)
            
            # 6. Escritura a Kafka (Uso directo de WriteToKafka)
            | "WriteToKafka" >> WriteToKafka(
                producer_config={'bootstrap.servers': 'localhost:9092'},
                topic='aggregates.v1'
            )
        )

if __name__ == '__main__':
    run()