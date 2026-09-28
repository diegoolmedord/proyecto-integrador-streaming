import json
import logging
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.transforms.trigger import Repeatedly, AfterCount, AccumulationMode

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- 1. LECTOR CONTINUO DE KAFKA ---
class ReadKafkaContinuous(beam.DoFn):
    def process(self, element):
        from confluent_kafka import Consumer, KafkaError

        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'beam-streaming-group-final',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        consumer.subscribe(['events.v1'])
        logging.info("🚀 [Beam] Conectado a Kafka. Escuchando eventos en tiempo real...")

        try:
            while True:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logging.error(f"Error de Kafka: {msg.error()}")
                    continue

                try:
                    payload_str = msg.value().decode('utf-8')
                    data = json.loads(payload_str)

                    # Parsear timestamp del evento
                    dt = datetime.fromisoformat(data["event_time"].replace("Z", "+00:00"))
                    timestamp = dt.timestamp()

                    merchant_id = data.get("payload", {}).get("merchant_id", "unknown_merchant")

                    # Emitir el elemento con su marca de tiempo para las ventanas
                    yield TimestampedValue((merchant_id, data), timestamp)
                except Exception as e:
                    logging.error(f"Error procesando mensaje individual: {e}")
        finally:
            consumer.close()


# --- 2. DEDUPLICACIÓN Y AGREGACIÓN ---
class AggregatePaymentsFn(beam.CombineFn):
    def create_accumulator(self):
        return {
            "seen_event_ids": set(),
            "total_amount": 0.0,
            "count": 0,
            "confirmed_count": 0
        }

    def add_input(self, accumulator, input_element):
        event_id = input_element["event_id"]

        if event_id in accumulator["seen_event_ids"]:
            logging.info(f"⚡ [Deduplicación] Evento duplicado filtrado: {event_id}")
            return accumulator

        accumulator["seen_event_ids"].add(event_id)

        payload = input_element.get("payload", {})
        accumulator["count"] += 1
        if payload.get("status") == "CONFIRMED":
            accumulator["total_amount"] += payload.get("amount", 0.0)
            accumulator["confirmed_count"] += 1

        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for accum in accumulators:
            merged["seen_event_ids"].update(accum["seen_event_ids"])
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


# --- 3. ESCRITOR A KAFKA (SALIDA) ---
class WriteToKafkaDoFn(beam.DoFn):
    def setup(self):
        from confluent_kafka import Producer
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def process(self, element, window=beam.DoFn.WindowParam, pane_info=beam.DoFn.PaneInfoParam):
        merchant_id, metrics = element

        start_str = window.start.to_utc_datetime().isoformat()
        end_str = window.end.to_utc_datetime().isoformat()
        idempotency_key = f"{merchant_id}|{start_str}|{end_str}"

        output_record = {
            "idempotency_key": idempotency_key,
            "merchant_id": merchant_id,
            "window_start": start_str,
            "window_end": end_str,
            "metrics": metrics
        }

        self.producer.produce(
            'aggregates.v1',
            key=idempotency_key.encode('utf-8'),
            value=json.dumps(output_record).encode('utf-8')
        )
        self.producer.flush()
        logging.info(f"📤 [Agregación Escrita] {merchant_id} -> Total: ${metrics['total_amount']} ({metrics['total_events']} eventos)")


# --- 4. PIPELINE PRINCIPAL ---
def run():
    options = PipelineOptions(["--runner=DirectRunner"])
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "GeneradorInicio" >> beam.Create([1])  # Disparador inicial único que abre el lector infinito
            | "LeerKafka" >> beam.ParDo(ReadKafkaContinuous())
            | "VentanasFijas" >> beam.WindowInto(
                FixedWindows(60),
                trigger=Repeatedly(AfterCount(1)),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=86400
            )
            | "AgruparPorComercio" >> beam.CombinePerKey(AggregatePaymentsFn())
            | "EscribirKafka" >> beam.ParDo(WriteToKafkaDoFn())
        )


if __name__ == '__main__':
    logging.info("🚀 Iniciando Pipeline de Streaming Apache Beam...")
    run()