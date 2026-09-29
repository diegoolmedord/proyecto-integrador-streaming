import json
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


# ============================================================
# CONFIGURACIÓN
# ============================================================

KAFKA_BOOTSTRAP = "localhost:9092"

INPUT_TOPIC = "events.v1"
OUTPUT_TOPIC = "aggregates.v1"

KAFKA_GROUP_ID = "streaming-pipeline-stateful-v2"

WINDOW_SIZE_SECONDS = 60

# Tiempo máximo permitido para eventos tardíos
ALLOWED_LATENESS_SECONDS = 120

BATCH_SIZE = 20
POLL_TIMEOUT = 1.0
BATCH_TIMEOUT = 5.0


# ============================================================
# ESTADO LOCAL
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

STATE_DB = PROJECT_ROOT / "streaming_state.db"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# KAFKA READER
# ============================================================

class KafkaBatchReader:

    def __init__(self):
        self.consumer = None

    def start(self):

        from confluent_kafka import Consumer

        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })

        self.consumer.subscribe([INPUT_TOPIC])

        logging.info(
            f"[Kafka] Consumidor iniciado. "
            f"Tópico: {INPUT_TOPIC}"
        )

    def read_batch(self):

        events = []

        start_time = time.time()

        while len(events) < BATCH_SIZE:

            elapsed = time.time() - start_time

            if elapsed >= BATCH_TIMEOUT:
                break

            msg = self.consumer.poll(POLL_TIMEOUT)

            if msg is None:
                continue

            if msg.error():

                logging.error(
                    f"[Kafka] Error: {msg.error()}"
                )

                continue

            try:

                value = msg.value()

                if isinstance(value, bytes):
                    value = value.decode("utf-8")

                event = json.loads(value)

                events.append(event)

                logging.info(
                    f"[Kafka] Recibido: "
                    f"{event.get('event_id')} | "
                    f"{event.get('merchant_id')} | "
                    f"{event.get('event_time')}"
                )

            except Exception as exc:

                logging.error(
                    f"[Kafka] Error procesando mensaje: {exc}"
                )

        return events

    def close(self):

        if self.consumer is not None:
            self.consumer.close()

        logging.info(
            "[Kafka] Consumidor cerrado."
        )


# ============================================================
# CONVERSIÓN A EVENT TIME
# ============================================================

class AddEventTimestamp(beam.DoFn):

    def process(self, event):

        try:

            event_time = datetime.fromisoformat(
                event["event_time"].replace("Z", "+00:00")
            )

            timestamp = event_time.timestamp()

            merchant_id = event.get(
                "merchant_id",
                "unknown_merchant"
            )

            yield beam.window.TimestampedValue(
                (merchant_id, event),
                timestamp
            )

        except Exception as exc:

            logging.error(
                f"[Timestamp] Error: {exc}"
            )


# ============================================================
# AGREGACIÓN Y DEDUPLICACIÓN
# ============================================================

class AggregatePaymentsFn(beam.CombineFn):

    def create_accumulator(self):

        return {
            "events": {}
        }

    def add_input(
        self,
        accumulator,
        input_element
    ):

        event_id = input_element.get("event_id")

        if not event_id:
            return accumulator

        if event_id in accumulator["events"]:

            logging.info(
                f"[Deduplicación] "
                f"Evento duplicado filtrado: {event_id}"
            )

            return accumulator

        accumulator["events"][event_id] = input_element

        return accumulator

    def merge_accumulators(
        self,
        accumulators
    ):

        merged = self.create_accumulator()

        for accumulator in accumulators:

            for event_id, event in accumulator["events"].items():

                if event_id not in merged["events"]:

                    merged["events"][event_id] = event

                else:

                    logging.info(
                        f"[Deduplicación] "
                        f"Duplicado detectado durante merge: "
                        f"{event_id}"
                    )

        return merged

    def extract_output(
        self,
        accumulator
    ):

        total_amount = 0.0
        total_events = 0
        confirmed_events = 0

        events = list(
            accumulator["events"].values()
        )

        for event in events:

            total_events += 1

            payload = event.get(
                "payload",
                {}
            )

            status = payload.get("status")

            amount = float(
                payload.get(
                    "amount",
                    0.0
                )
            )

            if status == "CONFIRMED":

                confirmed_events += 1

                total_amount += amount

        return {

            "total_amount": round(
                total_amount,
                2
            ),

            "total_events": total_events,

            "confirmed_events": confirmed_events,

            # Conservamos los eventos para que
            # el estado pueda acumularse entre batches.
            "events": events
        }


# ============================================================
# ESTADO PERSISTENTE DE VENTANAS
# ============================================================

class WindowStateStore:

    def __init__(self, database_path):

        self.database_path = str(
            database_path
        )

        self.connection = None

    def open(self):

        self.connection = sqlite3.connect(
            self.database_path,
            check_same_thread=False
        )

        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS window_events (

                event_id TEXT PRIMARY KEY,

                merchant_id TEXT NOT NULL,

                window_start TEXT NOT NULL,

                window_end TEXT NOT NULL,

                amount REAL NOT NULL,

                status TEXT NOT NULL,

                event_json TEXT NOT NULL

            )
            """
        )

        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_window_events_window
            ON window_events (
                merchant_id,
                window_start,
                window_end
            )
            """
        )

        self.connection.commit()

        logging.info(
            f"[Estado] SQLite iniciado: "
            f"{self.database_path}"
        )

    def add_events(
        self,
        merchant_id,
        window_start,
        window_end,
        events
    ):

        inserted = 0

        for event in events:

            event_id = event.get(
                "event_id"
            )

            if not event_id:
                continue

            payload = event.get(
                "payload",
                {}
            )

            amount = float(
                payload.get(
                    "amount",
                    0.0
                )
            )

            status = payload.get(
                "status",
                "UNKNOWN"
            )

            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO window_events (

                    event_id,
                    merchant_id,
                    window_start,
                    window_end,
                    amount,
                    status,
                    event_json

                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    merchant_id,
                    window_start,
                    window_end,
                    amount,
                    status,
                    json.dumps(event)
                )
            )

            if cursor.rowcount == 1:

                inserted += 1

            else:

                logging.info(
                    f"[Estado] Evento ya existente: "
                    f"{event_id}"
                )

        self.connection.commit()

        return inserted

    def calculate_metrics(
        self,
        merchant_id,
        window_start,
        window_end
    ):

        cursor = self.connection.execute(
            """
            SELECT

                COUNT(*),

                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'CONFIRMED'
                            THEN amount
                            ELSE 0
                        END
                    ),
                    0
                ),

                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'CONFIRMED'
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                )

            FROM window_events

            WHERE merchant_id = ?
              AND window_start = ?
              AND window_end = ?
            """,
            (
                merchant_id,
                window_start,
                window_end
            )
        )

        total_events, total_amount, confirmed_events = (
            cursor.fetchone()
        )

        return {

            "total_amount": round(
                float(total_amount),
                2
            ),

            "total_events": int(
                total_events
            ),

            "confirmed_events": int(
                confirmed_events
            )
        }

    def close(self):

        if self.connection is not None:

            self.connection.close()

            logging.info(
                "[Estado] SQLite cerrado."
            )

# ============================================================
# ESCRITURA EN KAFKA + UPSERT
# ============================================================

class WriteToKafkaDoFn(beam.DoFn):

    def setup(self):

        from confluent_kafka import Producer

        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP
        })

        self.state_store = WindowStateStore(
            STATE_DB
        )

        self.state_store.open()

        logging.info(
            "[Kafka] Productor de agregados iniciado."
        )

    def process(
        self,
        element,
        window=beam.DoFn.WindowParam
    ):

        merchant_id, partial_metrics = element

        window_start = str(
            window.start
        )

        window_end = str(
            window.end
        )

        idempotency_key = (
            f"{merchant_id}|"
            f"{window_start}|"
            f"{window_end}"
        )

        # ----------------------------------------------------
        # Incorporar eventos al estado persistente
        # ----------------------------------------------------

        events = partial_metrics.get(
            "events",
            []
        )

        inserted = self.state_store.add_events(
            merchant_id,
            window_start,
            window_end,
            events
        )

        # ----------------------------------------------------
        # Recalcular el agregado completo
        # ----------------------------------------------------

        metrics = self.state_store.calculate_metrics(
            merchant_id,
            window_start,
            window_end
        )

        output_record = {

            "idempotency_key": idempotency_key,

            "merchant_id": merchant_id,

            "window_start": window_start,

            "window_end": window_end,

            "metrics": metrics
        }

        # ----------------------------------------------------
        # Publicar nueva versión de la misma clave
        # ----------------------------------------------------

        self.producer.produce(

            OUTPUT_TOPIC,

            key=idempotency_key.encode(
                "utf-8"
            ),

            value=json.dumps(
                output_record
            ).encode(
                "utf-8"
            )
        )

        self.producer.flush()

        logging.info(
            "[UPSERT] "
            f"{merchant_id} | "
            f"Eventos nuevos: {inserted} | "
            f"Eventos acumulados: "
            f"{metrics['total_events']} | "
            f"Confirmados: "
            f"{metrics['confirmed_events']} | "
            f"Total: "
            f"${metrics['total_amount']} | "
            f"Ventana: "
            f"{window_start} - {window_end}"
        )

    def teardown(self):

        if hasattr(
            self,
            "producer"
        ):

            self.producer.flush()

        if hasattr(
            self,
            "state_store"
        ):

            self.state_store.close()

        logging.info(
            "[Kafka] Productor de agregados cerrado."
        )


# ============================================================
# PROCESAMIENTO DE UN LOTE
# ============================================================

def process_batch(events):

    if not events:

        logging.info(
            "[Beam] Sin eventos nuevos."
        )

        return

    logging.info(
        f"[Beam] Procesando lote de "
        f"{len(events)} eventos."
    )

    options = PipelineOptions([
        "--runner=PrismRunner",
        "--direct_num_workers=1",
    ])

    with beam.Pipeline(
        options=options
    ) as pipeline:

        (
            pipeline

            | "CrearEventos"
            >> beam.Create(events)

            | "AsignarEventTime"
            >> beam.ParDo(
                AddEventTimestamp()
            )

            | "Ventanas60Segundos"
            >> beam.WindowInto(

                FixedWindows(
                    WINDOW_SIZE_SECONDS
                ),

                allowed_lateness=(
                    ALLOWED_LATENESS_SECONDS
                )
            )

            | "AgruparPorComercio"
            >> beam.CombinePerKey(
                AggregatePaymentsFn()
            )

            | "EscribirAgregados"
            >> beam.ParDo(
                WriteToKafkaDoFn()
            )
        )

    logging.info(
        "[Beam] Lote procesado correctamente."
    )


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def run():

    reader = KafkaBatchReader()

    reader.start()

    logging.info(
        "=================================================="
    )

    logging.info(
        "PIPELINE DE STREAMING INICIADO"
    )

    logging.info(
        f"Entrada : {INPUT_TOPIC}"
    )

    logging.info(
        f"Salida  : {OUTPUT_TOPIC}"
    )

    logging.info(
        f"Ventana : "
        f"{WINDOW_SIZE_SECONDS} segundos"
    )

    logging.info(
        f"Allowed lateness : "
        f"{ALLOWED_LATENESS_SECONDS} segundos"
    )

    logging.info(
        f"Estado : {STATE_DB}"
    )

    logging.info(
        "=================================================="
    )

    try:

        while True:

            events = reader.read_batch()

            if events:

                process_batch(events)

            else:

                logging.info(
                    "[Streaming] "
                    "Esperando nuevos eventos..."
                )

                time.sleep(1)

    except KeyboardInterrupt:

        logging.info(
            "Pipeline detenido por el usuario."
        )

    finally:

        reader.close()


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    run()