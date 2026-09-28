import pytest
from src.pipeline.main import AggregatePaymentsFn, ParseAndTimestampDoFn

def test_deduplication_in_combine_fn():
    """
    Verifica que si entran 2 eventos con el mismo event_id,
    el segundo sea ignorado por el acumulador.
    """
    fn = AggregatePaymentsFn()
    accum = fn.create_accumulator()

    event_1 = {
        "event_id": "evt_100",
        "payload": {"amount": 50.0, "status": "CONFIRMED"}
    }
    event_duplicate = {
        "event_id": "evt_100",
        "payload": {"amount": 50.0, "status": "CONFIRMED"}
    }
    event_2 = {
        "event_id": "evt_200",
        "payload": {"amount": 100.0, "status": "CONFIRMED"}
    }

    # Procesar primer evento
    accum = fn.add_input(accum, ("merchant_alpha", event_1))
    assert accum["count"] == 1
    assert accum["total_amount"] == 50.0

    # Procesar duplicado (debe ser ignorado)
    accum = fn.add_input(accum, ("merchant_alpha", event_duplicate))
    assert accum["count"] == 1
    assert accum["total_amount"] == 50.0  # No cambió el monto

    # Procesar evento legítimo distinto
    accum = fn.add_input(accum, ("merchant_alpha", event_2))
    assert accum["count"] == 2
    assert accum["total_amount"] == 150.0


def test_rejected_status_not_summed():
    """
    Verifica que eventos con estado REJECTED incrementen el conteo general
    pero no sumen al total monetario.
    """
    fn = AggregatePaymentsFn()
    accum = fn.create_accumulator()

    event_rejected = {
        "event_id": "evt_300",
        "payload": {"amount": 200.0, "status": "REJECTED"}
    }

    accum = fn.add_input(accum, ("merchant_beta", event_rejected))
    output = fn.extract_output(accum)

    assert output["total_events"] == 1
    assert output["confirmed_events"] == 0
    assert output["total_amount"] == 0.0