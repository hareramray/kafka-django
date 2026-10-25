"""Offline regression checks for error and commit boundaries, not broker simulation."""
from unittest.mock import Mock
import pytest
from labtools import kafka


def message(value=b'{"number":1}', error=None):
    msg = Mock()
    msg.value.return_value = value
    msg.error.return_value = error
    msg.topic.return_value = "lab.test"
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    return msg


def test_decode_tombstone_and_json():
    assert kafka.decode(message()) == {"number": 1}
    assert kafka.decode(message(None)) is None


def test_defaults_disable_implicit_offset_advancement(monkeypatch):
    constructor = Mock()
    monkeypatch.setattr(kafka, "Consumer", constructor)
    kafka.consumer("lab.test")
    config = constructor.call_args.args[0]
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["auto.offset.reset"] == "earliest"


def test_consumer_closes_without_commit_after_partial_read(monkeypatch):
    c = Mock()
    monkeypatch.setattr(kafka, "consumer", Mock(return_value=c))
    monkeypatch.setattr(kafka, "poll_records", Mock(side_effect=TimeoutError("partial batch")))
    with pytest.raises(TimeoutError):
        kafka.collect("lab.test", count=3, commit=True)
    c.commit.assert_not_called()
    c.close.assert_called_once()


def test_commit_is_explicit_per_message_and_close_always_runs(monkeypatch):
    c, messages = Mock(), [message(), message()]
    monkeypatch.setattr(kafka, "consumer", Mock(return_value=c))
    monkeypatch.setattr(kafka, "poll_records", Mock(return_value=messages))
    assert kafka.collect("lab.test", count=2, commit=True) == messages
    assert c.commit.call_count == 2
    for call, msg in zip(c.commit.call_args_list, messages):
        assert call.kwargs == {"message": msg, "asynchronous": False}
    c.close.assert_called_once()


def test_publish_rejects_delivery_failure_even_when_queue_drains(monkeypatch):
    p = Mock()
    def enqueue(topic, **kwargs):
        kwargs["on_delivery"]("delivery failed", message())
    p.produce.side_effect = enqueue
    p.flush.return_value = 0
    monkeypatch.setattr(kafka, "producer", lambda: p)
    with pytest.raises(RuntimeError, match="1 delivery error"):
        kafka.publish("lab.test", [{"order_id": "one"}], key_field="order_id")


def test_publish_rejects_undrained_queue(monkeypatch):
    p = Mock()
    p.flush.return_value = 1
    monkeypatch.setattr(kafka, "producer", lambda: p)
    with pytest.raises(RuntimeError, match="1 pending"):
        kafka.publish("lab.test", [{"number": 1}])


def test_publish_serialization_failure_does_not_enqueue(monkeypatch):
    p = Mock()
    p.flush.return_value = 0
    monkeypatch.setattr(kafka, "producer", lambda: p)
    with pytest.raises(ValueError):
        kafka.publish("lab.test", [{"number": float("nan")}])
    p.produce.assert_not_called()


def test_invalid_topic_label_rejected_before_network(monkeypatch):
    constructor = Mock()
    monkeypatch.setattr(kafka, "AdminClient", constructor)
    with pytest.raises(ValueError):
        kafka.new_topic("invalid topic with spaces")
    constructor.assert_not_called()


def test_poll_records_preserves_caller_ownership():
    c = Mock()
    msg = message()
    c.poll.side_effect = [None, msg]
    assert kafka.poll_records(c, count=1, timeout=1) == [msg]
    c.commit.assert_not_called()
    c.close.assert_not_called()


def test_poll_records_reports_timeout():
    c = Mock()
    c.poll.return_value = None
    with pytest.raises(TimeoutError, match="received 0"):
        kafka.poll_records(c, count=1, timeout=0.001)

