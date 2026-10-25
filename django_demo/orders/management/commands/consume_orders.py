import json
import time
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from labtools.kafka import consumer
from orders.services import apply_event, validate_topic


class Command(BaseCommand):
    help = "Consume a bounded batch; persist projection, then synchronously commit each offset."

    def add_arguments(self, parser):
        parser.add_argument("--topic", default=None)
        parser.add_argument("--group", default="lab.django.projections")
        parser.add_argument("--max-messages", type=int, default=100)
        parser.add_argument("--timeout", type=float, default=30)

    def handle(self, *args, **options):
        if options["max_messages"] < 1 or options["timeout"] <= 0:
            raise CommandError("max-messages and timeout must be positive")
        topic = validate_topic(options["topic"] or settings.KAFKA_ORDERS_TOPIC)
        kafka_consumer = consumer(options["group"])
        read, applied = 0, 0
        deadline = time.monotonic() + options["timeout"]
        try:
            kafka_consumer.subscribe([topic])
            while read < options["max_messages"] and time.monotonic() < deadline:
                message = kafka_consumer.poll(min(0.5, max(0, deadline - time.monotonic())))
                if message is None:
                    continue
                if message.error():
                    raise CommandError(str(message.error()))
                try:
                    event = json.loads(message.value())
                    changed = apply_event(event)
                except (ValueError, TypeError, UnicodeDecodeError) as exc:
                    raise CommandError(f"Poison event at {topic}[{message.partition()}]@{message.offset()}; offset NOT committed: {exc}") from exc
                # A crash here causes replay. The InboxEvent receipt makes replay safe.
                kafka_consumer.commit(message=message, asynchronous=False)
                read += 1
                applied += int(changed)
        finally:
            kafka_consumer.close()
        self.stdout.write(f"Read {read}; applied {applied}; duplicates {read - applied}.")
