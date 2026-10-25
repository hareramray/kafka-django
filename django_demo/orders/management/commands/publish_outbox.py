from django.core.management.base import BaseCommand, CommandError
from orders.services import publish_pending


class Command(BaseCommand):
    help = "Publish a finite snapshot of pending outbox rows (one publisher only for SQLite)."

    def add_arguments(self, parser):
        parser.add_argument("--max-messages", type=int, default=100)
        parser.add_argument("--timeout", type=float, default=30)
        parser.add_argument("--topic", default=None)

    def handle(self, *args, **options):
        try:
            count = publish_pending(limit=options["max_messages"], timeout=options["timeout"], topic=options["topic"])
        except (ValueError, RuntimeError) as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(f"Published {count} outbox event(s).")
