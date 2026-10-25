import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST
from .models import OrderProjection
from .services import create_order_after_commit, create_order_with_outbox


@require_GET
def health(request):
    return JsonResponse({"status": "ok", "application": "local Kafka workshop"})


def _create(request, service):
    try:
        if request.content_type != "application/json":
            return JsonResponse({"error": "use Content-Type: application/json"}, status=415)
        body = json.loads(request.body)
        if not isinstance(body, dict):
            raise ValueError("body must be a JSON object")
        order, event = service(body.get("customer"), body.get("total"))
    except (ValueError, UnicodeDecodeError) as exc:
        return JsonResponse({"error": str(exc)}, status=400)
    except RuntimeError:
        return JsonResponse({"error": "Kafka delivery failed AFTER the order may have committed; inspect the database before retrying."}, status=503)
    return JsonResponse({"order_id": str(order.id), "event_id": event["event_id"], "total": str(order.total)}, status=201)


# These unauthenticated JSON endpoints are for a localhost workshop only.
# A deployed API needs authentication, authorization and appropriate CSRF handling.
@csrf_exempt
@require_POST
def create_order(request):
    return _create(request, create_order_with_outbox)


@csrf_exempt
@require_POST
def create_order_on_commit(request):
    return _create(request, create_order_after_commit)


@require_GET
def projection(request, order_id):
    try:
        row = OrderProjection.objects.get(order_id=order_id)
    except OrderProjection.DoesNotExist:
        return JsonResponse({"error": "projection not ready"}, status=404)
    return JsonResponse({"order_id": str(row.order_id), "customer": row.customer, "total": str(row.total), "source_event_id": str(row.source_event_id)})
