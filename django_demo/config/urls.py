from django.urls import path
from orders import views

urlpatterns = [
    path("health/", views.health),
    path("orders/", views.create_order),
    path("orders/on-commit/", views.create_order_on_commit),
    path("projections/<uuid:order_id>/", views.projection),
]
