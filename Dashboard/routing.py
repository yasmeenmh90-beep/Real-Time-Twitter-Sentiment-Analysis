# dashboard/websockets/routing.py

from django.urls import re_path
from dashboard.websockets.consumers import SentimentConsumer

websocket_urlpatterns = [
    re_path(r"ws/sentiment/?$", SentimentConsumer.as_asgi()),
]
