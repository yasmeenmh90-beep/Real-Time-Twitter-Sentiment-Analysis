# dashboard/api.py
from rest_framework import serializers, viewsets, routers
from .models import Tweet

class TweetSer(serializers.ModelSerializer):
    class Meta:
        model  = Tweet
        fields = ("id", "text", "sentiment", "created_at")

class TweetView(viewsets.ReadOnlyModelViewSet):
    queryset         = Tweet.objects.all().order_by("-created_at")[:200]
    serializer_class = TweetSer

router = routers.DefaultRouter()
router.register(r"tweets", TweetView, basename="tweet")
