from django.contrib import admin
from .models import Tweet  # Make sure Tweet is imported

@admin.register(Tweet)
class TweetAdmin(admin.ModelAdmin):
    list_display = ("ids", "sentiment", "created_at", "short_text")
    search_fields = ("text",)
    list_filter = ("sentiment",)

    def short_text(self, obj):
        return (obj.text[:50] + "…") if len(obj.text) > 50 else obj.text
