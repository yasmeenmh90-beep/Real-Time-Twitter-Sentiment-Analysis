from django.db import models

class Tweet(models.Model):
    """One scored tweet (unique by Twitter’s snowflake ID)."""

    objects = None
    SENTIMENT_CHOICES = [
        ("pos", "Positive"),
        ("neu", "Neutral"),
        ("neg", "Negative"),
    ]

    # Core tweet fields
    ids = models.CharField(max_length=255, unique=True, null=False, blank=False, db_index=True)
    user = models.CharField(max_length=255, null=True, blank=True)
    text = models.TextField(max_length=280)
    sentiment = models.CharField(max_length=20, choices=SENTIMENT_CHOICES, blank=True)
    created_at = models.DateTimeField()

    # Enriched features
    cleaned_text = models.TextField(null=True, blank=True)
    processed_text = models.TextField(null=True, blank=True)
    hashtags = models.JSONField(null=True, blank=True)

    vader_score = models.FloatField(null=True, blank=True)
    emoji_count = models.IntegerField(null=True, blank=True)
    capital_word_count = models.IntegerField(null=True, blank=True)
    negation_count = models.IntegerField(null=True, blank=True)
    sentiment_keyword_count = models.IntegerField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        preview = (self.text[:47] + "…") if len(self.text) > 50 else self.text
        return f"[{self.get_sentiment_display()}] {preview}"

    def get_sentiment_display(self):
        pass
