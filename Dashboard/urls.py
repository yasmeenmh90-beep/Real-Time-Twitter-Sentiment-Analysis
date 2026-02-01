# dashboard/urls.py

from django.urls import path
from . import views

app_name = "dashboard_frontend"

urlpatterns = [
    # Main dashboard
    path("", views.home, name="home"),

    # History view (MongoDB-based)
    path("history/", views.history, name="history"),
    path("api/history/", views.api_history, name="api_history"),

   # timeline shortcuts (open history pre-configured)

    path("timeline/", views.timeline, name="timeline"),

    # timeline APIs
    path("api/timeline/bursts/", views.api_tl_bursts, name="api_tl_bursts"),
    path("api/timeline/momentum/", views.api_tl_momentum, name="api_tl_momentum"),
    path("api/timeline/leaders/", views.api_tl_leaders, name="api_tl_leaders"),
    path("api/timeline/amplifiers/", views.api_tl_amplifiers, name="api_tl_amplifiers"),
    path("api/timeline/emojis/", views.api_tl_emojis, name="api_tl_emojis"),   # optional

    # JSON APIs
    path("api/metrics/", views.api_metrics, name="api_metrics"),
 path("api/latest/", views.api_latest, name="api_latest"),
    path("api/bars/", views.api_sentiment_bars, name="api_bars"),
    path("api/hourly-trend/", views.api_hourly_trend, name="api_hourly_trend"),
    path("api/language-pie/", views.api_language_pie, name="api_language_pie"),
    path("api/top-users/", views.api_top_users, name="api_top_users"),
    path("api/top-hashtags/",  views.api_top_hashtags,  name="api_top_hashtags"),
    path("api/hashtag-cloud/", views.api_hashtag_cloud, name="api_hashtag_cloud"),
    path("api/forecast/", views.api_forecast, name="api_forecast"),
    path("api/tweets-by-view/", views.api_tweets_by_view, name="api_tweets_by_view"),
    path("api/language-distribution", views.api_language_distribution, name="api_language_distribution"),
    path("redis-status/", views.redis_status, name="redis_status"),
    path("api/replay-buffered/", views.ReplayBufferedTweets.as_view(), name="replay_buffered"),
    path("api/kpis", views.kpis, name="kpis"),
    path("api/export/live.csv", views.export_live_csv, name="export_live_csv"),
]

