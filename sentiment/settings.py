"""
Django settings for the sentiment_realtime_project.
Generated from scratch – safe for local dev (DEBUG = True).
"""

from pathlib import Path
import os
from datetime import timedelta

BASE_DIR = Path(__file__).resolve().parent.parent

# 🔑 Dev-only key.  Replace with env var in prod!
SECRET_KEY = "django-insecure-change-me"

DEBUG = True
ALLOWED_HOSTS = []

# ───── Apps ──────────────────────────────────────────────────────────
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",

    "django.contrib.humanize",

    # local
    "dashboard",
    "channels",
    "corsheaders"
]

# ───── Middleware ─────────────────────────────────────────────────────
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "corsheaders.middleware.CorsMiddleware"
]
CORS_ALLOW_ALL_ORIGINS = True
ROOT_URLCONF = "sentiment.urls"

# ───── Templates ──────────────────────────────────────────────────────
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "dashboard" / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

TEMPLATES[0]["OPTIONS"]["context_processors"] += [
    "dashboard.context_processors.django_version",
]

WSGI_APPLICATION = "sentiment.wsgi.application"
ASGI_APPLICATION = "sentiment.asgi.application"

# ───── MongoDB (no default relational DB) ─────────────────────────────
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.dummy",
        "NAME": "mongodb-not-used-directly",
    }
}

# ───── Passwords / i18n / static ──────────────────────────────────────
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE     = "UTC"
USE_I18N      = True
USE_TZ        = True

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ───── WebSocket Channels Layer (Redis) ───────────────────────────────
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [("127.0.0.1", 6379)],
        },
    }
}

