from django.contrib import admin
from django.urls import path, include
from dashboard.api import router as api_router

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("dashboard.urls", namespace="dashboard_frontend")),  # ✅ fix here
    path("api/", include(api_router.urls)),
    path("dashboard/", include("dashboard.urls")),

]
