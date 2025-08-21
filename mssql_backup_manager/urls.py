from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('backup_app.urls')),  # Remove the redirect, just include backup_app URLs
]