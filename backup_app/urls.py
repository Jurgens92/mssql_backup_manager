from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('servers/', views.server_list, name='server_list'),
    path('servers/create/', views.server_create, name='server_create'),
    path('servers/<int:pk>/edit/', views.server_edit, name='server_edit'),
    path('servers/<int:server_id>/backup/', views.start_backup, name='start_backup'),
    path('test-connection/', views.test_connection, name='test_connection'),
    path('fetch-databases/', views.fetch_databases, name='fetch_databases'), 
    path('jobs/', views.job_list, name='job_list'),
    path('jobs/<int:pk>/', views.job_detail, name='job_detail'),
    path('jobs/<int:job_id>/cancel/', views.cancel_job, name='cancel_job'),
]