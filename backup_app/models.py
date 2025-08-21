from django.db import models
from django.utils import timezone
import json

class SQLServer(models.Model):
    name = models.CharField(max_length=100, unique=True)
    server_address = models.CharField(max_length=255)
    port = models.IntegerField(default=1433)
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=255)  # In production, encrypt this
    databases = models.TextField(help_text="JSON list of databases to backup")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return self.name
    
    def get_databases(self):
        try:
            return json.loads(self.databases)
        except:
            return []
    
    def set_databases(self, db_list):
        self.databases = json.dumps(db_list)

class BackupJob(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    server = models.ForeignKey(SQLServer, on_delete=models.CASCADE)
    database_name = models.CharField(max_length=100)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    backup_path = models.CharField(max_length=500, blank=True)
    file_size = models.BigIntegerField(null=True, blank=True)
    task_id = models.CharField(max_length=255, blank=True)  # Celery task ID
    
    class Meta:
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.server.name} - {self.database_name} ({self.status})"

class BackupSchedule(models.Model):
    FREQUENCY_CHOICES = [
        ('hourly', 'Hourly'),
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
    ]
    
    server = models.ForeignKey(SQLServer, on_delete=models.CASCADE)
    frequency = models.CharField(max_length=20, choices=FREQUENCY_CHOICES)
    time_of_day = models.TimeField(help_text="Time to run backup (for daily/weekly/monthly)")
    is_active = models.BooleanField(default=True)
    last_run = models.DateTimeField(null=True, blank=True)
    
    def __str__(self):
        return f"{self.server.name} - {self.frequency}"