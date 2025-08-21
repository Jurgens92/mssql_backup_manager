from celery import shared_task
from django.utils import timezone
from .models import BackupJob, SQLServer
from .backup_engine import MSSQLStreamBackup
import logging

logger = logging.getLogger(__name__)

@shared_task
def backup_database_task(job_id):
    """Background task to backup a database"""
    try:
        job = BackupJob.objects.get(id=job_id)
        job.status = 'running'
        job.started_at = timezone.now()
        job.save()
        
        # Get server configuration
        server = job.server
        server_config = {
            'name': server.name,
            'server_address': server.server_address,
            'port': server.port,
            'username': server.username,
            'password': server.password,
        }
        
        # Initialize backup engine
        backup_engine = MSSQLStreamBackup(server_config)
        
        # Perform backup
        backup_path, file_size = backup_engine.backup_database(
            job.database_name,
            progress_callback=lambda msg: logger.info(f"Job {job_id}: {msg}")
        )
        
        # Update job status
        job.status = 'completed'
        job.completed_at = timezone.now()
        job.backup_path = backup_path
        job.file_size = file_size
        job.save()
        
        logger.info(f"Backup job {job_id} completed successfully")
        return f"Backup completed: {backup_path}"
        
    except Exception as e:
        logger.error(f"Backup job {job_id} failed: {str(e)}")
        job.status = 'failed'
        job.completed_at = timezone.now()
        job.error_message = str(e)
        job.save()
        raise

@shared_task
def backup_server_databases(server_id):
    """Backup all databases for a server"""
    server = SQLServer.objects.get(id=server_id)
    databases = server.get_databases()
    
    job_ids = []
    for db_name in databases:
        job = BackupJob.objects.create(
            server=server,
            database_name=db_name,
            status='pending'
        )
        task = backup_database_task.delay(job.id)
        job.task_id = task.id
        job.save()
        job_ids.append(job.id)
    
    return job_ids