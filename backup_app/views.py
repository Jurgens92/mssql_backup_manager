from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.core.paginator import Paginator
from .models import SQLServer, BackupJob, BackupSchedule
from .forms import SQLServerForm, TestConnectionForm
from .tasks import backup_server_databases, backup_database_task
from .backup_engine import MSSQLStreamBackup
import json

def dashboard(request):
    """Main dashboard view"""
    servers = SQLServer.objects.filter(is_active=True)
    recent_jobs = BackupJob.objects.all()[:10]
    
    # Statistics
    stats = {
        'total_servers': servers.count(),
        'running_jobs': BackupJob.objects.filter(status='running').count(),
        'failed_jobs_today': BackupJob.objects.filter(
            status='failed',
            started_at__date=timezone.now().date()
        ).count(),
        'successful_jobs_today': BackupJob.objects.filter(
            status='completed',
            completed_at__date=timezone.now().date()
        ).count(),
    }
    
    return render(request, 'backup_app/dashboard.html', {
        'servers': servers,
        'recent_jobs': recent_jobs,
        'stats': stats,
    })

def server_list(request):
    """List all servers"""
    servers = SQLServer.objects.all()
    return render(request, 'backup_app/server_list.html', {'servers': servers})

def server_create(request):
    """Create new server"""
    if request.method == 'POST':
        form = SQLServerForm(request.POST)
        if form.is_valid():
            server = form.save()
            messages.success(request, f'Server "{server.name}" created successfully!')
            return redirect('server_list')
    else:
        form = SQLServerForm()
    
    return render(request, 'backup_app/server_form.html', {
        'form': form,
        'title': 'Add New Server'
    })

def server_edit(request, pk):
    """Edit existing server"""
    server = get_object_or_404(SQLServer, pk=pk)
    
    if request.method == 'POST':
        form = SQLServerForm(request.POST, instance=server)
        if form.is_valid():
            server = form.save()
            messages.success(request, f'Server "{server.name}" updated successfully!')
            return redirect('server_list')
    else:
        form = SQLServerForm(instance=server)
    
    return render(request, 'backup_app/server_form.html', {
        'form': form,
        'title': f'Edit Server: {server.name}',
        'server': server
    })

@require_http_methods(["POST"])
def test_connection(request):
    """Test database connection via AJAX"""
    server_id = request.POST.get('server_id')
    server = get_object_or_404(SQLServer, pk=server_id)
    
    server_config = {
        'name': server.name,
        'server_address': server.server_address,
        'port': server.port,
        'username': server.username,
        'password': server.password,
    }
    
    backup_engine = MSSQLStreamBackup(server_config)
    success, message = backup_engine.test_connection()
    
    return JsonResponse({
        'success': success,
        'message': message
    })

@require_http_methods(["POST"])
def start_backup(request, server_id):
    """Start backup for all databases on a server"""
    server = get_object_or_404(SQLServer, pk=server_id)
    
    try:
        job_ids = backup_server_databases.delay(server_id)
        messages.success(
            request, 
            f'Backup started for server "{server.name}". '
            f'{len(server.get_databases())} databases queued.'
        )
    except Exception as e:
        messages.error(request, f'Failed to start backup: {str(e)}')
    
    return redirect('dashboard')

def job_list(request):
    """List all backup jobs with pagination"""
    jobs = BackupJob.objects.all()
    
    # Filtering
    status_filter = request.GET.get('status')
    if status_filter:
        jobs = jobs.filter(status=status_filter)
    
    server_filter = request.GET.get('server')
    if server_filter:
        jobs = jobs.filter(server_id=server_filter)
    
    paginator = Paginator(jobs, 25)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    return render(request, 'backup_app/job_list.html', {
        'page_obj': page_obj,
        'servers': SQLServer.objects.all(),
        'status_filter': status_filter,
        'server_filter': server_filter,
    })

def job_detail(request, pk):
    """View job details"""
    job = get_object_or_404(BackupJob, pk=pk)
    return render(request, 'backup_app/job_detail.html', {'job': job})

@require_http_methods(["POST"])
def cancel_job(request, job_id):
    """Cancel a running job"""
    job = get_object_or_404(BackupJob, pk=job_id)
    
    if job.status == 'running' and job.task_id:
        from celery import current_app
        current_app.control.revoke(job.task_id, terminate=True)
        job.status = 'failed'
        job.error_message = 'Cancelled by user'
        job.save()
        messages.success(request, 'Job cancelled successfully')
    else:
        messages.error(request, 'Job cannot be cancelled')
    
    return redirect('job_list')