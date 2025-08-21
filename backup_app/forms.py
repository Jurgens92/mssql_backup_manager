from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Submit, Row, Column, HTML, Div
from .models import SQLServer
import json

class SQLServerForm(forms.ModelForm):
    selected_databases = forms.MultipleChoiceField(
        choices=[],
        widget=forms.CheckboxSelectMultiple,
        required=False,
        label='Select Databases to Backup'
    )
    
    class Meta:
        model = SQLServer
        fields = ['name', 'server_address', 'port', 'username', 'password', 'is_active']
        widgets = {
            'password': forms.PasswordInput(),
        }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.layout = Layout(
            Row(
                Column('name', css_class='form-group col-md-6 mb-0'),
                Column('server_address', css_class='form-group col-md-6 mb-0'),
            ),
            Row(
                Column('port', css_class='form-group col-md-4 mb-0'),
                Column('username', css_class='form-group col-md-4 mb-0'),
                Column('password', css_class='form-group col-md-4 mb-0'),
            ),
            'is_active',
            HTML('<hr>'),
            HTML('<h5>Database Selection</h5>'),
            HTML('''
                <div class="mb-3">
                    <button type="button" id="fetch-databases-btn" class="btn btn-info">
                        <i class="fas fa-download me-1"></i> Fetch Databases from Server
                    </button>
                    <div id="fetch-status" class="mt-2"></div>
                </div>
            '''),
            Div(
                'selected_databases',
                css_id='databases-container',
                style='display: none;'
            ),
            Submit('submit', 'Save Server', css_class='btn btn-primary')
        )
        
        # Populate selected databases if editing
        if self.instance.pk:
            databases = self.instance.get_databases()
            self.fields['selected_databases'].choices = [(db, db) for db in databases]
            self.fields['selected_databases'].initial = databases
    
    def save(self, commit=True):
        instance = super().save(commit=False)
        
        # Save selected databases
        selected_databases = self.cleaned_data.get('selected_databases', [])
        instance.set_databases(selected_databases)
        
        if commit:
            instance.save()
        return instance

class TestConnectionForm(forms.Form):
    server_id = forms.ModelChoiceField(
        queryset=SQLServer.objects.filter(is_active=True),
        label='Select Server'
    )