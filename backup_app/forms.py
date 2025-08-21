from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Submit, Row, Column
from .models import SQLServer
import json

class SQLServerForm(forms.ModelForm):
    databases_text = forms.CharField(
        widget=forms.Textarea(attrs={'rows': 4}),
        label='Databases',
        help_text='Enter database names, one per line'
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
            'databases_text',
            'is_active',
            Submit('submit', 'Save Server', css_class='btn btn-primary')
        )
        
        # Populate databases field if editing
        if self.instance.pk:
            databases = self.instance.get_databases()
            self.fields['databases_text'].initial = '\n'.join(databases)
    
    def save(self, commit=True):
        instance = super().save(commit=False)
        
        # Parse databases
        databases_text = self.cleaned_data.get('databases_text', '')
        databases = [db.strip() for db in databases_text.split('\n') if db.strip()]
        instance.set_databases(databases)
        
        if commit:
            instance.save()
        return instance

class TestConnectionForm(forms.Form):
    server_id = forms.ModelChoiceField(
        queryset=SQLServer.objects.filter(is_active=True),
        label='Select Server'
    )