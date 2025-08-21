import pyodbc
import json
import gzip
import os
from datetime import datetime
from pathlib import Path
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

class MSSQLStreamBackup:
    def __init__(self, server_config):
        self.server_config = server_config
        self.backup_path = Path(settings.BACKUP_ROOT) / server_config['name']
        self.backup_path.mkdir(parents=True, exist_ok=True)
        
    def get_connection_string(self, database_name='master'):
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server_config['server_address']},{self.server_config['port']};"
            f"DATABASE={database_name};"
            f"UID={self.server_config['username']};"
            f"PWD={self.server_config['password']};"
            f"TrustServerCertificate=yes;"
        )
    
    def test_connection(self):
        """Test database connection"""
        try:
            with pyodbc.connect(self.get_connection_string(), timeout=10):
                return True, "Connection successful"
        except Exception as e:
            return False, str(e)
    
    def get_database_tables(self, database_name):
        """Get all tables from the specified database"""
        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        with pyodbc.connect(self.get_connection_string(database_name)) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in cursor.fetchall()]

    def stream_table_data(self, database_name, schema_name, table_name, output_file):
        """Stream table data to compressed JSON file"""
        full_table_name = f"[{schema_name}].[{table_name}]"
        
        with pyodbc.connect(self.get_connection_string(database_name)) as conn:
            cursor = conn.cursor()
            
            # Get column information
            cursor.execute(f"SELECT * FROM {full_table_name} WHERE 1=0")
            columns = [column[0] for column in cursor.description]
            
            # Stream data in chunks
            cursor.execute(f"SELECT * FROM {full_table_name}")
            
            with gzip.open(output_file, 'wt', encoding='utf-8') as f:
                row_count = 0
                chunk_size = 1000
                first_chunk = True
                
                # Write opening metadata
                f.write('{"database":"' + database_name + '","schema":"' + schema_name + 
                       '","table":"' + table_name + '","columns":' + json.dumps(columns) + 
                       ',"backup_timestamp":"' + datetime.now().isoformat() + '","data":[')
                
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    for row in rows:
                        if not first_chunk or row_count > 0:
                            f.write(',')
                        
                        row_dict = {}
                        for i, value in enumerate(row):
                            if isinstance(value, datetime):
                                row_dict[columns[i]] = value.isoformat()
                            elif value is None:
                                row_dict[columns[i]] = None
                            else:
                                row_dict[columns[i]] = str(value)
                        
                        f.write(json.dumps(row_dict))
                        row_count += 1
                    
                    first_chunk = False
                
                f.write(']}')
                logger.info(f"Backed up {row_count} rows from {full_table_name}")
                return row_count

    def backup_database(self, database_name, progress_callback=None):
        """Backup entire database by streaming all tables"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.backup_path / f"{database_name}_{timestamp}"
        backup_dir.mkdir(exist_ok=True)
        
        logger.info(f"Starting backup of database: {database_name}")
        
        try:
            tables = self.get_database_tables(database_name)
            total_size = 0
            
            for i, (schema_name, table_name) in enumerate(tables):
                output_file = backup_dir / f"{schema_name}_{table_name}.json.gz"
                logger.info(f"Backing up table: {schema_name}.{table_name}")
                
                if progress_callback:
                    progress_callback(f"Backing up {schema_name}.{table_name}")
                
                row_count = self.stream_table_data(database_name, schema_name, table_name, output_file)
                total_size += output_file.stat().st_size
            
            # Create backup manifest
            manifest = {
                'database': database_name,
                'backup_timestamp': timestamp,
                'tables_count': len(tables),
                'total_size': total_size,
                'tables': [{'schema': schema, 'table': table} for schema, table in tables]
            }
            
            with open(backup_dir / "backup_manifest.json", 'w') as f:
                json.dump(manifest, f, indent=2)
            
            logger.info(f"Backup completed successfully: {backup_dir}")
            return str(backup_dir), total_size
            
        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            raise