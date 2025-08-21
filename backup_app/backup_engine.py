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

    def backup_database(self, database_name, progress_callback=None, include_schema=True):
        """Enhanced backup with better error handling and schema support"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.backup_path / f"{database_name}_{timestamp}"
        backup_dir.mkdir(exist_ok=True)
        
        try:
            # Backup schema information if requested
            if include_schema:
                self.backup_schema(database_name, backup_dir)
            
            tables = self.get_database_tables(database_name)
            total_size = 0
            
            for i, (schema_name, table_name) in enumerate(tables):
                try:
                    output_file = backup_dir / f"{schema_name}_{table_name}.json.gz"
                    if progress_callback:
                        progress_callback(f"Backing up {schema_name}.{table_name} ({i+1}/{len(tables)})")
                    
                    row_count = self.stream_table_data(database_name, schema_name, table_name, output_file)
                    total_size += output_file.stat().st_size
                    
                except Exception as e:
                    logger.warning(f"Failed to backup table {schema_name}.{table_name}: {str(e)}")
                    # Continue with other tables
                    continue
            
            # Create backup manifest with more details
            manifest = {
                'database': database_name,
                'backup_timestamp': timestamp,
                'backup_type': 'streaming_json',
                'tables_count': len(tables),
                'total_size': total_size,
                'compression': 'gzip',
                'tables': [{'schema': schema, 'table': table} for schema, table in tables]
            }
            
            with open(backup_dir / "backup_manifest.json", 'w') as f:
                json.dump(manifest, f, indent=2)
            
            return str(backup_dir), total_size
            
        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            raise

    def backup_schema(self, database_name, backup_dir):
        """Backup database schema information"""
        schema_info = {}
        
        with pyodbc.connect(self.get_connection_string(database_name)) as conn:
            cursor = conn.cursor()
            
            # Get table schemas
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, 
                    IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """)
            
            for row in cursor.fetchall():
                table_key = f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
                if table_key not in schema_info:
                    schema_info[table_key] = {'columns': []}
                
                schema_info[table_key]['columns'].append({
                    'name': row.COLUMN_NAME,
                    'type': row.DATA_TYPE,
                    'nullable': row.IS_NULLABLE == 'YES',
                    'default': row.COLUMN_DEFAULT,
                    'max_length': row.CHARACTER_MAXIMUM_LENGTH
                })
        
        with open(backup_dir / "schema.json", 'w') as f:
            json.dump(schema_info, f, indent=2)


def get_all_databases(self):
    """Get all user databases from the SQL Server (excluding system databases)"""
    try:
        with pyodbc.connect(self.get_connection_string(), timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name 
                FROM sys.databases 
                WHERE database_id > 4  -- Exclude system databases (master, tempdb, model, msdb)
                AND state = 0  -- Only online databases
                AND name NOT IN ('ReportServer', 'ReportServerTempDB')  -- Exclude common system databases
                ORDER BY name
            """)
            return [row.name for row in cursor.fetchall()]
    except Exception as e:
        raise Exception(f"Failed to retrieve databases: {str(e)}")