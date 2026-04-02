from sqlalchemy import create_engine, text
import os

# Use full path to psql
psql_path = r'C:\Program Files\PostgreSQL\18\bin\psql.exe'

try:
    # Setup SQL commands
    setup_sql = '''psql -U postgres -h localhost -c "
    DROP DATABASE IF EXISTS tech_layoffs_dw;
    DROP USER IF EXISTS admin;
    CREATE USER admin WITH PASSWORD 'admin123' CREATEDB;
    CREATE DATABASE tech_layoffs_dw OWNER admin;
    "'''
    
    # For Windows, try direct command
    cmd = f'{psql_path} -U postgres -h localhost -c "DROP DATABASE IF EXISTS tech_layoffs_dw; DROP USER IF EXISTS admin; CREATE USER admin WITH PASSWORD \'admin123\' CREATEDB; CREATE DATABASE tech_layoffs_dw OWNER admin;"'
    
    result = os.system(cmd)
    
    if result == 0:
        print('✓ Database setup complete')
    else:
        print(f'Command returned: {result}')
        
except Exception as e:
    print(f'Error: {e}')
