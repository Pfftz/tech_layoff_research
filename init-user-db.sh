#!/bin/bash
# Initialize admin user with md5 password
psql -v ON_ERROR_STOP=1 --username "postgres" <<-EOSQL
    CREATE USER admin WITH ENCRYPTED PASSWORD 'admin123';
    GRANT ALL PRIVILEGES ON DATABASE tech_layoffs_dw TO admin;
    GRANT USAGE ON SCHEMA public TO admin;
    GRANT CREATE ON SCHEMA public TO admin;
EOSQL
