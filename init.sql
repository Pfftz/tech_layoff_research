-- Create schemas
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant privileges
GRANT ALL ON SCHEMA public TO admin;
GRANT ALL ON SCHEMA staging TO admin;
GRANT ALL ON SCHEMA silver TO admin;
GRANT ALL ON SCHEMA gold TO admin;

-- Alter role authentication
ALTER ROLE admin WITH PASSWORD 'admin123';
