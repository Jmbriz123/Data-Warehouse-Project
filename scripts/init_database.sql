-- 1. Enable the dblink extension
-- This allows us to execute a command on the server from a separate 'connection'
CREATE EXTENSION IF NOT EXISTS dblink;

-- 2. The Idempotent Database Creation Block
DO $$
BEGIN
   -- Check the 'pg_database' system catalog for our project name
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dwh_project') THEN
      -- dblink_exec opens a side-door to run the CREATE command outside this block
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE dwh_project');
   END IF;
END
$$;

-- 3. Security: Dedicated Service Account
-- Using 'postgres' for everything is a security risk.
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dwh_admin') THEN
      CREATE ROLE dwh_admin WITH LOGIN PASSWORD 'your_secure_password';
   END IF;
END
$$;

-- 4. Permissions: The Handover
GRANT ALL PRIVILEGES ON DATABASE dwh_project TO dwh_admin;