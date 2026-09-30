\set ON_ERROR_STOP on
\set QUIET on
\if :{?mode}
\else
  SELECT 'invalid restore role invocation'::integer;
\endif
SELECT :'mode' IN ('cluster', 'database', 'application') AS valid_mode,
       :'mode' = 'cluster' AS cluster_mode, :'mode' = 'database' AS database_mode,
       :'mode' = 'application' AS application_mode,
       :'writer' = 'r640_writer' AND :'reader' = 'r640_reader' AS valid_roles
\gset
\if :valid_mode
\else
  SELECT 'invalid restore role invocation'::integer;
\endif
\if :valid_roles
\else
  SELECT 'invalid restore role invocation'::integer;
\endif
\if :cluster_mode
  -- Roles are cluster prerequisites, not claimed as restored database objects.
  SELECT format('CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
                :'writer', :'writer_password')
  \gexec
  SELECT format('CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
                :'reader', :'reader_password')
  \gexec
\endif
\if :database_mode
  SELECT current_database() = :'database' AS correct_database
  \gset
  \if :correct_database
  \else
    SELECT 'invalid restore role invocation'::integer;
  \endif
  SELECT format('REVOKE CREATE ON DATABASE %I FROM PUBLIC', :'database'),
         format('GRANT CONNECT ON DATABASE %I TO %I, %I', :'database', :'writer', :'reader'),
         format('GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO %I, %I', :'writer', :'reader')
  \gexec
  REVOKE CREATE ON SCHEMA public FROM PUBLIC;
\endif
\if :application_mode
  -- Run only on original H0 after MIGRATOR and sealed init-schema.
  SELECT current_database() = :'database' AND :'database' = 'r640_original' AS correct_original
  \gset
  \if :correct_original
  \else
    SELECT 'invalid restore role invocation'::integer;
  \endif
  SELECT format('GRANT USAGE ON SCHEMA %I TO %I', nspname, :'reader'),
         format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO %I', nspname, :'reader'),
         format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO %I',
                :'writer', nspname, :'reader')
  FROM pg_namespace WHERE nspname IN ('public', 'bigname_phase') ORDER BY nspname
  \gexec
\endif
SELECT jsonb_build_object('mode', :'mode', 'database', current_database(),
                          'writer', :'writer', 'reader', :'reader', 'completed', true);
