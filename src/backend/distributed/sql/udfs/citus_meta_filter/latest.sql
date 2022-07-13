CREATE OR REPLACE FUNCTION pg_catalog.is_citus_depended_object(oid, oid)
  RETURNS bool
LANGUAGE C
AS 'MODULE_PATHNAME', $$is_citus_depended_object$$;
COMMENT ON FUNCTION is_citus_depended_object(oid, oid)
    IS 'returns true if the given object for the meta table is a filtered citus object';
