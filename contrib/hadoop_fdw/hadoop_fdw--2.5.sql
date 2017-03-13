/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for HADOOP
 *
 * IDENTIFICATION
 *                hadoop_fdw/hadoop_fdw--2.5.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION hadoop_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION hadoop_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER hadoop_fdw
  HANDLER hadoop_fdw_handler
  VALIDATOR hadoop_fdw_validator;
