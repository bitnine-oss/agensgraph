/* src/pl/plpgsql/src/plagcypher--unpackaged--1.0.sql */

ALTER EXTENSION plagcypher ADD PROCEDURAL LANGUAGE plagcypher;
-- ALTER ADD LANGUAGE doesn't pick up the support functions, so we have to.
ALTER EXTENSION plagcypher ADD FUNCTION plpgsql_call_handler();
ALTER EXTENSION plagcypher ADD FUNCTION plpgsql_inline_handler(internal);
ALTER EXTENSION plagcypher ADD FUNCTION plpgsql_validator(oid);
