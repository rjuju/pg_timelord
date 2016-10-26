-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2016: Julien Rouhaud

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_timelord" to load this file. \quit

SET client_encoding = 'UTF8';

CREATE FUNCTION pg_timelord_oldest_xact()
    RETURNS xid
    LANGUAGE c
    COST 10
    AS '$libdir/pg_timelord', 'pg_timelord_oldest_xact';

CREATE VIEW pg_timelord_oldest_xact
    AS SELECT current_setting('pg_timelord.database') AS datname,
        pg_timelord_oldest_xact() AS xmin,
        pg_xact_commit_timestamp(pg_timelord_oldest_xact()) AS xmin_ts;
