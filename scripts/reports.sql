

CREATE OR REPLACE VIEW VW_STATS_TABLES AS
SELECT table_name, row_estimate, pg_size_pretty(total_bytes) AS total
  FROM (
  SELECT *, total_bytes-index_bytes-coalesce(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS table_name
              , cast(c.reltuples as bigint) AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r' and nspname = 'public'
  ) a
) a;


/*
Number (estimate) of records by table
*/
CREATE OR REPLACE PROCEDURE calculate_num_records_by_table(table_name varchar)
LANGUAGE plpgsql
AS $$
DECLARE
  	num_records numeric;
	cu_records_by_table CURSOR (s_table_name varchar) FOR
		SELECT row_estimate FROM VW_STATS_TABLES WHERE VW_STATS_TABLES.table_name = s_table_name;
BEGIN
	OPEN cu_records_by_table(table_name);
	FETCH cu_records_by_table into num_records;
	INSERT INTO report (report_type, report_key, report_value) VALUES ('records_by_table', table_name, num_records);
	CLOSE cu_records_by_table;
END$$;


CREATE OR REPLACE PROCEDURE report()
LANGUAGE plpgsql
AS $$

DECLARE
  	cu_all_tables CURSOR FOR
		SELECT table_name FROM VW_STATS_TABLES WHERE table_name != 'report';
  	sb_table_name  varchar;

BEGIN
	truncate table report;
	OPEN cu_all_tables;
  	LOOP
    	FETCH FROM cu_all_tables INTO sb_table_name;
    	EXIT WHEN NOT FOUND;
		CALl calculate_num_records_by_table(sb_table_name);
  	END LOOP;
END;$$

