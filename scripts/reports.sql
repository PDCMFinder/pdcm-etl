DROP TABLE IF EXISTS REPORT;

CREATE TABLE REPORT (
    report_type TEXT,
    report_key TEXT,
    report_value TEXT
);

/*
A view with all the public tables in the schema and the estimate number of records (faster than doing a count,
and in most cases with the same result - only really big tables show a small difference)
*/
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
Inserts in the table report the number (estimate) of records by table
*/
CREATE OR REPLACE PROCEDURE calculate_num_records_by_table()
LANGUAGE plpgsql
AS $$
DECLARE
	cu_all_tables CURSOR FOR
		SELECT table_name, row_estimate FROM VW_STATS_TABLES WHERE table_name != 'report';
	sb_table_name  varchar;
  	num_records numeric;
BEGIN
	OPEN cu_all_tables;
  	LOOP
    	FETCH FROM cu_all_tables INTO sb_table_name, num_records;
    	EXIT WHEN NOT FOUND;
		INSERT INTO report (report_type, report_key, report_value)
		    VALUES ('records_by_table', sb_table_name, num_records);
  	END LOOP;

	CLOSE cu_all_tables;
END$$;


/*
Inserts in the table report the tables that have a foreign key that have a null value.
This could be a normal result, but could also be that a join was wrong so it is better to have
this in the report to spot potential issues
*/
CREATE OR REPLACE PROCEDURE number_of_null_fk()
LANGUAGE plpgsql
AS $$
DECLARE
  	rc_fk_row record;
  	rf_cursor refcursor;
  	null_records_count numeric;
  	description varchar;
	cu_all_fks CURSOR FOR
		SELECT
            tc.constraint_name,
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY';
BEGIN
	OPEN cu_all_fks;

	LOOP
    	FETCH FROM cu_all_fks INTO rc_fk_row;
    	EXIT WHEN NOT FOUND;
		OPEN rf_cursor FOR EXECUTE 'SELECT count(1) FROM ' || quote_ident(rc_fk_row.table_name)
                                        || ' WHERE ' || rc_fk_row.column_name || ' is null';
        FETCH rf_cursor INTO null_records_count;

		IF null_records_count > 0
		THEN
			description := rc_fk_row.table_name || '->' || rc_fk_row.foreign_table_name || ' (' || rc_fk_row.column_name || ')';
			INSERT INTO report (report_type, report_key, report_value)
				VALUES ('fk_null_values', description, null_records_count);
		END IF;

		CLOSE rf_cursor;
  	END LOOP;


END$$;


CREATE OR REPLACE PROCEDURE report()
LANGUAGE plpgsql
AS $$
BEGIN
	TRUNCATE table report;
  	CALl calculate_num_records_by_table();
  	CALL number_of_null_fk();
END;$$
