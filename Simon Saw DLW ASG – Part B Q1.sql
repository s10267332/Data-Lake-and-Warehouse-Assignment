USE ROLE training_role;

ALTER SESSION SET QUERY_TAG = 'FERRET- Assignment: Working with External Table in Data Lake';

ALTER WAREHOUSE FERRET_WH RESUME;

USE WAREHOUSE FERRET_WH;
ALTER WAREHOUSE FERRET_WH SET WAREHOUSE_SIZE = 'XSMALL';

USE DATABASE FERRET_DB;
USE SCHEMA RAW;

SHOW WAREHOUSES LIKE 'FERRET_WH';

--------------------------------------------------------------------------------
-- Step 3/5: Create stored procedure (ASG_UNLOAD...) and GET_DDL for report
--------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE FERRET_DB.RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV(
  "FOLDER" VARCHAR, "YEAR" VARCHAR, "QUARTER" VARCHAR
)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
  var counter = 0;

  snowflake.execute({ sqlText:
      "copy into @raw.datalake_stage/" + FOLDER + "/" + YEAR + "Q" + QUARTER + "_OTP from (" +
      "   select * from raw.ONTIME_REPORTING WHERE YEAR = ''" + YEAR + "'' AND QUARTER = ''" + QUARTER + "''" +
      ") OVERWRITE=TRUE file_format = (type = ''CSV'' COMPRESSION = NONE FIELD_DELIMITER = '','' " +
      "   skip_header = 1 NULL_IF = () FIELD_OPTIONALLY_ENCLOSED_BY=''\\"'');"
  });

  return counter;
';

SHOW PROCEDURES LIKE 'ASG_UNLOAD_ONTIME_REPORTING_CSV%' IN SCHEMA FERRET_DB.RAW;

SELECT GET_DDL(
  'PROCEDURE',
  'FERRET_DB.RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV(VARCHAR, VARCHAR, VARCHAR)'
);

--------------------------------------------------------------------------------
-- Step 6: Calls (Q1 only) -> load into FERRET_ASG
--------------------------------------------------------------------------------

CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('FERRET_ASG', '2018', '1');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('FERRET_ASG', '2019', '1');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('FERRET_ASG', '2020', '1');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV('FERRET_ASG', '2021', '1');

-- verify folder + files exist
LIST @RAW.DATALAKE_STAGE/FERRET_ASG;

--------------------------------------------------------------------------------
-- File format (custom name for your assignment)
--------------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT RAW.ONTIME_REPORTING_CSV_FERRET
  TYPE = CSV
  SKIP_HEADER = 1
  COMPRESSION = AUTO
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

--------------------------------------------------------------------------------
-- Step 7: External tables (RAW only) -> point to FERRET_ASG
--------------------------------------------------------------------------------

-- external WITHOUT partition
CREATE OR REPLACE EXTERNAL TABLE RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_NOPART
  LOCATION = @RAW.DATALAKE_STAGE/FERRET_ASG
  FILE_FORMAT = (FORMAT_NAME = 'RAW.ONTIME_REPORTING_CSV_FERRET');

-- external WITH partition (safe casting to handle \N)
CREATE OR REPLACE EXTERNAL TABLE RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_PART (
    year INT AS (SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', -1), 1, 4)::INT),
    quarter STRING AS (CONCAT('Q', SUBSTR(SPLIT_PART(METADATA$FILENAME, '/', -1), 6, 1))),

    month INT AS (TRY_TO_NUMBER(value:c3::string)),
    day_of_month INT AS (TRY_TO_NUMBER(value:c4::string)),
    day_of_week INT AS (TRY_TO_NUMBER(value:c5::string)),
    fl_date DATE AS (TRY_TO_DATE(value:c6::string)),
    op_carrier STRING AS (value:c9::string),
    origin STRING AS (value:c15::string),
    dest STRING AS (value:c24::string),
    arr_delay INT AS (TRY_TO_NUMBER(NULLIF(value:c43::string, '\\N'))),
    cancelled INT AS (TRY_TO_NUMBER(NULLIF(value:c48::string, '\\N')))
)
PARTITION BY (year, quarter)
LOCATION = @RAW.DATALAKE_STAGE/FERRET_ASG
FILE_FORMAT = (FORMAT_NAME = 'RAW.ONTIME_REPORTING_CSV_FERRET');

ALTER EXTERNAL TABLE RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_NOPART REFRESH;
ALTER EXTERNAL TABLE RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_PART REFRESH;

--------------------------------------------------------------------------------
-- Step 8: Screenshots (No partition vs Partition)
--------------------------------------------------------------------------------

-- no partition
SELECT COUNT(*) AS row_count
FROM RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_NOPART
WHERE METADATA$FILENAME LIKE '%2019Q1_OTP%';

-- partition
SELECT COUNT(*) AS row_count
FROM RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_PART
WHERE year = 2019
  AND quarter = 'Q1';

--------------------------------------------------------------------------------
-- Step 9: View vs Materialized View (in MODELED)
--------------------------------------------------------------------------------

USE SCHEMA MODELED;

CREATE OR REPLACE VIEW MODELED.V_FERRET_ASG_FY2019_SEA AS
SELECT
  year, quarter, op_carrier, dest, origin, arr_delay
FROM RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_PART
WHERE origin = 'SEA'
  AND dest IN ('SFO','LAX','ORD','JFK','SLC','HNL','DEN','BOS','IAH','ATL')
  AND cancelled = 0
  AND year = 2019
  AND arr_delay IS NOT NULL;

SELECT
  dest,
  op_carrier,
  AVG(arr_delay) AS avg_arr_delay
FROM MODELED.V_FERRET_ASG_FY2019_SEA
GROUP BY 1,2
ORDER BY 1,3;

CREATE OR REPLACE MATERIALIZED VIEW MODELED.MV_FERRET_ASG_FY2019_SEA AS
SELECT
  dest,
  op_carrier,
  AVG(arr_delay) AS avg_arr_delay
FROM RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_PART
WHERE origin = 'SEA'
  AND dest IN ('SFO','LAX','ORD','JFK','SLC','HNL','DEN','BOS','IAH','ATL')
  AND cancelled = 0
  AND year = 2019
  AND arr_delay IS NOT NULL
GROUP BY 1,2;

SELECT *
FROM MODELED.MV_FERRET_ASG_FY2019_SEA
ORDER BY 1;

--------------------------------------------------------------------------------
-- Cleanup
--------------------------------------------------------------------------------


ALTER SESSION UNSET QUERY_TAG;
ALTER WAREHOUSE FERRET_WH SUSPEND;
