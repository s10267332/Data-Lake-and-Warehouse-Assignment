

-- -----------------------------------------------------------------------------
-- A) CONTEXT
-- -----------------------------------------------------------------------------
USE ROLE TRAINING_ROLE;

ALTER SESSION SET QUERY_TAG =
  'FERRET )- Assignment: Automating Transformations with Unstructured Data Using UDFs, Streams, and Tasks';

USE DATABASE FERRET_DB;
USE SCHEMA RAW;

ALTER WAREHOUSE FERRET_WH RESUME;
USE WAREHOUSE FERRET_WH;
ALTER WAREHOUSE FERRET_WH SET WAREHOUSE_SIZE = 'MEDIUM';

CREATE SCHEMA IF NOT EXISTS FERRET_DB.MODELED;
--Remove code to remove files in stage
--REMOVE @FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL PATTERN='.*\.pdf';

-- -----------------------------------------------------------------------------
-- B) STAGE + DIRECTORY + STREAM
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL
  COMMENT = 'FERRET Part B Q2 internal PDF stage (upload PDFs AFTER setup)';

-- Enable directory table to allow Snowflake to track file-level metadata
ALTER STAGE FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL SET DIRECTORY = (ENABLE = TRUE);

-- Refresh directory table to register newly uploaded PDFs
ALTER STAGE FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL REFRESH;

-- Stream on stage directory to capture new PDF INSERT events, consumed ONLY by T1 to avoid stream consumption conflicts
CREATE OR REPLACE STREAM FERRET_DB.RAW.FERRET_PDF_STAGE_STREAM_FINAL
  ON STAGE FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL;

-- -----------------------------------------------------------------------------
-- C) PYTHON UDF 
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION FERRET_DB.RAW.FERRET_GET_PDF_PARSED(file_url STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'PyPDF2')
HANDLER = 'run'
AS
$$
from PyPDF2 import PdfReader
from snowflake.snowpark.files import SnowflakeFile

def run(file_url: str):
    # Open PDF securely from Snowflake internal stage using scoped URL
    with SnowflakeFile.open(file_url, 'rb', require_scoped_url=True) as f:
        reader = PdfReader(f)
        page_count = len(reader.pages)

        texts = []
        for page in reader.pages:
            # Extract readable text from each page
            t = page.extract_text()
            if t:
                texts.append(t)
    # Combine all extracted page text
    full_text = "\n".join(texts).strip()

     # Split into lines to separate metadata-like header content
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]

    return {
        "meta": {"pages": page_count, "chars": len(full_text)},
        "metadata_lines": lines[:10],
        "body_text": "\n".join(lines[10:])[:150000]
    }
$$;

-- -----------------------------------------------------------------------------
-- D) TABLES + STREAM
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE FERRET_DB.RAW.FERRET_RAW_FILE_CATALOG (
  file_name       STRING,
  file_url        STRING,
  file_size       NUMBER,
  last_modified   TIMESTAMP_LTZ,
  insert_ts       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE FERRET_DB.RAW.FERRET_PROCESSED_FILE_CATALOG (
  file_name       STRING,
  file_url        STRING,
  file_size       NUMBER,
  last_modified   TIMESTAMP_LTZ,
  raw_insert_ts   TIMESTAMP_LTZ,
  insert_ts       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  report_id       STRING,
  pdf_metadata    VARIANT,   -- keep VARIANT (matches UDF output)
  pdf_body        STRING,
  page_count      NUMBER,
  char_count      NUMBER
);
CREATE OR REPLACE STREAM FERRET_DB.RAW.FERRET_RAW_FILE_CATALOG_STREAM
ON TABLE FERRET_DB.RAW.FERRET_RAW_FILE_CATALOG;

-- -----------------------------------------------------------------------------
-- E) TASKS DAG (T1 ROOT -> T2 -> T3)
-- -----------------------------------------------------------------------------

-- T1 ROOT: consume stream INSERTs and write to RAW_FILE_CATALOG

CREATE OR REPLACE TASK FERRET_DB.RAW.FERRET_T1_PDF_METADATA
  WAREHOUSE = FERRET_WH
  SCHEDULE  = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('FERRET_DB.RAW.FERRET_PDF_STAGE_STREAM_FINAL')
AS
INSERT INTO FERRET_DB.RAW.FERRET_RAW_FILE_CATALOG (file_name, file_url, file_size, last_modified)
SELECT
  s.RELATIVE_PATH AS file_name,
  BUILD_SCOPED_FILE_URL(@FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL, s.RELATIVE_PATH) AS file_url,
  s.SIZE AS file_size,
  s.LAST_MODIFIED::TIMESTAMP_LTZ AS last_modified
FROM FERRET_DB.RAW.FERRET_PDF_STAGE_STREAM_FINAL s
WHERE s.METADATA$ACTION = 'INSERT';

-- T2 CHILD: parse PDFs via UDF and upsert into PROCESSED_FILE_CATALOG
CREATE OR REPLACE TASK FERRET_DB.RAW.FERRET_T2_PARSE_PDF
  WAREHOUSE = FERRET_WH
  AFTER FERRET_DB.RAW.FERRET_T1_PDF_METADATA
  WHEN SYSTEM$STREAM_HAS_DATA('FERRET_DB.RAW.FERRET_RAW_FILE_CATALOG_STREAM')
AS
INSERT INTO FERRET_DB.RAW.FERRET_PROCESSED_FILE_CATALOG
(
  file_name, file_url, file_size, last_modified, raw_insert_ts,
  report_id, pdf_metadata, pdf_body, page_count, char_count
)
SELECT
  r.file_name,
  r.file_url,
  r.file_size,
  r.last_modified,
  r.insert_ts AS raw_insert_ts,

  -- report_id derived from PDF filename prefix to link unstructured PDFs
  -- with structured Part A curated data
  REGEXP_SUBSTR(r.file_name, '^[^_]+') AS report_id,

  parsed AS pdf_metadata,
  parsed:"body_text"::STRING AS pdf_body,
  parsed:"meta":"pages"::NUMBER AS page_count,
  parsed:"meta":"chars"::NUMBER AS char_count
FROM (
  SELECT
    s.file_name,
    s.file_url,
    s.file_size,
    s.last_modified,
    s.insert_ts,
    FERRET_DB.RAW.FERRET_GET_PDF_PARSED(s.file_url) AS parsed
  FROM FERRET_DB.RAW.FERRET_RAW_FILE_CATALOG_STREAM s
  WHERE s.METADATA$ACTION = 'INSERT'
) r;

-- T3 Getting final views ( Partitioned vs Non Partitioned)
CREATE OR REPLACE TASK FERRET_DB.RAW.FERRET_T3_FINAL_VIEW
  WAREHOUSE = FERRET_WH
  AFTER FERRET_DB.RAW.FERRET_T2_PARSE_PDF
AS
BEGIN

  -- ============================================================
  -- VIEW 1: PARTITIONED (Uses YEAR and QUARTER partition columns on the external table)
  -- ============================================================
  CREATE OR REPLACE VIEW FERRET_DB.MODELED.FERRET_VW_FINAL_PARTITIONED AS
  WITH partb_stats AS (
    SELECT
      YEAR::INT       AS year,
      QUARTER::STRING AS quarter,
      COUNT(*)        AS flights_cnt,
      AVG(ARR_DELAY)  AS avg_arr_delay
    FROM FERRET_DB.RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_PART
    WHERE YEAR = 2019
      AND QUARTER = 'Q1'
      AND ARR_DELAY IS NOT NULL
    GROUP BY 1,2
  )
  SELECT
    p.report_id,
    p.file_name,
    p.file_url,
    p.file_size,
    p.last_modified,
    p.page_count,
    p.char_count,
    p.pdf_metadata,
    p.pdf_body,

    a.* EXCLUDE (REPORT_ID),

    s.year        AS ontime_year,
    s.quarter     AS ontime_quarter,
    s.flights_cnt,
    s.avg_arr_delay
  FROM FERRET_DB.RAW.FERRET_PROCESSED_FILE_CATALOG p
  LEFT JOIN CHEETAH_DB.ASG_PART_A.CURATED_DAMAGE a
    ON a.REPORT_ID = p.report_id
  LEFT JOIN partb_stats s
    ON 1=1
  WHERE p.report_id IS NOT NULL;


  -- ============================================================
  -- VIEW 2: NON-PARTITIONED 
  -- Avoids YEAR/QUARTER predicates; filters using FL_DATE instead.
  -- ============================================================
  CREATE OR REPLACE VIEW FERRET_DB.MODELED.FERRET_VW_FINAL_NON_PARTITIONED AS
  -- External table is aggregated FIRST to avoid exploding joins
  -- This reduces millions of flight rows into a small summary result
  -- before joining with PDF-level data
  WITH partb_stats AS (
    SELECT
      YEAR::INT       AS year,
      QUARTER::STRING AS quarter,
      COUNT(*)        AS flights_cnt,
      AVG(ARR_DELAY)  AS avg_arr_delay
    FROM FERRET_DB.RAW.EXTERNAL_ONTIME_REPORTING_FERRET_ASG_PART
    WHERE FL_DATE >= '2019-01-01'::DATE
      AND FL_DATE <  '2019-04-01'::DATE
      AND ARR_DELAY IS NOT NULL
    GROUP BY 1,2
  )
  SELECT
    p.report_id,
    p.file_name,
    p.file_url,
    p.file_size,
    p.last_modified,
    p.page_count,
    p.char_count,
    p.pdf_metadata,
    p.pdf_body,

    a.* EXCLUDE (REPORT_ID),

    s.year        AS ontime_year,
    s.quarter     AS ontime_quarter,
    s.flights_cnt,
    s.avg_arr_delay
  FROM FERRET_DB.RAW.FERRET_PROCESSED_FILE_CATALOG p
  LEFT JOIN CHEETAH_DB.ASG_PART_A.CURATED_DAMAGE a
    ON a.REPORT_ID = p.report_id
  LEFT JOIN partb_stats s
    ON 1=1
  WHERE p.report_id IS NOT NULL;

END;


-- -----------------------------------------------------------------------------
-- F) UPLOAD PDFs then ALTER 
-- -----------------------------------------------------------------------------
-- STEP 1: Upload PDFs into:
--   @FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL
--
-- STEP 2: After uploading, run these in THIS ORDER:

-- (1) Register new files into directory table (creates stream INSERT rows)
ALTER STAGE FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL REFRESH;

-- (2) Resume ONLY the child tasks (practical-correct)
ALTER TASK FERRET_DB.RAW.FERRET_T2_PARSE_PDF  RESUME;
ALTER TASK FERRET_DB.RAW.FERRET_T3_FINAL_VIEW RESUME;

-- (3) Trigger root once for testing (it will chain to T2 and T3)
EXECUTE TASK FERRET_DB.RAW.FERRET_T1_PDF_METADATA;

-- -----------------------------------------------------------------------------
-- G)VALIDATION QUERIES
-- -----------------------------------------------------------------------------
-- Stage files
SELECT COUNT(*) FROM DIRECTORY(@FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL);

-- Stream rows waiting
SELECT METADATA$ACTION, RELATIVE_PATH FROM FERRET_DB.RAW.FERRET_PDF_STAGE_STREAM_FINAL;

-- Raw table
SELECT * FROM FERRET_DB.RAW.FERRET_RAW_FILE_CATALOG ORDER BY insert_ts DESC;

-- Processed table
SELECT * FROM FERRET_DB.RAW.FERRET_PROCESSED_FILE_CATALOG ORDER BY insert_ts DESC;

-- Stream data (proof new PDFs were detected)
SELECT
  METADATA$ACTION,
  COUNT(*) AS file_count
FROM FERRET_DB.RAW.FERRET_PDF_STAGE_STREAM_FINAL
GROUP BY METADATA$ACTION;



--Task history
SELECT
  NAME,
  STATE,
  ERROR_MESSAGE,
  SCHEDULED_TIME,
  COMPLETED_TIME
FROM TABLE(FERRET_DB.INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -6, CURRENT_TIMESTAMP()),
  RESULT_LIMIT => 50
))
WHERE NAME ILIKE '%FERRET_T%'
ORDER BY COMPLETED_TIME DESC;

--UDF Output
SELECT RELATIVE_PATH
FROM DIRECTORY(@FERRET_DB.RAW.FERRET_PDF_FOR_INT_STAGE_FINAL)
WHERE RELATIVE_PATH ILIKE '%.pdf'
LIMIT 1;

-- Partition view
SELECT COUNT(*)
FROM FERRET_DB.MODELED.FERRET_VW_FINAL_PARTITIONED;

-- Non partition view
SELECT COUNT(*)
FROM FERRET_DB.MODELED.FERRET_VW_FINAL_NON_PARTITIONED;


-- -----------------------------------------------------------------------------
-- H) POST CLEANUP (Suspend tasks and warehouse to prevent unnecessary credit usage)
-- -----------------------------------------------------------------------------
ALTER TASK FERRET_DB.RAW.FERRET_T3_FINAL_VIEW SUSPEND;
ALTER TASK FERRET_DB.RAW.FERRET_T2_PARSE_PDF  SUSPEND;
ALTER TASK FERRET_DB.RAW.FERRET_T1_PDF_METADATA SUSPEND;
ALTER SESSION UNSET QUERY_TAG;
ALTER WAREHOUSE FERRET_WH SET WAREHOUSE_SIZE='XSMALL';
ALTER WAREHOUSE FERRET_WH SUSPEND;



