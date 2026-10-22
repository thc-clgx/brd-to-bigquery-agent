/****************************************************************************************************************************************
* This is for the [INSERT NAME OF TABLE] flow.
* 
* Pre-requirements:
*		Tables: 01_[INSERT NAME OF TABLE], 02_[INSERT NAME OF TABLE], 02_[INSERT NAME OF TABLE]_stg
*		User Defined Functions(UDF): NA
*      
* Design: 	
*           Preparation: Initialization of Global Variables
*           Step 1: Staging Table - Transformation
*           Step 2: Data Quality Check
*           Step 3: Delete Records
*			Step 99: Insertion
*			
* Template Version 3.2.0
* Last Updated: 2026-08-18
*****************************************************************************************************************************************/

--  =====================================================================================================================================
--  ************ INITIALIZATION OF GLOBAL VARIABLES *************************************************************************************
--  1. Initializes global variables for the job such as JOB_ID, AMPD_SYSTEM_LOG, etc.
--  2. Sets up parameters specific to the environment and table being processed.
--  3. Utilizing @@query_label for GCP Cost Tracking.

DECLARE BQ_ERROR,
        STATUS,
        DELTA_DATE,
        LOADING_TYPE,
        SOURCE_FILENAME,
        CRITERIA_VALUE,
        CRITERIA_TYPE,
        AMPD_SOURCE_PROJECT_ID,
        AMPD_DATASET,
        AMPD_LOADING_LOG,
        AMPD_TABLE,
        QUERY_LABEL_PRODUCT_GROUP,
        QUERY_LABEL_OWNER,
        QUERY_LABEL_RESOURCE_NAME,
        QUERY_LABEL_MANAGED_BY,
        ML_SOURCE_PROJECT_ID,
        ML_PUBSUB_TOPIC,
        ML_EVENT_TYPE,
        ML_PIPELINE_RUN_UID
        STRING DEFAULT NULL;

DECLARE SOURCE_PROJECT_ID,
        SOURCE_DATASET,
        SOURCE_TABLE,
        SOURCE_TABLE_PATH
        /* ADD MORE SOURCE TABLE VARIABLES IF NEEDED */
        STRING DEFAULT NULL;

DECLARE RECORD_CNT,
        STAGING_TABLE_FLG,
        STEP_NUMBER,
        DELETED_RECORD_CNT,
        ADDED_RECORD_CNT
        INT64 DEFAULT NULL;

DECLARE ML_RUN_DATE DATE DEFAULT NULL;

DECLARE JOB_ID INT64 DEFAULT UNIX_SECONDS(CURRENT_TIMESTAMP());
DECLARE AMPD_SYSTEM_LOG STRING DEFAULT 'ampd_System_Logs';
DECLARE UNIQUE_COLUMN_NAMES, SOURCE_ENV_FILTER ARRAY<STRING>;
DECLARE FIRST_COLUMN_LENGTH STRING DEFAULT '150'; /*refer to confluence page section II step 1*/
DECLARE DELIMITER_CHECK_FLAG STRING DEFAULT 'N'; /*refer to confluence page section II step 1*/
DECLARE DUP_CHECK_FLAG STRING DEFAULT 'Y'; /*refer to confluence page section II step 1*/
DECLARE ENABLE_STEP3_DELETE_CRITERIA_PROCEDURE STRING DEFAULT 'N'; /*SET to 'Y', IF YOU WANT TO USE STEP 3 DELETE FOR CRITERIA PROCEDURE*/
-- ***************************************** FILL OUT ***********************************************************************************

-- ====================================================================================================
-- SECTION 1: DESTINATION TABLE & LOADING CONFIGURATION 
-- ====================================================================================================
/* FILL OUT DESTINATION TABLE INFO -> refer to confluence page section II step 2*/
SET AMPD_LOADING_LOG = '01_HPI_Loading_Log'; 
SET AMPD_DATASET = 'ampd_01_HPI';
SET AMPD_TABLE = '01_HPI_Output_Bulk_Export_by_STATE';

/*refer to confluence page section II step 3*/
SET QUERY_LABEL_PRODUCT_GROUP = 'hpi';
SET QUERY_LABEL_OWNER = 'memcgoldrick_at_corelogic_dot_com';
 
/* WHICH LOADING TYPE PROCEDURE? TRUNCATE_PROCEDURE, CRITERIA_PROCEDURE, UPDATE_PROCEDURE -> refer to confluence page section II step 4*/   
SET LOADING_TYPE = 'CRITERIA_PROCEDURE';

/* WHAT IS YOUR UNIQUE COLUMN NAME? -> refer to confluence page section II step 5*/
SET UNIQUE_COLUMN_NAMES = ['STATE_CODE', 'YYYYMM', 'TIER_CODE', 'RUN_DATE'];
    
/* WHAT IS YOUR PUBSUB TOPIC?*/
SET ML_PUBSUB_TOPIC = 'DTTPRTO_TO_AMPD_QC_EVENTS';

-- ====================================================================================================
-- DO NOT MODIFY SECTION: DSP METADATA (DYNAMIC)
-- ====================================================================================================

SET ML_EVENT_TYPE = (SELECT CASE
                            WHEN LOWER(AMPD_TABLE) LIKE r'%\_tp\_%' THEN 'TRAINING_PRERUN'
                            WHEN LOWER(AMPD_TABLE) LIKE r'%\_td\_%' THEN 'TRAINING_DATAPREP_COMPLETE'
                            WHEN LOWER(AMPD_TABLE) LIKE r'%\_tr\_%' THEN 'TRAINING_COMPLETE'
                            WHEN LOWER(AMPD_TABLE) LIKE r'%\_pp\_%' THEN 'PREDICTION_PRERUN'
                            WHEN LOWER(AMPD_TABLE) LIKE r'%\_pd\_%' THEN 'PREDICTION_DATAPREP_COMPLETE'
                            WHEN LOWER(AMPD_TABLE) LIKE r'%\_pr\_%' THEN 'PREDICTION_COMPLETE'
                            WHEN LOWER(AMPD_TABLE) LIKE r'%\_qc_results\_%' THEN 'QC_RESULTS'
                            ELSE 'NO EVENT TYPE FOUND'
                        END AS event_type);

SET ML_PIPELINE_RUN_UID = (SELECT JSON_VALUE(event_metadata, '$.pipeline_run_uid') AS scalar_name_run_date FROM ampd_System_Logs.ML_QC_pubsub_start_message_log
						   WHERE pubsub_topic = ML_PUBSUB_TOPIC AND event_type = ML_EVENT_TYPE
						   ORDER BY system_insert_date DESC	LIMIT 1);		

SET ML_RUN_DATE = SAFE_CAST((SELECT JSON_VALUE(event_metadata, '$.run_date') AS scalar_name_run_date FROM ampd_System_Logs.ML_QC_pubsub_start_message_log
						     WHERE pubsub_topic = ML_PUBSUB_TOPIC AND event_type = ML_EVENT_TYPE
						    ORDER BY system_insert_date DESC LIMIT 1)  AS DATE);

SET ML_SOURCE_PROJECT_ID = (SELECT event_source FROM ampd_System_Logs.ML_QC_pubsub_start_message_log
                         WHERE pubsub_topic = ML_PUBSUB_TOPIC AND event_type = ML_EVENT_TYPE
                         ORDER BY system_insert_date DESC LIMIT 1);

SET AMPD_SOURCE_PROJECT_ID = (SELECT @@project_id);

-- ====================================================================================================
-- SECTION 2: SOURCE TABLE CONFIGURATION
-- INSTRUCTIONS: Fill out your source dataset and table name below.
-- ADD MORE SOURCE TABLE INFO IF NEEDED. DON"T FORGET TO DECLARE THE VARIABLES. (e.g., SOURCE_DATASET_1, SOURCE_TABLE_1, SOURCE_TABLE_PATH_1)
-- ====================================================================================================

-- SECTION 2.1: SOURCE ENVIRONMENT FILTER =============================================================
-- INSTRUCTIONS: Specify the GCP environments that should be considered for sourcing data INTERNALLY within amo'd BECAUSE ACCESS TO EXTERNAL PROJECTS IS NOT AVAILABLE.
-- This is a safeguard to prevent accidental sourcing from production in a lower environment.
--
-- ***SET TO NULL IF THERE ARE NO ISSUES WITH EXTERNAL ACCESS.***
-- Example: SET SOURCE_ENV_FILTER = NULL;
--
-- If this query, when run in 'prf' or 'uat', faces issues with sourcing from the external environment, it should source from a 'prf' or 'uat' table, use the setting below.
-- Example: SET SOURCE_ENV_FILTER = ['prf', 'uat'];

SET SOURCE_ENV_FILTER = NULL; -- OPTIONS: ['sbx', 'dev', 'prf', 'npgreg', 'int', 'uat'] OR set to NULL if there are no issues with external access.

-- ====================================================================================================

-- SECTION 2.2: SOURCE ENVIRONMENT FILTER LOGIC =======================================================
-- INSTRUCTIONS: The logic below determines whether to source data from an external project or the current project based on the SOURCE_ENV_FILTER.


IF REGEXP_CONTAINS(LOWER(@@project_id), 'prd|reg')
   OR SOURCE_ENV_FILTER IS NULL
   OR NOT REGEXP_CONTAINS(LOWER(@@project_id), LOWER(ARRAY_TO_STRING(SOURCE_ENV_FILTER, '|'))) THEN

    -----------------------------------------------------------------------------------------------------------------------------------
    -- INSTRUCTION: Use this section to determine the source table path if no access issues exist with sourcing from external projects.
    -- ADD MORE SOURCE TABLE INFO IF NEEDED. DON"T FORGET TO DECLARE THE VARIABLES. (e.g., SOURCE_PROJECT_ID_1, SOURCE_DATASET_1, SOURCE_TABLE_1, SOURCE_TABLE_PATH_1)
    -----------------------------------------------------------------------------------------------------------------------------------

    -- --> OPTION 1: ML_SOURCE_PROJECT_ID (External Project Sourcing which retrieves the source project ID from Pub/Sub logs. Typically used for 01 Tables.)
    -- --> OPTION 2: AMPD_SOURCE_PROJECT_ID (Same Project Sourcing within the current amp'd gcp project. Typically used for 02 Tables.)
    SET SOURCE_PROJECT_ID = /* CHOOSE EITHER ML_SOURCE_PROJECT_ID OR AMPD_SOURCE_PROJECT_ID */ ; 

    SET SOURCE_DATASET = 'INSERT SOURCE DATASET NAME';
    SET SOURCE_TABLE = 'INSERT SOURCE TABLE NAME';
    SET SOURCE_TABLE_PATH = FORMAT('%s.%s.%s', SOURCE_PROJECT_ID, SOURCE_DATASET, SOURCE_TABLE); -- Path is automatically generated from variables above

    /* DEFINE MORE SOURCE TABLES IF NEEDED */

ELSE

    -----------------------------------------------------------------------------------------------------------------------------------
    -- INSTRUCTION: THIS IS AN ALTERNATIVE OPTION FOR SOURCING DATA WITHIN THE CURRENT amp'd GCP PROJECT, IF ACCESS TO EXTERNAL PROJECTS IS NOT AVAILABLE. 
    -- ADD MORE SOURCE TABLE INFO IF NEEDED. DON"T FORGET TO DECLARE THE VARIABLES. (e.g., SOURCE_PROJECT_ID_1, SOURCE_DATASET_1, SOURCE_TABLE_1, SOURCE_TABLE_PATH_1)
    -- *** NOTE: This section can be left blank if not applicable. ***
    -----------------------------------------------------------------------------------------------------------------------------------

    SET SOURCE_PROJECT_ID = (SELECT @@project_id);
    SET SOURCE_DATASET = 'INSERT SOURCE DATASET NAME';
    SET SOURCE_TABLE = 'INSERT SOURCE TABLE NAME';
    SET SOURCE_TABLE_PATH = FORMAT('%s.%s.%s', SOURCE_PROJECT_ID, SOURCE_DATASET, SOURCE_TABLE); -- Path is automatically generated from variables above

    /* DEFINE MORE SOURCE TABLES IF NEEDED */

END IF;

-- ====================================================================================================
-- SECTION 3: DELTA DATE SELECTION
-- PURPOSE: Determines the max delta date for incremental data processing based on the destination table's data or metadata.
-- INSTRUCTIONS: Choose ONLY ONE of the two options below and delete or comment out the other.
--               *Note: Cast DELTA_DATE to DATE in STEP 1 STAGING TABLE to match.*
-- ====================================================================================================

-- --> OPTION 1: SINGLE DATE (MAX DESTINATION TABLE DATE VIA FULL TABLE SCAN)
    -- BEHAVIOR: Retrieves the max date from the destination table's data.
    -- BATCH FREQUENCY options: DAY (Daily) | WEEK (Weekly) | MONTH (Monthly) | YEAR (Yearly)
    -- *Note: Choosing the appropriate batch frequency is crucial to avoid unnecessary full table scans and optimize performance.* 

EXECUTE IMMEDIATE FORMAT('''

    SELECT

        CAST(IFNULL(MAX(RUN_DATE), CAST("1900-01-01" AS DATE)) AS STRING) AS DELTA_DATE  -- Update RUN_DATE to match your tables delta date column.

    FROM `%s.%s`

    /* BATCH FREQ options: DAY (Daily) | WEEK (Weekly) | MONTH (Monthly) | YEAR (Yearly) */
    WHERE RUN_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 MONTH)                         -- Update RUN_DATE to match your tables delta date column.

    ''', AMPD_DATASET, AMPD_TABLE 

) INTO DELTA_DATE;


-- --> OPTION 2: SINGLE DATE (MAX DESTINATION TABLE DATE VIA METADATA)
    -- BEHAVIOR: Retrieves the latest partition ID from table metadata.
    -- PREREQUISITE: PARTITIONED TABLES ONLY. Will not work with non-partitioned tables.
    -- COST EFFICIENT: Queries metadata instead of scanning the table, saving costs.

EXECUTE IMMEDIATE FORMAT('''

    SELECT 
        CAST(PARSE_DATE("%%Y%%m%%d", MAX(partition_id)) AS STRING) AS DELTA_DATE
    FROM 
        `%s.INFORMATION_SCHEMA.PARTITIONS`
    WHERE 
        table_name = "%s"
        AND partition_id NOT IN ("__NULL__", "__UNPARTITIONED__")

    ''', AMPD_DATASET, AMPD_TABLE 

) INTO DELTA_DATE;

-- ***************************************** END ****************************************************************************************

-- **************************************** DO NOT EDIT *********************************************************************************
SET QUERY_LABEL_RESOURCE_NAME = LOWER(AMPD_TABLE);
SET QUERY_LABEL_MANAGED_BY = (SELECT CASE WHEN @@project_id IN ('clgx-surveillance-prd-f609', 'clgx-surveillance-reg-ec2c', 'clgx-surveillance-uat-bf45') THEN 'devops' ELSE 'developer' END);

-- ### Enables GCP cost tracking
SET @@query_label = 
  "productgroup:" || LOWER(IFNULL(QUERY_LABEL_PRODUCT_GROUP, 'unknown')) || 
  ", owner:" || LOWER(IFNULL(QUERY_LABEL_OWNER, 'unknown')) || 
  ", resource_name:" || LOWER(IFNULL(QUERY_LABEL_RESOURCE_NAME, 'unknown')) || 
  ", managed_by:" || LOWER(IFNULL(QUERY_LABEL_MANAGED_BY, 'unknown'));

-- ### Setting up staging flag condition
SET STAGING_TABLE_FLG = (SELECT COUNTIF(table_id = (SELECT AMPD_TABLE || '_stg')) FROM ampd_System_Logs.__TABLES__ );
-- **************************************** END *****************************************************************************************

--  =====================================================================================================================================
--  ************ STEP 1 STAGING TABLE - TRANSFORMATION PROCEDURE ************************************************************************
--  1. Verifies if staging table (_stg) already exists.
--  2. Creates staging table if it doesn't exist and Initializes the job and prepares for loading & transformation of data into the Staging table.
--  3. Executes SQL scripts based on loading type (truncate, criteria, or update).
--  4. Checks if the staging process completed successfully and updates the loading log accordingly.

BEGIN

      /* EDIT: OPTIONAL - Declare and set local variables here -> refer to confluence page section III step 1*/

      DECLARE SOURCE_FILENAME_COLUMN_EXISTENCE BOOL DEFAULT FALSE;
      SET STEP_NUMBER = 1;
      SET CRITERIA_TYPE = 'STAGING';

      IF ML_EVENT_TYPE = 'NO EVENT TYPE FOUND' THEN
        SET STATUS = 'EXECUTION FAILED';
        RAISE USING MESSAGE = 'BQ_ERROR: ML_EVENT_TYPE NOT IDENTIFIED, CHECK ML_EVENT_TYPE VALUE ASSIGNMENT';
      END IF;

      IF STAGING_TABLE_FLG IS NOT NULL AND STAGING_TABLE_FLG > 0 THEN

          EXECUTE IMMEDIATE ('DROP TABLE IF EXISTS `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_stg`');
          
      END IF;

      -- **************************************** EDIT *******************************************************************************************

      /* EDIT: OPTIONAL - INSERT TEMP TABLE -> refer to confluence page section III step 2*/

      /* Guidance with how to create and prepare staging table -> refer to confluence page section III step 3*/
      EXECUTE IMMEDIATE FORMAT('''

        CREATE OR REPLACE TABLE `%s.%s_stg`

        OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        )

        AS (

        SELECT
        CAST(@REF_ID_1 AS STRING) AS JOB_ID,
        DATE_TRUNC(CAST(@REF_ID_3 AS DATE), MONTH) AS RUN_DATE,     -- Normalizes the run date to the first of the month. This serves as a stable key for partitioning and enables clean re-runs by allowing an entire months data to be replaced.
        CAST(@REF_ID_3 AS DATE) AS run_identifier,                  -- The exact event date from the Pub/Sub message, kept for precise auditing and traceability.
        CAST(@REF_ID_4 AS STRING) AS pipeline_run_uid,

        --**!![INSERT COLUMN NAMES]!!**--,

        /*LOAD_DATE SUGGESTIONS - REFER TO CONFLUENCE PAGE SECTION II STEP 3*/
            SUGGESTIONS 1. SAFE_CAST(CURRENT_DATETIME() AS DATETIME) AS LOAD_DATE_--**!![01 OR 02]!!**--,
            SUGGESTIONS 2. SAFE_CAST(CURRENT_DATE() AS DATE) AS LOAD_DATE_--**!![01 OR 02]!!**--,

        FROM
            `%s`

        -- ====================================================================
        -- WHERE CLAUSE OPTIONS FOR CRITERIA OR UPDATE LOADING TYPE
        -- ====================================================================
        WHERE 
            -- ==========================================
            -- NOTE: CHANGE '>' TO '>=' IF USING STEP 3 UPDATE/CRITERIA PROCEDURE. 
            --       THIS ENSURES THE LATEST DELTA DATE IS CAPTURED IN THE STAGING TABLE UPON RERUNS, ALLOWING STEP 3 TO DELETE EXISTING DATA IN THE DESTINATION TABLE BEFORE LOADING REPROCESSED DATA.
            -- ==========================================
            SUGGESTIONS 1. RUN_DATE > CAST(@REF_ID_2 AS DATE)      -- Update RUN_DATE to match your tables delta date column.(COMPARE SINGLE DELTA DATE FROM TABLE)

      )''', AMPD_SYSTEM_LOG, AMPD_TABLE, SOURCE_TABLE_PATH

      ) USING JOB_ID AS REF_ID_1, DELTA_DATE AS REF_ID_2, ML_RUN_DATE AS REF_ID_3, ML_PIPELINE_RUN_UID AS REF_ID_4;

      /* EDIT: OPTIONAL - DROP TEMP TABLE -> refer to confluence page section III step 4*/

      -- **************************************** END ********************************************************************************************

      EXECUTE IMMEDIATE ('SELECT COUNT(1) AS cnt FROM `' || AMPD_SYSTEM_LOG || '.' ||AMPD_TABLE || '_stg`') INTO RECORD_CNT; -- Stores staging table record count

      SET CRITERIA_VALUE = CAST(RECORD_CNT || ' RECORDS INSERTED INTO STAGING TABLE' AS STRING); -- Stores record count into CRITERIA_VALUE to be used when inserting into loading log

      EXECUTE IMMEDIATE ('''SELECT COUNT(*) > 0 FROM `''' || AMPD_SYSTEM_LOG || '''.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = "'''|| AMPD_TABLE ||'''_stg" AND UPPER(column_name) = UPPER("SOURCE_FILENAME"); ''') INTO SOURCE_FILENAME_COLUMN_EXISTENCE;

      IF SOURCE_FILENAME_COLUMN_EXISTENCE = TRUE THEN 
        
        EXECUTE IMMEDIATE ('''SELECT ARRAY_TO_STRING(ARRAY(SELECT DISTINCT CAST(SOURCE_FILENAME AS STRING) AS SOURCE_FILENAME FROM `''' || AMPD_SYSTEM_LOG || '''.'''|| AMPD_TABLE || '''_stg` ORDER BY CAST(SOURCE_FILENAME AS STRING) DESC), ", \\n") AS SOURCE_FILENAME''') INTO SOURCE_FILENAME;
      ELSE 
        
        SET SOURCE_FILENAME = NULL;
      END IF;
        
      -- Begins conditional checks: If successful, the script proceeds to next step; otherwise, it fails and execution halts after metadata is inserted into the loading log
      IF RECORD_CNT > 0 THEN

        SET STATUS = 'EXECUTION COMPLETED';
        SET BQ_ERROR = 'NO ERRORS DETECTED';

      ELSEIF RECORD_CNT = 0 OR RECORD_CNT IS NULL THEN

        SET STATUS = 'QUERY HAS EXITED';
        SET BQ_ERROR = '0 RECORD COUNT, NO NEW RECORDS TO LOAD';
        EXECUTE IMMEDIATE ('DROP TABLE IF EXISTS `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_stg`');

      ELSE
        
        SET STATUS = 'EXECUTION FAILED';
        SET BQ_ERROR = 'UNKNOWN ERROR FROM STEP ' || CAST(STEP_NUMBER AS STRING);

      END IF;
      
      -- Inserts execution metadata into the loading log for record keeping
      IF STATUS = 'EXECUTION FAILED' THEN
          RAISE USING MESSAGE = BQ_ERROR;
      ELSE
          EXECUTE IMMEDIATE FORMAT("""
                                    INSERT INTO %s
                                        VALUES (
                                                CAST(@COLUMN1 AS INT64),
                                                CAST(@COLUMN2 AS STRING),
                                                CAST(@COLUMN3 AS TIMESTAMP),
                                                CAST(@COLUMN4 AS STRING),
                                                CAST(@COLUMN5 AS STRING),
                                                CAST(@COLUMN6 AS STRING),
                                                CAST(@COLUMN7 AS STRING),
                                                CAST(@COLUMN8 AS INT64),
                                                CAST(@COLUMN9 AS INT64),
                                                CAST(@COLUMN10 AS STRING),
                                                CAST(@COLUMN11 AS STRING),
                                                CAST(@COLUMN12 AS STRING)
                                                )
                                    """, AMPD_SYSTEM_LOG || '.' || AMPD_LOADING_LOG
                                ) USING     
                                        JOB_ID AS COLUMN1,
                                        AMPD_TABLE AS COLUMN2,
                                        CURRENT_TIMESTAMP() AS COLUMN3,
                                        LOADING_TYPE AS COLUMN4,
                                        CAST(DELTA_DATE AS STRING) AS COLUMN5,
                                        CRITERIA_TYPE AS COLUMN6,
                                        CRITERIA_VALUE AS COLUMN7,
                                        RECORD_CNT AS COLUMN8,
                                        STEP_NUMBER AS COLUMN9,
                                        STATUS AS COLUMN10,
                                        BQ_ERROR AS COLUMN11,
                                        SOURCE_FILENAME AS COLUMN12;
          IF STATUS = 'QUERY HAS EXITED' THEN
             RETURN;
          END IF;
      END IF;
  
      EXCEPTION WHEN ERROR THEN
          SET STATUS = 'EXECUTION FAILED';
          SET BQ_ERROR = (SELECT @@error.message);
          EXECUTE IMMEDIATE FORMAT("""
                                    INSERT INTO %s
                                        VALUES (
                                                CAST(@COLUMN1 AS INT64),
                                                CAST(@COLUMN2 AS STRING),
                                                CAST(@COLUMN3 AS TIMESTAMP),
                                                CAST(@COLUMN4 AS STRING),
                                                CAST(@COLUMN5 AS STRING),
                                                CAST(@COLUMN6 AS STRING),
                                                CAST(@COLUMN7 AS STRING),
                                                CAST(@COLUMN8 AS INT64),
                                                CAST(@COLUMN9 AS INT64),
                                                CAST(@COLUMN10 AS STRING),
                                                CAST(@COLUMN11 AS STRING),
                                                CAST(@COLUMN12 AS STRING)
                                                )
                                    """, AMPD_SYSTEM_LOG || '.' || AMPD_LOADING_LOG
                                ) USING     
                                        JOB_ID AS COLUMN1,
                                        AMPD_TABLE AS COLUMN2,
                                        CURRENT_TIMESTAMP() AS COLUMN3,
                                        LOADING_TYPE AS COLUMN4,
                                        CAST(DELTA_DATE AS STRING) AS COLUMN5,
                                        CRITERIA_TYPE AS COLUMN6,
                                        CRITERIA_VALUE AS COLUMN7,
                                        RECORD_CNT AS COLUMN8,
                                        STEP_NUMBER AS COLUMN9,
                                        STATUS AS COLUMN10,
                                        BQ_ERROR AS COLUMN11,
                                        SOURCE_FILENAME AS COLUMN12;
          RAISE USING MESSAGE = BQ_ERROR;

END;

--  =====================================================================================================================================
--  ************ STEP 2 DATA QUALITY CHECK **********************************************************************************************
--  1. Performs basic data quality checks such as checking for duplicates and delimiter issues in the data.
--  2. Creates a temporary table to identify duplicates.
--  3. Determines if there are any delimiter issues or duplicates found and updates the loading log accordingly.

BEGIN

    DECLARE D_TEST_VALUE, DUP_TEST_VALUE INT64 DEFAULT 0;
    DECLARE BASE_TEST_VALUE, FIRST_COLUMN_NAME STRING DEFAULT NULL;
    DECLARE D_TEST_FLAG, DUP_TEST_FLAG STRING DEFAULT 'N';
    SET STEP_NUMBER = STEP_NUMBER + 1;
    SET CRITERIA_TYPE = 'DATA QUALITY CHECK';

    IF UPPER(DELIMITER_CHECK_FLAG) = 'Y' OR UPPER(DUP_CHECK_FLAG) = 'Y' THEN

        IF RECORD_CNT > 0 AND STATUS = 'EXECUTION COMPLETED' THEN
            SET BQ_ERROR = 'NO ERRORS DETECTED';

            -- Checks for delimiter issue only if 'DELIMITER_CHECK_FLAG' is set to 'Y'
            IF UPPER(DELIMITER_CHECK_FLAG) = 'Y' THEN

                -- Retrieves the first column name from your table schema and stores it in the variable FIRST_COLUMN_NAME
                EXECUTE IMMEDIATE ('SELECT column_name ' ||
                                'FROM `' || AMPD_DATASET || '.INFORMATION_SCHEMA.COLUMNS` ' ||
                                'WHERE table_name = "'|| AMPD_TABLE ||'" AND ordinal_position = 1'
                                ) INTO FIRST_COLUMN_NAME;

                -- Retrieves the total count of rows where the character length exceeds the value of what was set in the 'FIRST_COLUMN_LENGTH' variable
                EXECUTE IMMEDIATE ('SELECT COUNT(1) AS cnt FROM `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_stg` WHERE CHAR_LENGTH(SAFE_CAST(' || FIRST_COLUMN_NAME || ' AS STRING)) > ' || ' SAFE_CAST(' || FIRST_COLUMN_LENGTH || ' AS INT64)') INTO D_TEST_VALUE;
            
            END IF;

            -- Checks for duplicates only if 'DUP_CHECK_FLAG' is set to 'Y'
            IF UPPER(DUP_CHECK_FLAG) = 'Y' THEN

                -- Clone empty staging structure with 7-day expiration
                EXECUTE IMMEDIATE ('CREATE OR REPLACE TABLE `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_dupchk_step2` ' ||
                                'LIKE `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_stg` ' ||
                                'OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))'
                                );

                -- Isolate & insert duplicate rows only. If the Dup_cnt column has a value larger than but not equal to 1, the row is considered a duplicate
                EXECUTE IMMEDIATE ('INSERT INTO `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_dupchk_step2` (' ||
                                'SELECT * FROM `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_stg` ' ||
                                'QUALIFY ROW_NUMBER() OVER (PARTITION BY ' || (SELECT STRING_AGG(UNIQUE_COLUMN_NAMES, ', ') FROM UNNEST(UNIQUE_COLUMN_NAMES) AS UNIQUE_COLUMN_NAMES) || ') > 1' ||
                                ')');

                -- Retrieves the total count of duplicate rows from the dupchk table
                SET DUP_TEST_VALUE = @@row_count;  -- @@row_count is set by BigQuery after any DML statement

            END IF;

            IF UPPER(DELIMITER_CHECK_FLAG) = 'Y' AND UPPER(DUP_CHECK_FLAG) = 'N' THEN

                -- Stores data quality count into CRITERIA_VALUE to be used when inserting into loading log
                SET CRITERIA_VALUE = CAST(D_TEST_VALUE AS STRING) || ' DELIMITER ISSUE DETECTED';    

            ELSEIF UPPER(DELIMITER_CHECK_FLAG) = 'N' AND UPPER(DUP_CHECK_FLAG) = 'Y' THEN

                -- Stores data quality count into CRITERIA_VALUE to be used when inserting into loading log
                SET CRITERIA_VALUE = CAST(DUP_TEST_VALUE AS STRING) || ' DUPLICATE IN DATA';

            ELSE

                -- Stores data quality count into CRITERIA_VALUE to be used when inserting into loading log
                SET CRITERIA_VALUE = CAST(DUP_TEST_VALUE AS STRING) || ' DUPLICATE IN DATA & ' || CAST(D_TEST_VALUE AS STRING) || ' DELIMITER ISSUE DETECTED';

            END IF;
        
            -- Begins conditional checks: If successful, the script proceeds to next step; otherwise, it fails and execution halts after metadata is inserted into the loading log
            IF D_TEST_VALUE = 0 THEN 
                Set D_TEST_FLAG = 'Y';
            ELSE 
                Set D_TEST_FLAG = 'N';
            END IF;

            IF DUP_TEST_VALUE = 0 THEN 
                Set DUP_TEST_FLAG = 'Y';
            ELSE 
                SET DUP_TEST_FLAG = 'N';
            END IF;

            IF D_TEST_FLAG = 'Y' AND DUP_TEST_FLAG = 'Y' THEN
                SET BASE_TEST_VALUE = 'NO DELIMITER ISSUE & DUPLICATE DETECTED';
                SET STATUS =  'EXECUTION COMPLETED';
            ELSEIF D_TEST_FLAG = 'N' AND DUP_TEST_FLAG = 'Y' THEN
                SET BASE_TEST_VALUE = 'DELIMITER ISSUE';
                SET STATUS = 'EXECUTION FAILED';
            ELSEIF D_TEST_FLAG = 'Y' AND DUP_TEST_FLAG = 'N' THEN
                SET BASE_TEST_VALUE = 'DUPLICATE ISSUE';
                SET STATUS = 'EXECUTION FAILED';
            ELSE
                SET BASE_TEST_VALUE = 'DELIMITER & DUPLICATE ISSUE';
                SET STATUS = 'EXECUTION FAILED';
            END IF;
            
            SET BQ_ERROR = BASE_TEST_VALUE;
        ELSE
            SET STATUS = 'EXECUTION FAILED';
            SET BQ_ERROR = 'UNKNOWN ERROR FROM STEP ' || CAST(STEP_NUMBER AS STRING);
        END IF;
        
        -- Inserts execution metadata into the loading log for record keeping
        IF STATUS = 'EXECUTION FAILED' THEN
            RAISE USING MESSAGE = BQ_ERROR;
        ELSE
            EXECUTE IMMEDIATE FORMAT("""
                                    INSERT INTO %s
                                        VALUES (
                                                CAST(@COLUMN1 AS INT64),
                                                CAST(@COLUMN2 AS STRING),
                                                CAST(@COLUMN3 AS TIMESTAMP),
                                                CAST(@COLUMN4 AS STRING),
                                                CAST(@COLUMN5 AS STRING),
                                                CAST(@COLUMN6 AS STRING),
                                                CAST(@COLUMN7 AS STRING),
                                                CAST(@COLUMN8 AS INT64),
                                                CAST(@COLUMN9 AS INT64),
                                                CAST(@COLUMN10 AS STRING),
                                                CAST(@COLUMN11 AS STRING),
                                                CAST(@COLUMN12 AS STRING)
                                                )
                                    """, AMPD_SYSTEM_LOG || '.' || AMPD_LOADING_LOG
                                ) USING     
                                        JOB_ID AS COLUMN1,
                                        AMPD_TABLE AS COLUMN2,
                                        CURRENT_TIMESTAMP() AS COLUMN3,
                                        LOADING_TYPE AS COLUMN4,
                                        CAST(DELTA_DATE AS STRING) AS COLUMN5,
                                        CRITERIA_TYPE AS COLUMN6,
                                        CRITERIA_VALUE AS COLUMN7,
                                        RECORD_CNT AS COLUMN8,
                                        STEP_NUMBER AS COLUMN9,
                                        STATUS AS COLUMN10,
                                        BQ_ERROR AS COLUMN11,
                                        SOURCE_FILENAME AS COLUMN12;
        END IF;

    END IF;

    EXCEPTION WHEN ERROR THEN
        SET STATUS = 'EXECUTION FAILED';
        SET BQ_ERROR = (SELECT @@error.message);
        EXECUTE IMMEDIATE FORMAT("""
                                INSERT INTO %s
                                    VALUES (
                                            CAST(@COLUMN1 AS INT64),
                                            CAST(@COLUMN2 AS STRING),
                                            CAST(@COLUMN3 AS TIMESTAMP),
                                            CAST(@COLUMN4 AS STRING),
                                            CAST(@COLUMN5 AS STRING),
                                            CAST(@COLUMN6 AS STRING),
                                            CAST(@COLUMN7 AS STRING),
                                            CAST(@COLUMN8 AS INT64),
                                            CAST(@COLUMN9 AS INT64),
                                            CAST(@COLUMN10 AS STRING),
                                            CAST(@COLUMN11 AS STRING),
                                            CAST(@COLUMN12 AS STRING)
                                            )
                                """, AMPD_SYSTEM_LOG || '.' || AMPD_LOADING_LOG
                            ) USING     
                                    JOB_ID AS COLUMN1,
                                    AMPD_TABLE AS COLUMN2,
                                    CURRENT_TIMESTAMP() AS COLUMN3,
                                    LOADING_TYPE AS COLUMN4,
                                    CAST(DELTA_DATE AS STRING) AS COLUMN5,
                                    CRITERIA_TYPE AS COLUMN6,
                                    CRITERIA_VALUE AS COLUMN7,
                                    RECORD_CNT AS COLUMN8,
                                    STEP_NUMBER AS COLUMN9,
                                    STATUS AS COLUMN10,
                                    BQ_ERROR AS COLUMN11,
                                    SOURCE_FILENAME AS COLUMN12;
        RAISE USING MESSAGE = BQ_ERROR;

END;

--  ==============================================================================================================================================
--  **************** STEP 3 UPDATE/CRITERIA PROCEDURE - DELETE RECORD COUNT **********************************************************************
--  1. If the loading type is an update/criteria procedure, it deletes records from the main table based on existing records in the staging table.
--  2. Calculates the number of deleted records.
--  3. Updates the loading log based on the success or failure of the deletion process.
--  4. If this step is being executes, please ensure your detination table has been partitioned by the delta date column to optimize the deletion process and reduce costs.

BEGIN

    DECLARE MAX_RUN_DATE_FROM_SOURCE DATE DEFAULT NULL;
    SET STEP_NUMBER = STEP_NUMBER + 1;
    SET CRITERIA_TYPE = 'DELETED RECORDS';
    
    IF LOADING_TYPE = 'UPDATE_PROCEDURE' OR (LOADING_TYPE = 'CRITERIA_PROCEDURE' AND ENABLE_STEP3_DELETE_CRITERIA_PROCEDURE = 'Y') THEN

        IF RECORD_CNT > 0 AND STATUS = 'EXECUTION COMPLETED' THEN
            SET BQ_ERROR = 'NO ERRORS DETECTED';

            -- **************************************** EDIT *******************************************************************************************

            -- /** INSERT THE NAME OF THE RUN DATE COLUMN FROM THE STAGING TABLE **/ PURPOSE: Retrieves the latest delta date to drive partition-pruned deletions.
            EXECUTE IMMEDIATE FORMAT('SELECT MAX(RUN_DATE) FROM `%s.%s_stg`', AMPD_SYSTEM_LOG, AMPD_TABLE) INTO MAX_RUN_DATE_FROM_SOURCE;

            -- Deletes obsolete records and natively captures the affected row count via @@row_count.
            -- This optimizes the pipeline by eliminating the need for a redundant pre-scan COUNT(*) query.
            EXECUTE IMMEDIATE FORMAT( '''
                
                MERGE `%s.%s` a
                USING (SELECT DISTINCT

                        -- ======================================================================
                        -- EDIT 1: JOIN KEYS (Required)
                        -- Purpose: Restricts staging to distinct keys, significantly reducing the
                        --          join size and enabling partition pruning on the main table.
                        -- Rec:     Use ALL unique business keys to ensure accurate deletion.
                        -- ======================================================================

                            -- example: RUN_DATE

                            [/*INSERT COLUMN NAME 1*/],
                            [/*INSERT COLUMN NAME 2*/]

                        -- ======================================================================

                        FROM `%s.%s_stg`

                        -- ======================================================================
                        -- EDIT 2: DELTA DATE FILTER (Optional)
                        -- Purpose: Filters source data by delta dates passed in @REF_ID_1 (comma-separated).
                        -- ======================================================================
                        WHERE 
                            -- example: RUN_DATE = "%t"

                            [/*INSERT COLUMN NAME*/] = "%t"

                        -- ======================================================================

                    ) stg
                ON 
                    -- ======================================================================
                    -- EDIT 3: MERGE JOIN CONDITIONS (Required)
                    -- Action: Join on the exact same keys defined in EDIT 1.
                    -- ======================================================================

                    -- example: a.RUN_DATE = stg.RUN_DATE

                    a.[/*INSERT COLUMN NAME 1*/] = stg.[/*INSERT COLUMN NAME 1*/]
                    AND a.[/*INSERT COLUMN NAME 2*/] = stg.[/*INSERT COLUMN NAME 2*/]

                    -- ======================================================================

                WHEN MATCHED THEN DELETE

            ''', AMPD_DATASET, AMPD_TABLE, AMPD_SYSTEM_LOG, AMPD_TABLE, MAX_RUN_DATE_FROM_SOURCE);

            -- **************************************** END ********************************************************************************************
            SET DELETED_RECORD_CNT = @@row_count;  -- @@row_count is set by BigQuery after any DML statement
            SET CRITERIA_VALUE = CAST(DELETED_RECORD_CNT || ' RECORDS DELETED FROM ' || AMPD_TABLE AS STRING);  -- Stores deleted record count into CRITERIA_VALUE to be used when inserting into loading log
            
            SET STATUS = 'EXECUTION COMPLETED';
        ELSE
            SET STATUS = 'EXECUTION FAILED';
            SET BQ_ERROR = 'UNKNOWN ERROR FROM STEP ' || CAST(STEP_NUMBER AS STRING);
        END IF;

        -- Inserts execution metadata into the loading log for record keeping
        IF STATUS = 'EXECUTION FAILED' THEN
            RAISE USING MESSAGE = BQ_ERROR;
        ELSE
          EXECUTE IMMEDIATE FORMAT("""
                                    INSERT INTO %s
                                        VALUES (
                                                CAST(@COLUMN1 AS INT64),
                                                CAST(@COLUMN2 AS STRING),
                                                CAST(@COLUMN3 AS TIMESTAMP),
                                                CAST(@COLUMN4 AS STRING),
                                                CAST(@COLUMN5 AS STRING),
                                                CAST(@COLUMN6 AS STRING),
                                                CAST(@COLUMN7 AS STRING),
                                                CAST(@COLUMN8 AS INT64),
                                                CAST(@COLUMN9 AS INT64),
                                                CAST(@COLUMN10 AS STRING),
                                                CAST(@COLUMN11 AS STRING),
                                                CAST(@COLUMN12 AS STRING)
                                                )
                                    """, AMPD_SYSTEM_LOG || '.' || AMPD_LOADING_LOG
                                ) USING     
                                        JOB_ID AS COLUMN1,
                                        AMPD_TABLE AS COLUMN2,
                                        CURRENT_TIMESTAMP() AS COLUMN3,
                                        LOADING_TYPE AS COLUMN4,
                                        CAST(DELTA_DATE AS STRING) AS COLUMN5,
                                        CRITERIA_TYPE AS COLUMN6,
                                        CRITERIA_VALUE AS COLUMN7,
                                        DELETED_RECORD_CNT AS COLUMN8,
                                        STEP_NUMBER AS COLUMN9,
                                        STATUS AS COLUMN10,
                                        BQ_ERROR AS COLUMN11,
                                        SOURCE_FILENAME AS COLUMN12;
        END IF;

    END IF;
        
    EXCEPTION WHEN ERROR THEN
        SET STATUS = 'EXECUTION FAILED';
        SET BQ_ERROR = (SELECT @@error.message);
        EXECUTE IMMEDIATE FORMAT("""
                                INSERT INTO %s
                                    VALUES (
                                            CAST(@COLUMN1 AS INT64),
                                            CAST(@COLUMN2 AS STRING),
                                            CAST(@COLUMN3 AS TIMESTAMP),
                                            CAST(@COLUMN4 AS STRING),
                                            CAST(@COLUMN5 AS STRING),
                                            CAST(@COLUMN6 AS STRING),
                                            CAST(@COLUMN7 AS STRING),
                                            CAST(@COLUMN8 AS INT64),
                                            CAST(@COLUMN9 AS INT64),
                                            CAST(@COLUMN10 AS STRING),
                                            CAST(@COLUMN11 AS STRING),
                                            CAST(@COLUMN12 AS STRING)
                                            )
                                """, AMPD_SYSTEM_LOG || '.' || AMPD_LOADING_LOG
                            ) USING     
                                    JOB_ID AS COLUMN1,
                                    AMPD_TABLE AS COLUMN2,
                                    CURRENT_TIMESTAMP() AS COLUMN3,
                                    LOADING_TYPE AS COLUMN4,
                                    CAST(DELTA_DATE AS STRING) AS COLUMN5,
                                    CRITERIA_TYPE AS COLUMN6,
                                    CRITERIA_VALUE AS COLUMN7,
                                    DELETED_RECORD_CNT AS COLUMN8,
                                    STEP_NUMBER AS COLUMN9,
                                    STATUS AS COLUMN10,
                                    BQ_ERROR AS COLUMN11,
                                    SOURCE_FILENAME AS COLUMN12;
        RAISE USING MESSAGE = BQ_ERROR;

END;

--  =====================================================================================================================================
--  **************** STEP 99 INSERTION ************************************************************************************************** 
--  1. Truncates the main table if the loading type is a truncate procedure.
--  2. Inserts records from the staging table into the main table.
--  3. Calculates the number of added records.
--  4. Updates the loading log based on the success or failure of the insertion process.

BEGIN

    DECLARE SCHEMA_COLUMN_NAMES STRING DEFAULT NULL;
    
    SET STEP_NUMBER = 99;
    SET CRITERIA_TYPE = 'INSERTED RECORDS';

    IF RECORD_CNT > 0 AND STATUS = 'EXECUTION COMPLETED' THEN
        SET BQ_ERROR = 'NO ERRORS DETECTED';

        -- Retrieves table schema and formats as SAFE_CAST(*COLUMN_NAME AS DATA_TYPE) AS COLUMN_NAME, ensuring only the schema-compatible columns are inserted into the destination table
        EXECUTE IMMEDIATE ('SELECT STRING_AGG(CONCAT("SAFE_CAST(", column_name, " AS ", data_type, ") AS ", column_name), ", ") AS COLUMN_NAMES ' ||
                        'FROM `' || AMPD_DATASET || '.INFORMATION_SCHEMA.COLUMNS` ' ||
                        'WHERE table_name = "'|| AMPD_TABLE ||'"'
                        ) INTO SCHEMA_COLUMN_NAMES;
        
        -- If loading type is TRUNCATE_PROCEDURE, the table will drop all rows but keep the table structure intact
        IF LOADING_TYPE = 'TRUNCATE_PROCEDURE' THEN
            EXECUTE IMMEDIATE ('TRUNCATE TABLE `' || AMPD_DATASET ||  '.' || AMPD_TABLE || '`');
        END IF;
        
        -- Inserts records from staging table into destination table using SCHEMA_COLUMN_NAMES variable (contains SAFE_CAST(COLUMN_NAME AS DATA_TYPE) AS COLUMN_NAME) and sets LOAD_DATE_* column with current time of insertion
        EXECUTE IMMEDIATE ('INSERT `' || AMPD_DATASET || '.' || AMPD_TABLE || '` (' ||
                                    'SELECT ' ||
                                    SCHEMA_COLUMN_NAMES ||
                                    ' FROM `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_stg`' ||
                                    ')'
                                );

        SET ADDED_RECORD_CNT =  @@row_count;
        SET CRITERIA_VALUE = SAFE_CAST(ADDED_RECORD_CNT || ' RECORDS INSERTED INTO ' || AMPD_TABLE AS STRING);  -- Stores added record count into CRITERIA_VALUE to be used when inserting into loading log

        -- Begins conditional checks: If successful, the script drops the staging & duplicate tables; Otherwise, it fails and execution halts after metadata is inserted into the loading log
        IF ADDED_RECORD_CNT > 0 THEN

            SET STATUS = 'EXECUTION COMPLETED';
            EXECUTE IMMEDIATE ('DROP TABLE IF EXISTS `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_stg`');
            EXECUTE IMMEDIATE ('DROP TABLE IF EXISTS `' || AMPD_SYSTEM_LOG || '.' || AMPD_TABLE || '_dupchk_step2`');

        ELSE

            SET STATUS = 'EXECUTION FAILED';
            SET BQ_ERROR = 'NO INSERTED RECORD COUNT INFO FROM SYSTEM VARIABLE row_count';
            SET CRITERIA_VALUE = 'INSERT COMPLETED. RECORD_CNT = '  || RECORD_CNT ||', FAILED TO RETRIEVE INSERTED RECORD COUNT; STAGING AND/OR DUPCHK TABLE CLEANUP NOT PERFORMED.';
       
        END IF;

    ELSE

        SET STATUS = 'EXECUTION FAILED';
        SET BQ_ERROR = 'UNKNOWN ERROR FROM STEP ' || CAST(STEP_NUMBER AS STRING);

    END IF;

    -- Inserts execution metadata into the loading log for record keeping
    IF STATUS = 'EXECUTION FAILED' THEN
        RAISE USING MESSAGE = BQ_ERROR;
    ELSE
        EXECUTE IMMEDIATE FORMAT("""
                                INSERT INTO %s
                                    VALUES (
                                            CAST(@COLUMN1 AS INT64),
                                            CAST(@COLUMN2 AS STRING),
                                            CAST(@COLUMN3 AS TIMESTAMP),
                                            CAST(@COLUMN4 AS STRING),
                                            CAST(@COLUMN5 AS STRING),
                                            CAST(@COLUMN6 AS STRING),
                                            CAST(@COLUMN7 AS STRING),
                                            CAST(@COLUMN8 AS INT64),
                                            CAST(@COLUMN9 AS INT64),
                                            CAST(@COLUMN10 AS STRING),
                                            CAST(@COLUMN11 AS STRING),
                                            CAST(@COLUMN12 AS STRING)
                                            )
                                """, AMPD_SYSTEM_LOG || '.' || AMPD_LOADING_LOG
                            ) USING     
                                    JOB_ID AS COLUMN1,
                                    AMPD_TABLE AS COLUMN2,
                                    CURRENT_TIMESTAMP() AS COLUMN3,
                                    LOADING_TYPE AS COLUMN4,
                                    CAST(DELTA_DATE AS STRING) AS COLUMN5,
                                    CRITERIA_TYPE AS COLUMN6,
                                    CRITERIA_VALUE AS COLUMN7,
                                    ADDED_RECORD_CNT AS COLUMN8,
                                    STEP_NUMBER AS COLUMN9,
                                    STATUS AS COLUMN10,
                                    BQ_ERROR AS COLUMN11,
                                    SOURCE_FILENAME AS COLUMN12;
    END IF;

    EXCEPTION WHEN ERROR THEN
        SET STATUS = 'EXECUTION FAILED';
        SET BQ_ERROR = (SELECT @@error.message);
        EXECUTE IMMEDIATE FORMAT("""
                                INSERT INTO %s
                                    VALUES (
                                            CAST(@COLUMN1 AS INT64),
                                            CAST(@COLUMN2 AS STRING),
                                            CAST(@COLUMN3 AS TIMESTAMP),
                                            CAST(@COLUMN4 AS STRING),
                                            CAST(@COLUMN5 AS STRING),
                                            CAST(@COLUMN6 AS STRING),
                                            CAST(@COLUMN7 AS STRING),
                                            CAST(@COLUMN8 AS INT64),
                                            CAST(@COLUMN9 AS INT64),
                                            CAST(@COLUMN10 AS STRING),
                                            CAST(@COLUMN11 AS STRING),
                                            CAST(@COLUMN12 AS STRING)
                                            )
                                """, AMPD_SYSTEM_LOG || '.' || AMPD_LOADING_LOG
                            ) USING     
                                    JOB_ID AS COLUMN1,
                                    AMPD_TABLE AS COLUMN2,
                                    CURRENT_TIMESTAMP() AS COLUMN3,
                                    LOADING_TYPE AS COLUMN4,
                                    CAST(DELTA_DATE AS STRING) AS COLUMN5,
                                    CRITERIA_TYPE AS COLUMN6,
                                    CRITERIA_VALUE AS COLUMN7,
                                    ADDED_RECORD_CNT AS COLUMN8,
                                    STEP_NUMBER AS COLUMN9,
                                    STATUS AS COLUMN10,
                                    BQ_ERROR AS COLUMN11,
                                    SOURCE_FILENAME AS COLUMN12;
        RAISE USING MESSAGE = BQ_ERROR;

END;