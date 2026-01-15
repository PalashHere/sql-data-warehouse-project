/*
=============================================================
ETL Script: Bronze Layer Load Procedure
=============================================================
Purpose:
    This script creates and executes the `bronze.load_bronze()` stored procedure
    to refresh the Bronze layer by truncating and reloading raw CRM and ERP
    tables from CSV extracts. It acts as the repeatable ingestion step in the
    Medallion Architecture (Bronze → Silver → Gold).

What the Procedure Does:
    - Captures start/end timestamps and logs progress via RAISE NOTICE
    - TRUNCATEs each Bronze table to perform a full refresh
    - COPY loads data from CSV files located in `/tmp/`
    - Raises a single exception with SQLERRM/SQLSTATE if any step fails
    - (After execution) returns row-count checks for each loaded table

Tables Loaded:
    - bronze.crm_cust_info
    - bronze.crm_prd_info
    - bronze.crm_sales_details
    - bronze.erp_cust_az12
    - bronze.erp_loc_a101
    - bronze.erp_px_cat_g1v2

Notes:
    - This is a full reload (not incremental); existing data is removed each run.
    - Assumes the database server can access the referenced `/tmp/*.csv` paths
      and has permissions to execute COPY FROM file.
=============================================================
*/


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    
    RAISE NOTICE 'Starting Bronze Layer Load...';

    -- CRM Tables
    TRUNCATE bronze.crm_cust_info;
    COPY bronze.crm_cust_info FROM '/tmp/cust_info.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '"');
    RAISE NOTICE '✓ Loaded crm_cust_info';

    TRUNCATE bronze.crm_prd_info;
    COPY bronze.crm_prd_info FROM '/tmp/prd_info.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '"');
    RAISE NOTICE '✓ Loaded crm_prd_info';

    TRUNCATE bronze.crm_sales_details;
    COPY bronze.crm_sales_details FROM '/tmp/sales_details.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '"');
    RAISE NOTICE '✓ Loaded crm_sales_details';

    -- ERP Tables
    TRUNCATE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 FROM '/tmp/CUST_AZ12.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '"');
    RAISE NOTICE '✓ Loaded erp_cust_az12';

    TRUNCATE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101 FROM '/tmp/LOC_A101.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '"');
    RAISE NOTICE '✓ Loaded erp_loc_a101';

    TRUNCATE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2 FROM '/tmp/PX_CAT_G1V2.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '"');
    RAISE NOTICE '✓ Loaded erp_px_cat_g1v2';

    v_end_time := clock_timestamp();
    RAISE NOTICE 'Load Complete! Duration: %', v_end_time - v_start_time;

-- Single global exception handler
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in bronze.load_bronze: % | SQLSTATE: %', SQLERRM, SQLSTATE;
END;
$$;

call bronze.load_bronze();

-- Check row counts
SELECT 'crm_cust_info' AS table_name, COUNT(*) FROM bronze.crm_cust_info
UNION ALL
SELECT 'crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
UNION ALL
SELECT 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
UNION ALL
SELECT 'erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
UNION ALL
SELECT 'erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
UNION ALL
SELECT 'erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;
