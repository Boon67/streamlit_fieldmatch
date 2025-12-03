-- Field Match Prediction - Database Setup Script
-- Run this script in Snowflake before deploying the Streamlit app

-- Create database and schema (adjust names as needed)
CREATE DATABASE IF NOT EXISTS FIELD_MATCH_DB;
CREATE SCHEMA IF NOT EXISTS FIELD_MATCH_DB.FIELD_MATCH_SCHEMA;

USE DATABASE FIELD_MATCH_DB;
USE SCHEMA FIELD_MATCH_SCHEMA;

-- Create the mappings table
-- DROP TABLE IF EXISTS mappings_list; -- Uncomment if you want to start from scratch

CREATE TABLE IF NOT EXISTS mappings_list (
    src VARCHAR,
    target VARCHAR
);

-- Use MERGE to avoid duplicates
MERGE INTO mappings_list AS target
USING (
    VALUES
    ('Policy Effective Date', 'Policy Effective Date'),
    ('Group Name', 'Group Name'),
    ('Insured First Name', 'Insured First Name'),
    ('Insured Last Name', 'Insured Last Name'),
    ('Insured DOB', 'Insured DOB'),
    ('Claimant First Name', 'Claimant First Name'),
    ('Claimant Last Name', 'Claimant Last Name'),
    ('Claimant DOB', 'Claimant DOB'),
    ('Beginning Service Date', 'Beginning Service Date'),
    ('Claim Number/Claim Control Number', 'Claim Number/Claim Control Number'),
    ('Ending Service Date', 'Ending Service Date'),
    ('Processed Date', 'Processed Date'),
    ('Primary ICD', 'Primary ICD'),
    ('Secondary ICD', 'Secondary ICD'),
    ('CPT Code', 'CPT Code'),
    ('HCPCS Code', 'HCPCS Code'),
    ('Revenue Code', 'Revenue Code'),
    ('Modifier Code', 'Modifier Code'),
    ('Product Type', 'Product Type'),
    ('NPI?', 'NPI?'),
    ('Rx Name', 'Rx Name'),
    ('Rx Quantity', 'Rx Quantity'),
    ('Rx Days Supply', 'Rx Days Supply'),
    ('Rx Date Filled', 'Rx Date Filled'),
    ('Billed Amount', 'Billed Amount'),
    ('Copay Amount', 'Copay Amount'),
    ('Deductible Amount', 'Deductible Amount'),
    ('Coinsurance Amount', 'Coinsurance Amount'),
    ('Allowed Amount', 'Allowed Amount'),
    ('Net Amount', 'Net Amount'),
    ('Ineligible Amount', 'Ineligible Amount'),
    ('COB Amount', 'COB Amount'),
    ('Other Reduced Amount', 'Other Reduced Amount'),
    ('Denied Amount', 'Denied Amount'),
    ('Paid Amount', 'Paid Amount'),
    ('Service Line/Claim Type', 'Service Line/Claim Type'),
    ('Payee Name', 'Payee Name'),
    ('Payee Address', 'Payee Address'),
    ('Payee TIN', 'Payee TIN')
) AS source (src, target)
ON target.src = source.src AND target.target = source.target
WHEN NOT MATCHED THEN
    INSERT (src, target) VALUES (source.src, source.target);

-- Verify data loaded
SELECT * FROM mappings_list;

-- Create the classification model
-- Note: The model predicts TARGET field based on SRC field input
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION FIELD_MATCH_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'FIELD_MATCH_DB.FIELD_MATCH_SCHEMA.MAPPINGS_LIST'),
    TARGET_COLNAME => 'TARGET'
);

-- Verify model was created
SHOW SNOWFLAKE.ML.CLASSIFICATION;

-- Test a prediction
SELECT FIELD_MATCH_MODEL!PREDICT(
    INPUT_DATA => OBJECT_CONSTRUCT('SRC', 'Policy Date')
) AS PREDICTION;

-- View model metrics (optional)
CALL FIELD_MATCH_MODEL!SHOW_GLOBAL_EVALUATION_METRICS();
CALL FIELD_MATCH_MODEL!SHOW_FEATURE_IMPORTANCE();

