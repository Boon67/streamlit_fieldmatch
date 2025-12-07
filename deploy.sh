#!/bin/bash

# Healthcare Field Mappings Manager - Snowflake Deployment Script
# This script deploys the Streamlit app and stored procedure to Snowflake using Snow CLI

set -e

# Configuration - Update these values for your environment
DATABASE="CLAIMSIQ"
SCHEMA="PROCESSOR"
WAREHOUSE="COMPUTE_WH"  # Update with your warehouse name

echo "╔════════════════════════════════════════════════════════════╗"
echo "║              ClaimsIQ - Snowflake Deployment               ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check if snow CLI is installed
if ! command -v snow &> /dev/null; then
    echo "❌ Error: Snowflake CLI (snow) is not installed."
    echo "   Install it with: pip install snowflake-cli"
    exit 1
fi

echo "✓ Snowflake CLI found"
echo ""

# Step 1: Create database and schema if they don't exist
echo "📦 Step 1: Setting up database and schema..."
snow sql -q "CREATE DATABASE IF NOT EXISTS ${DATABASE};" || true
snow sql -q "CREATE SCHEMA IF NOT EXISTS ${DATABASE}.${SCHEMA};" || true
echo "✓ Database and schema ready"
echo ""

# Step 2: Create the mappings table
echo "📋 Step 2: Creating mappings table..."
snow sql -q "
CREATE TABLE IF NOT EXISTS ${DATABASE}.${SCHEMA}.MAPPINGS_LIST (
    SRC VARCHAR,
    TARGET VARCHAR,
    DESCRIPTION VARCHAR
);
" || true
# Add DESCRIPTION column if it doesn't exist (for existing tables)
snow sql -q "
ALTER TABLE ${DATABASE}.${SCHEMA}.MAPPINGS_LIST ADD COLUMN IF NOT EXISTS DESCRIPTION VARCHAR;
" || true
echo "✓ Mappings table ready"
echo ""

# Step 3: Load initial data from CSV file using MERGE
echo "📊 Step 3: Loading field mappings data from mappings.csv..."

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV_FILE="${SCRIPT_DIR}/mappings.csv"

if [ ! -f "$CSV_FILE" ]; then
    echo "❌ Error: mappings.csv not found at ${CSV_FILE}"
    exit 1
fi

# Build VALUES clause from CSV file (skip header line)
VALUES_CLAUSE=""
while IFS=',' read -r src target description || [ -n "$src" ]; do
    # Skip header row
    if [ "$src" = "SRC" ]; then
        continue
    fi
    # Skip empty lines
    if [ -z "$src" ]; then
        continue
    fi
    # Escape single quotes in values
    src_escaped=$(echo "$src" | sed "s/'/''/g")
    target_escaped=$(echo "$target" | sed "s/'/''/g")
    description_escaped=$(echo "$description" | sed "s/'/''/g")
    
    if [ -n "$VALUES_CLAUSE" ]; then
        VALUES_CLAUSE="${VALUES_CLAUSE},"$'\n'
    fi
    VALUES_CLAUSE="${VALUES_CLAUSE}    ('${src_escaped}', '${target_escaped}', '${description_escaped}')"
done < "$CSV_FILE"

snow sql -q "
MERGE INTO ${DATABASE}.${SCHEMA}.MAPPINGS_LIST AS target
USING (
    SELECT * FROM VALUES
${VALUES_CLAUSE}
    AS source (src, target, description)
) AS source
ON target.src = source.src AND target.target = source.target
WHEN NOT MATCHED THEN
    INSERT (src, target, description) VALUES (source.src, source.target, source.description)
WHEN MATCHED THEN
    UPDATE SET description = source.description;
" || true
echo "✓ Field mappings loaded from mappings.csv"
echo ""

# Step 4: Deploy the stored procedure from mapping_proc.sql
echo "🔧 Step 4: Deploying field matcher stored procedure from mapping_proc.sql..."

SQL_FILE="${SCRIPT_DIR}/mapping_proc.sql"

if [ ! -f "$SQL_FILE" ]; then
    echo "❌ Error: mapping_proc.sql not found at ${SQL_FILE}"
    exit 1
fi

# Read the SQL file and replace the procedure name with fully qualified name
SQL_CONTENT=$(cat "$SQL_FILE")
# Replace "CREATE OR REPLACE PROCEDURE field_matcher_advanced" with fully qualified name
SQL_CONTENT=$(echo "$SQL_CONTENT" | sed "s/CREATE OR REPLACE PROCEDURE field_matcher_advanced/CREATE OR REPLACE PROCEDURE ${DATABASE}.${SCHEMA}.field_matcher_advanced/")

snow sql -q "$SQL_CONTENT" || true
echo "✓ Stored procedure deployed from mapping_proc.sql"
echo ""

# Step 5: Deploy the Streamlit app
echo "🚀 Step 5: Deploying Streamlit app..."
export SNOWFLAKE_WAREHOUSE="${WAREHOUSE}"
snow streamlit deploy \
    --database "${DATABASE}" \
    --schema "${SCHEMA}" \
    --replace
echo "✓ Streamlit app deployed"
echo ""

# Step 6: Get the app URL
echo "🔗 Step 6: Getting app URL..."
APP_URL=$(snow streamlit get-url CLAIMSIQ_APP --database "${DATABASE}" --schema "${SCHEMA}" 2>/dev/null || echo "")

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    Deployment Complete!                     ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "📍 Database: ${DATABASE}"
echo "📍 Schema:   ${SCHEMA}"
echo "📍 App:      CLAIMSIQ_APP"
echo ""
if [ -n "$APP_URL" ]; then
    echo "🌐 App URL: ${APP_URL}"
else
    echo "🌐 Open Snowsight and navigate to Streamlit to view the app"
fi
echo ""
echo "📝 Next Steps:"
echo "   1. Open the app in Snowsight"
echo "   2. Use 'Edit Mappings' tab to manage field mappings"
echo "   3. Use 'Test Matches' tab to test the matching algorithm"
echo "   4. Use 'Upload & Map File' tab to map and load data files"
echo ""
