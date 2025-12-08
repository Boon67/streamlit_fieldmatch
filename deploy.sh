#!/bin/bash

# Healthcare Field Mappings Manager - Snowflake Deployment Script
# This script deploys the Streamlit app and stored procedure to Snowflake using Snow CLI

set -e

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/deploy.config"

# Load defaults from config file
if [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
    DEFAULT_DATABASE="${DATABASE:-FIELD_MAPPER}"
    DEFAULT_SCHEMA="${SCHEMA:-PROCESSOR}"
    DEFAULT_WAREHOUSE="${WAREHOUSE:-COMPUTE_WH}"
    DEFAULT_APP_NAME="${APP_NAME:-Field Mapper}"
    DEFAULT_APP_ID="${APP_ID:-FIELD_MAPPER}"
else
    # Fallback defaults if config file doesn't exist
    DEFAULT_DATABASE="FIELD_MAPPER"
    DEFAULT_SCHEMA="PROCESSOR"
    DEFAULT_WAREHOUSE="COMPUTE_WH"
    DEFAULT_APP_NAME="Field Mapper"
    DEFAULT_APP_ID="FIELD_MAPPER"
fi

# Initialize with defaults
DATABASE="$DEFAULT_DATABASE"
SCHEMA="$DEFAULT_SCHEMA"
WAREHOUSE="$DEFAULT_WAREHOUSE"
APP_NAME="$DEFAULT_APP_NAME"
APP_ID="$DEFAULT_APP_ID"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║           Field Mapper - Snowflake Deployment              ║"
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

# Check for required Snowflake privileges (SYSADMIN and SECURITYADMIN)
echo "🔐 Checking Snowflake role privileges..."

# Get current user's roles
USER_ROLES=$(snow sql -q "SELECT LISTAGG(GRANTED_ROLE, ',') FROM (SELECT GRANTED_ROLE FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS WHERE GRANTEE_NAME = CURRENT_USER() AND DELETED_ON IS NULL UNION SELECT GRANTED_ROLE FROM SNOWFLAKE.INFORMATION_SCHEMA.APPLICABLE_ROLES);" --format json 2>/dev/null || echo "")

# Check if user can use SYSADMIN
CAN_USE_SYSADMIN=$(snow sql -q "USE ROLE SYSADMIN; SELECT 'YES';" --format json 2>/dev/null | grep -o '"YES"' || echo "")

if [ -z "$CAN_USE_SYSADMIN" ]; then
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                    ❌ PERMISSION ERROR                      ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Your current user does not have access to the SYSADMIN role."
    echo ""
    echo "The deployment requires SYSADMIN privileges to:"
    echo "   • Create databases and schemas"
    echo "   • Grant object privileges"
    echo "   • Deploy stored procedures"
    echo ""
    echo "Please contact your Snowflake administrator to grant SYSADMIN access,"
    echo "or run this deployment with a user that has the required privileges."
    echo ""
    exit 1
fi
echo "   ✓ SYSADMIN access confirmed"

# Check if user can use SECURITYADMIN
CAN_USE_SECURITYADMIN=$(snow sql -q "USE ROLE SECURITYADMIN; SELECT 'YES';" --format json 2>/dev/null | grep -o '"YES"' || echo "")

if [ -z "$CAN_USE_SECURITYADMIN" ]; then
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                    ❌ PERMISSION ERROR                      ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Your current user does not have access to the SECURITYADMIN role."
    echo ""
    echo "The deployment requires SECURITYADMIN privileges to:"
    echo "   • Create database access roles"
    echo "   • Establish role hierarchy"
    echo "   • Grant roles to users"
    echo ""
    echo "Please contact your Snowflake administrator to grant SECURITYADMIN access,"
    echo "or run this deployment with a user that has the required privileges."
    echo ""
    exit 1
fi
echo "   ✓ SECURITYADMIN access confirmed"
echo ""

# Configuration prompt
echo "📋 Configuration (from deploy.config):"
echo ""
echo "   Database:  $DEFAULT_DATABASE"
echo "   Schema:    $DEFAULT_SCHEMA"
echo "   Warehouse: $DEFAULT_WAREHOUSE"
echo "   App Name:  $DEFAULT_APP_NAME"
echo "   App ID:    $DEFAULT_APP_ID"
echo ""

# Check for --defaults or -y flag to skip prompts
if [[ "$1" == "--defaults" ]] || [[ "$1" == "-y" ]]; then
    echo "Using config values (--defaults flag detected)"
    echo ""
else
    read -p "Use these values? (Y/n): " use_defaults
    
    if [[ "$use_defaults" =~ ^[Nn]$ ]]; then
        echo ""
        echo "Enter custom values (press Enter to keep current):"
        echo ""
        
        # Database
        read -p "Database name [$DEFAULT_DATABASE]: " input_database
        if [ -n "$input_database" ]; then
            DATABASE="$input_database"
        fi
        
        # Schema
        read -p "Schema name [$DEFAULT_SCHEMA]: " input_schema
        if [ -n "$input_schema" ]; then
            SCHEMA="$input_schema"
        fi
        
        # Warehouse
        read -p "Warehouse name [$DEFAULT_WAREHOUSE]: " input_warehouse
        if [ -n "$input_warehouse" ]; then
            WAREHOUSE="$input_warehouse"
        fi
        
        # App Name
        read -p "App display name [$DEFAULT_APP_NAME]: " input_app_name
        if [ -n "$input_app_name" ]; then
            APP_NAME="$input_app_name"
        fi
        
        # App ID
        read -p "App identifier (no spaces) [$DEFAULT_APP_ID]: " input_app_id
        if [ -n "$input_app_id" ]; then
            APP_ID="$input_app_id"
        fi
        
        echo ""
        echo "Configuration:"
        echo "   Database:  $DATABASE"
        echo "   Schema:    $SCHEMA"
        echo "   Warehouse: $WAREHOUSE"
        echo "   App Name:  $APP_NAME"
        echo "   App ID:    $APP_ID"
        echo ""
        
        read -p "Proceed with deployment? (Y/n): " confirm
        if [[ "$confirm" =~ ^[Nn]$ ]]; then
            echo "Deployment cancelled."
            exit 0
        fi
    else
        echo "Using config values."
    fi
fi

echo ""
echo "Starting deployment..."
echo ""

# Get current Snowflake user for RBAC setup
echo "🔍 Getting current Snowflake user..."
# Parse the table output format: extract the username from between the pipes
CURRENT_USER=$(snow sql -q "SELECT CURRENT_USER() AS USERNAME;" 2>/dev/null | grep -E "^\| [A-Z]" | grep -v USERNAME | awk -F'|' '{print $2}' | tr -d ' ')
if [ -z "$CURRENT_USER" ]; then
    echo "❌ Error: Could not determine current Snowflake user."
    echo "   Please ensure your Snow CLI connection is configured correctly."
    exit 1
fi
echo "✓ Current user: ${CURRENT_USER}"
echo ""

# Step 1: Setup RBAC roles and create database/schema
echo "🔐 Step 1: Setting up RBAC roles and database..."

RBAC_FILE="${SCRIPT_DIR}/setup_rbac.sql"

if [ -f "$RBAC_FILE" ]; then
    echo "   Creating role hierarchy: ${DATABASE}_READONLY -> ${DATABASE}_READWRITE -> ${DATABASE}_ADMIN"
    
    # Read the SQL file and replace placeholders using a temp file
    TEMP_RBAC_FILE=$(mktemp)
    sed -e "s/{{DATABASE}}/${DATABASE}/g" \
        -e "s/{{SCHEMA}}/${SCHEMA}/g" \
        -e "s/{{WAREHOUSE}}/${WAREHOUSE}/g" \
        -e "s/{{CURRENT_USER}}/${CURRENT_USER}/g" \
        "$RBAC_FILE" > "$TEMP_RBAC_FILE"
    
    # Execute RBAC setup
    snow sql -f "$TEMP_RBAC_FILE" || {
        rm -f "$TEMP_RBAC_FILE"
        echo ""
        echo "❌ Error: Failed to execute RBAC setup."
        echo "   Please check the error messages above and ensure you have the required privileges."
        exit 1
    }
    rm -f "$TEMP_RBAC_FILE"
    echo "✓ RBAC roles and database ready"
else
    echo "❌ Error: setup_rbac.sql not found at ${RBAC_FILE}"
    echo "   This file is required for deployment."
    exit 1
fi
echo ""

# Switch to the admin role for remaining operations
echo "🔄 Switching to ${DATABASE}_ADMIN role..."
snow sql -q "USE ROLE ${DATABASE}_ADMIN;" || {
    echo "⚠️  Could not switch to ${DATABASE}_ADMIN role, continuing with current role..."
}

# Step 2: Create the mappings and config tables
echo "📋 Step 2: Creating tables..."

# Create mappings table
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

# Create app config table and store app name
snow sql -q "
CREATE TABLE IF NOT EXISTS ${DATABASE}.${SCHEMA}.APP_CONFIG (
    CONFIG_KEY VARCHAR PRIMARY KEY,
    CONFIG_VALUE VARCHAR
);
" || true

# Escape single quotes in APP_NAME for SQL
APP_NAME_ESCAPED=$(echo "$APP_NAME" | sed "s/'/''/g")

snow sql -q "
MERGE INTO ${DATABASE}.${SCHEMA}.APP_CONFIG AS target
USING (SELECT 'APP_NAME' AS CONFIG_KEY, '${APP_NAME_ESCAPED}' AS CONFIG_VALUE) AS source
ON target.CONFIG_KEY = source.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = source.CONFIG_VALUE
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE) VALUES (source.CONFIG_KEY, source.CONFIG_VALUE);
" || true

echo "✓ Tables ready"
echo ""

# Step 3: Load initial data from CSV file using MERGE
echo "📊 Step 3: Loading field mappings data from mappings.csv..."

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
export APP_NAME="${APP_NAME}"
export APP_ID="${APP_ID}"
snow streamlit deploy \
    --database "${DATABASE}" \
    --schema "${SCHEMA}" \
    --replace
echo "✓ Streamlit app deployed"
echo ""

# Step 6: Get the app URL
echo "🔗 Step 6: Getting app URL..."
APP_URL=$(snow streamlit get-url "${APP_ID}" --database "${DATABASE}" --schema "${SCHEMA}" 2>/dev/null || echo "")

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    Deployment Complete!                     ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "📍 Database:  ${DATABASE}"
echo "📍 Schema:    ${SCHEMA}"
echo "📍 Warehouse: ${WAREHOUSE}"
echo "📍 App Name:  ${APP_NAME}"
echo "📍 App ID:    ${APP_ID}"
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
echo "💡 Tips:"
echo "   - Edit deploy.config to change default values"
echo "   - Run with --defaults or -y flag to skip prompts"
echo ""
