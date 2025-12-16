@echo off
setlocal enabledelayedexpansion

REM Field Mapper - Snowflake Deployment Script for Windows
REM This script deploys the Streamlit app and stored procedure to Snowflake using Snow CLI

REM Get the directory where the script is located
set "SCRIPT_DIR=%~dp0"
set "CONFIG_FILE=%SCRIPT_DIR%deploy.config"

REM Default values
set "DEFAULT_DATABASE=FIELD_MAPPER"
set "DEFAULT_SCHEMA=PROCESSOR"
set "DEFAULT_WAREHOUSE=COMPUTE_WH"
set "DEFAULT_APP_NAME=Field Mapper"
set "DEFAULT_APP_ID=FIELD_MAPPER"
set "DEFAULT_CREATE_ROLES=true"
set "DEFAULT_LLM_MODEL=claude-4-sonnet"

REM Load config file if it exists
if exist "%CONFIG_FILE%" (
    for /f "usebackq tokens=1,* delims==" %%a in ("%CONFIG_FILE%") do (
        set "line=%%a"
        REM Skip comments and empty lines
        if not "!line:~0,1!"=="#" (
            if not "!line!"=="" (
                REM Remove quotes from value
                set "value=%%b"
                set "value=!value:"=!"
                if "%%a"=="DATABASE" set "DEFAULT_DATABASE=!value!"
                if "%%a"=="SCHEMA" set "DEFAULT_SCHEMA=!value!"
                if "%%a"=="WAREHOUSE" set "DEFAULT_WAREHOUSE=!value!"
                if "%%a"=="APP_NAME" set "DEFAULT_APP_NAME=!value!"
                if "%%a"=="APP_ID" set "DEFAULT_APP_ID=!value!"
                if "%%a"=="CREATE_ROLES" set "DEFAULT_CREATE_ROLES=!value!"
                if "%%a"=="DEFAULT_LLM_MODEL" set "DEFAULT_LLM_MODEL=!value!"
            )
        )
    )
)

REM Initialize with defaults
set "DATABASE=%DEFAULT_DATABASE%"
set "SCHEMA=%DEFAULT_SCHEMA%"
set "WAREHOUSE=%DEFAULT_WAREHOUSE%"
set "APP_NAME=%DEFAULT_APP_NAME%"
set "APP_ID=%DEFAULT_APP_ID%"
set "CREATE_ROLES=%DEFAULT_CREATE_ROLES%"
set "LLM_MODEL=%DEFAULT_LLM_MODEL%"
set "SKIP_PERMISSION_CHECK=false"

REM Parse command line arguments
for %%a in (%*) do (
    if "%%a"=="--skip-permission-check" set "SKIP_PERMISSION_CHECK=true"
)

echo.
echo ================================================================
echo            Field Mapper - Snowflake Deployment
echo ================================================================
echo.

REM Check if snow CLI is installed
where snow >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Snowflake CLI ^(snow^) is not installed.
    echo         Install it with: pip install snowflake-cli
    exit /b 1
)

echo [OK] Snowflake CLI found
echo.

REM Check for required Snowflake privileges (unless skipped)
if "%SKIP_PERMISSION_CHECK%"=="true" (
    echo [SKIP] Skipping permission checks ^(--skip-permission-check flag^)
    echo.
) else (
    echo [INFO] Checking Snowflake role privileges...

    REM Check if user can use SYSADMIN
    snow sql -q "USE ROLE SYSADMIN; SELECT 'YES';" >nul 2>nul
    if !ERRORLEVEL! neq 0 (
        echo.
        echo ================================================================
        echo                    [ERROR] PERMISSION ERROR
        echo ================================================================
        echo.
        echo Your current user does not have access to the SYSADMIN role.
        echo.
        echo The deployment requires SYSADMIN privileges to:
        echo    - Create databases and schemas
        echo    - Grant object privileges
        echo    - Deploy stored procedures
        echo.
        echo Please contact your Snowflake administrator to grant SYSADMIN access,
        echo or run this deployment with a user that has the required privileges.
        echo.
        echo [TIP] Use --skip-permission-check to bypass this check
        echo.
        exit /b 1
    )
    echo    [OK] SYSADMIN access confirmed

    REM Check if user can use SECURITYADMIN (only required if CREATE_ROLES=true)
    if "!DEFAULT_CREATE_ROLES!"=="true" (
        snow sql -q "USE ROLE SECURITYADMIN; SELECT 'YES';" >nul 2>nul
        if !ERRORLEVEL! neq 0 (
            echo.
            echo ================================================================
            echo                    [ERROR] PERMISSION ERROR
            echo ================================================================
            echo.
            echo Your current user does not have access to the SECURITYADMIN role.
            echo.
            echo The deployment requires SECURITYADMIN privileges to:
            echo    - Create database access roles
            echo    - Establish role hierarchy
            echo    - Grant roles to users
            echo.
            echo Options:
            echo    1. Contact your Snowflake administrator to grant SECURITYADMIN access
            echo    2. Set CREATE_ROLES="false" in deploy.config to skip role creation
            echo    3. Use --skip-permission-check to bypass this check
            echo.
            exit /b 1
        )
        echo    [OK] SECURITYADMIN access confirmed
    ) else (
        echo    [SKIP] SECURITYADMIN check skipped ^(CREATE_ROLES=false^)
    )
    echo.
)

REM Configuration prompt
echo [CONFIG] Configuration ^(from deploy.config^):
echo.
echo    Database:     %DEFAULT_DATABASE%
echo    Schema:       %DEFAULT_SCHEMA%
echo    Warehouse:    %DEFAULT_WAREHOUSE%
echo    App Name:     %DEFAULT_APP_NAME%
echo    App ID:       %DEFAULT_APP_ID%
echo    Create Roles: %DEFAULT_CREATE_ROLES%
echo    LLM Model:    %DEFAULT_LLM_MODEL%
echo.

REM Check for --defaults or -y flag to skip prompts
set "USE_DEFAULTS=0"
if "%~1"=="--defaults" set "USE_DEFAULTS=1"
if "%~1"=="-y" set "USE_DEFAULTS=1"

if "%USE_DEFAULTS%"=="1" (
    echo Using config values ^(--defaults flag detected^)
    echo.
) else (
    set /p "use_defaults=Use these values? (Y/n): "
    
    if /i "!use_defaults!"=="n" (
        echo.
        echo Enter custom values ^(press Enter to keep current^):
        echo.
        
        set /p "input_database=Database name [%DEFAULT_DATABASE%]: "
        if not "!input_database!"=="" set "DATABASE=!input_database!"
        
        set /p "input_schema=Schema name [%DEFAULT_SCHEMA%]: "
        if not "!input_schema!"=="" set "SCHEMA=!input_schema!"
        
        set /p "input_warehouse=Warehouse name [%DEFAULT_WAREHOUSE%]: "
        if not "!input_warehouse!"=="" set "WAREHOUSE=!input_warehouse!"
        
        set /p "input_app_name=App display name [%DEFAULT_APP_NAME%]: "
        if not "!input_app_name!"=="" set "APP_NAME=!input_app_name!"
        
        set /p "input_app_id=App identifier ^(no spaces^) [%DEFAULT_APP_ID%]: "
        if not "!input_app_id!"=="" set "APP_ID=!input_app_id!"
        
        echo.
        echo Configuration:
        echo    Database:  !DATABASE!
        echo    Schema:    !SCHEMA!
        echo    Warehouse: !WAREHOUSE!
        echo    App Name:  !APP_NAME!
        echo    App ID:    !APP_ID!
        echo.
        
        set /p "confirm=Proceed with deployment? (Y/n): "
        if /i "!confirm!"=="n" (
            echo Deployment cancelled.
            exit /b 0
        )
    ) else (
        echo Using config values.
    )
)

echo.
echo Starting deployment...
echo.

REM Get current Snowflake user for RBAC setup
echo [INFO] Getting current Snowflake user...
for /f "tokens=*" %%i in ('snow sql -q "SELECT CURRENT_USER^(^);" 2^>nul ^| findstr /r "^|" ^| findstr /v "CURRENT_USER" ^| findstr /v "^\-"') do (
    set "line=%%i"
)
REM Parse the username from the table output
for /f "tokens=2 delims=|" %%a in ("!line!") do (
    set "CURRENT_USER=%%a"
)
REM Trim whitespace
for /f "tokens=* delims= " %%a in ("!CURRENT_USER!") do set "CURRENT_USER=%%a"
for /l %%a in (1,1,100) do if "!CURRENT_USER:~-1!"==" " set "CURRENT_USER=!CURRENT_USER:~0,-1!"

if "!CURRENT_USER!"=="" (
    echo [ERROR] Could not determine current Snowflake user.
    echo         Please ensure your Snow CLI connection is configured correctly.
    exit /b 1
)
echo [OK] Current user: !CURRENT_USER!
echo.

REM Step 1: Setup RBAC roles and create database/schema
if "%CREATE_ROLES%"=="true" (
    echo [STEP 1] Setting up RBAC roles and database...
    
    set "RBAC_FILE=%SCRIPT_DIR%setup_rbac.sql"
    
    if exist "!RBAC_FILE!" (
        echo    Creating role hierarchy: !DATABASE!_READONLY -^> !DATABASE!_READWRITE -^> !DATABASE!_ADMIN
        
        REM Create temp file with replaced placeholders
        set "TEMP_RBAC_FILE=%TEMP%\rbac_setup_%RANDOM%.sql"
        
        REM Use PowerShell for reliable text replacement
        powershell -Command "(Get-Content '!RBAC_FILE!') -replace '\{\{DATABASE\}\}', '!DATABASE!' -replace '\{\{SCHEMA\}\}', '!SCHEMA!' -replace '\{\{WAREHOUSE\}\}', '!WAREHOUSE!' -replace '\{\{CURRENT_USER\}\}', '!CURRENT_USER!' | Set-Content '!TEMP_RBAC_FILE!'"
        
        REM Execute RBAC setup
        snow sql -f "!TEMP_RBAC_FILE!"
        if !ERRORLEVEL! neq 0 (
            del "!TEMP_RBAC_FILE!" 2>nul
            echo.
            echo [ERROR] Failed to execute RBAC setup.
            echo         Please check the error messages above and ensure you have the required privileges.
            exit /b 1
        )
        del "!TEMP_RBAC_FILE!" 2>nul
        echo [OK] RBAC roles and database ready
    ) else (
        echo [ERROR] setup_rbac.sql not found at !RBAC_FILE!
        echo         This file is required for deployment.
        exit /b 1
    )
    echo.
    
    REM Switch to the admin role for remaining operations
    echo [INFO] Switching to !DATABASE!_ADMIN role...
    snow sql -q "USE ROLE !DATABASE!_ADMIN;" 2>nul
    if !ERRORLEVEL! neq 0 (
        echo [WARN] Could not switch to !DATABASE!_ADMIN role, continuing with current role...
    )
) else (
    echo [STEP 1] Skipping RBAC role creation ^(CREATE_ROLES=false^)
    echo.
    
    REM Create database and schema without RBAC
    echo [INFO] Creating database and schema...
    snow sql -q "CREATE DATABASE IF NOT EXISTS !DATABASE!;" 2>nul
    snow sql -q "CREATE SCHEMA IF NOT EXISTS !DATABASE!.!SCHEMA!;" 2>nul
    echo [OK] Database and schema ready
)

REM Step 2: Create the mappings and config tables
echo [STEP 2] Creating tables...

REM Create mappings table
snow sql -q "CREATE TABLE IF NOT EXISTS !DATABASE!.!SCHEMA!.MAPPINGS_LIST (SRC VARCHAR, TARGET VARCHAR, DESCRIPTION VARCHAR);" 2>nul

REM Add DESCRIPTION column if it doesn't exist (for existing tables)
snow sql -q "ALTER TABLE !DATABASE!.!SCHEMA!.MAPPINGS_LIST ADD COLUMN IF NOT EXISTS DESCRIPTION VARCHAR;" 2>nul

REM Create app config table
snow sql -q "CREATE TABLE IF NOT EXISTS !DATABASE!.!SCHEMA!.APP_CONFIG (CONFIG_KEY VARCHAR PRIMARY KEY, CONFIG_VALUE VARCHAR);" 2>nul

REM Store APP_NAME in config (escape single quotes)
set "APP_NAME_ESCAPED=!APP_NAME:'=''!"
snow sql -q "MERGE INTO !DATABASE!.!SCHEMA!.APP_CONFIG AS target USING (SELECT 'APP_NAME' AS CONFIG_KEY, '!APP_NAME_ESCAPED!' AS CONFIG_VALUE) AS source ON target.CONFIG_KEY = source.CONFIG_KEY WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = source.CONFIG_VALUE WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE) VALUES (source.CONFIG_KEY, source.CONFIG_VALUE);" 2>nul

REM Store DEFAULT_LLM_MODEL in config
set "LLM_MODEL_ESCAPED=!LLM_MODEL:'=''!"
snow sql -q "MERGE INTO !DATABASE!.!SCHEMA!.APP_CONFIG AS target USING (SELECT 'DEFAULT_LLM_MODEL' AS CONFIG_KEY, '!LLM_MODEL_ESCAPED!' AS CONFIG_VALUE) AS source ON target.CONFIG_KEY = source.CONFIG_KEY WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = source.CONFIG_VALUE WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE) VALUES (source.CONFIG_KEY, source.CONFIG_VALUE);" 2>nul

echo [OK] Tables ready
echo.

REM Step 3: Load initial data from CSV file using MERGE
echo [STEP 3] Loading field mappings data from mappings.csv...

set "CSV_FILE=%SCRIPT_DIR%mappings.csv"

if not exist "%CSV_FILE%" (
    echo [ERROR] mappings.csv not found at %CSV_FILE%
    exit /b 1
)

REM Build VALUES clause from CSV file using PowerShell for reliable CSV parsing
set "VALUES_FILE=%TEMP%\values_%RANDOM%.txt"

powershell -Command ^
    "$csv = Import-Csv '%CSV_FILE%'; " ^
    "$values = @(); " ^
    "foreach ($row in $csv) { " ^
    "    $src = $row.SRC -replace \"'\", \"''\"; " ^
    "    $target = $row.TARGET -replace \"'\", \"''\"; " ^
    "    $desc = $row.DESCRIPTION -replace \"'\", \"''\"; " ^
    "    $values += \"('$src', '$target', '$desc')\"; " ^
    "} " ^
    "$values -join \",`n    \" | Out-File -Encoding ASCII '%VALUES_FILE%'"

set /p VALUES_CLAUSE=<"%VALUES_FILE%"
for /f "usebackq delims=" %%i in ("%VALUES_FILE%") do set "VALUES_CLAUSE=%%i"

REM Read full content with PowerShell
for /f "usebackq delims=" %%i in (`powershell -Command "Get-Content '%VALUES_FILE%' -Raw"`) do set "VALUES_CLAUSE=%%i"

REM Execute MERGE using PowerShell to handle the multiline SQL properly
powershell -Command ^
    "$values = Get-Content '%VALUES_FILE%' -Raw; " ^
    "$sql = \"MERGE INTO !DATABASE!.!SCHEMA!.MAPPINGS_LIST AS target USING (SELECT * FROM VALUES $values AS source (src, target, description)) AS source ON target.src = source.src AND target.target = source.target WHEN NOT MATCHED THEN INSERT (src, target, description) VALUES (source.src, source.target, source.description) WHEN MATCHED THEN UPDATE SET description = source.description;\"; " ^
    "snow sql -q $sql"

del "%VALUES_FILE%" 2>nul

echo [OK] Field mappings loaded from mappings.csv
echo.

REM Step 4: Deploy the stored procedure from mapping_proc.sql
echo [STEP 4] Deploying field matcher stored procedure from mapping_proc.sql...

set "SQL_FILE=%SCRIPT_DIR%mapping_proc.sql"

if not exist "%SQL_FILE%" (
    echo [ERROR] mapping_proc.sql not found at %SQL_FILE%
    exit /b 1
)

REM Use PowerShell to read and modify the SQL file
set "TEMP_PROC_FILE=%TEMP%\proc_%RANDOM%.sql"
powershell -Command "(Get-Content '%SQL_FILE%' -Raw) -replace 'CREATE OR REPLACE PROCEDURE field_matcher_advanced', 'CREATE OR REPLACE PROCEDURE !DATABASE!.!SCHEMA!.field_matcher_advanced' | Set-Content '%TEMP_PROC_FILE%' -Encoding ASCII"

snow sql -f "%TEMP_PROC_FILE%"
del "%TEMP_PROC_FILE%" 2>nul

echo [OK] Stored procedure deployed from mapping_proc.sql
echo.

REM Step 5: Deploy the Streamlit app
echo [STEP 5] Deploying Streamlit app...
set "SNOWFLAKE_WAREHOUSE=!WAREHOUSE!"
set "APP_NAME=!APP_NAME!"
set "APP_ID=!APP_ID!"

snow streamlit deploy --database "!DATABASE!" --schema "!SCHEMA!" --replace

echo [OK] Streamlit app deployed
echo.

REM Step 6: Get the app URL
echo [STEP 6] Getting app URL...
for /f "tokens=*" %%i in ('snow streamlit get-url "!APP_ID!" --database "!DATABASE!" --schema "!SCHEMA!" 2^>nul') do set "APP_URL=%%i"

echo.
echo ================================================================
echo                     Deployment Complete!
echo ================================================================
echo.
echo    Database:  !DATABASE!
echo    Schema:    !SCHEMA!
echo    Warehouse: !WAREHOUSE!
echo    App Name:  !APP_NAME!
echo    App ID:    !APP_ID!
echo.
if not "!APP_URL!"=="" (
    echo    App URL: !APP_URL!
) else (
    echo    Open Snowsight and navigate to Streamlit to view the app
)
echo.
echo [NEXT STEPS]
echo    1. Open the app in Snowsight
echo    2. Use 'Edit Mappings' tab to manage field mappings
echo    3. Use 'Test Matches' tab to test the matching algorithm
echo    4. Use 'Upload ^& Map File' tab to map and load data files
echo.
echo [TIPS]
echo    - Edit deploy.config to change default values
echo    - Run with --defaults or -y flag to skip prompts
echo    - Run with --skip-permission-check to bypass role checks
echo.

endlocal


