# ClaimsIQ - Healthcare Field Mapping Application

A Streamlit in Snowflake (SiS) application for intelligent field mapping in healthcare claims data processing. ClaimsIQ automates the tedious process of mapping source data columns to standardized target schemas using both algorithmic matching and AI-powered recommendations.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Deployment](#deployment)
- [Configuration](#configuration)
- [Usage Guide](#usage-guide)
- [Field Mappings](#field-mappings)
- [Matching Algorithms](#matching-algorithms)
- [LLM Integration](#llm-integration)
- [Sample Data Generation](#sample-data-generation)
- [Troubleshooting](#troubleshooting)

## Overview

ClaimsIQ solves the common challenge of mapping healthcare claims data from various sources (insurance companies, TPAs, providers) to a standardized target schema. It offers two matching approaches:

1. **Stored Procedure Matching**: Uses `field_matcher_advanced` with TF-IDF similarity algorithms
2. **LLM Matching**: Uses Snowflake Cortex AI models for intelligent semantic matching

The application supports CSV, TXT, and Excel files, and creates standardized tables in Snowflake with consistent column structures.

## Features

### 📤 Upload & Map File
- **Multi-format Support**: Upload CSV, TXT (tab/pipe/comma delimited), or Excel (XLS/XLSX) files
- **Dual Matching Methods**: Choose between Stored Procedure or LLM-based matching
- **LLM Model Selection**: Select from multiple Cortex AI models (Snowflake Arctic, Mistral, Llama, Gemma)
- **Confidence Threshold Slider**: Adjust auto-mapping sensitivity (0.0-1.0)
- **Visual Confidence Indicators**: Color-coded scores (🟢 High ≥0.8, 🟡 Medium 0.5-0.8, 🔴 Low <0.5)
- **Duplicate Detection**: Warns when multiple source columns map to the same target
- **Add to Mappings**: Save new source→target mappings directly from the mapping interface
- **Full Schema Tables**: Creates tables with ALL target columns (unmapped columns are NULL)
- **Debug Panel**: View SQL queries, raw results, and LLM responses

### 📝 Edit Mappings
- **Interactive Data Editor**: Browse, edit, add, and delete field mappings
- **Filter & Search**: Filter by target field or search source fields
- **Bulk Operations**: Delete multiple mappings, export to CSV
- **Summary Statistics**: View total mappings, unique targets, and unique sources
- **Protected Base Mappings**: Prevents deletion of core mappings where SRC=TARGET

### 🔍 Test Matches
- **Algorithm Testing**: Test field matching with custom input
- **Configurable Parameters**: Adjust top_n results and minimum threshold
- **Detailed Scoring**: View all similarity components (exact, substring, sequence, word overlap, TF-IDF)
- **Quick Test Examples**: Pre-loaded test cases for common scenarios
- **Results Export**: Download matching results as CSV

### 📊 View Tables
- **Table Browser**: View all tables in the PROCESSOR schema
- **Data Preview**: Preview table contents with configurable row limits
- **Schema Inspector**: View column names, data types, and positions
- **Table Metrics**: Row counts, storage size, creation/modification dates
- **Table Management**: Delete tables with confirmation (system tables protected)

### 🤖 LLM Settings
- **Editable Prompt Template**: Customize the LLM prompt for field matching
- **Placeholder Support**: Use `{source_list}` and `{target_list}` placeholders
- **Prompt Preview**: Preview the formatted prompt with sample data
- **Reset to Default**: Restore the original prompt template
- **Best Practices Tips**: Guidance for crafting effective prompts

## Project Structure

```
streamlit_fieldmatch/
├── streamlit_app.py          # Main Streamlit application
├── environment.yml           # Python dependencies for Snowflake
├── snowflake.yml            # Snowflake project configuration
├── deploy.sh                # Automated deployment script
├── mapping_proc.sql         # field_matcher_advanced stored procedure
├── mappings.csv             # Initial field mappings (SRC, TARGET, DESCRIPTION)
├── generate_sample_data.py  # Sample data generation script
├── sample_data/             # Generated sample claims files
│   ├── anthem_bluecross-claims-20240115.csv
│   ├── unitedhealth-claims-20240201.csv
│   ├── cigna_healthcare-claims-20240215.xlsx
│   ├── aetna_dental-claims-20240301.csv
│   └── kaiser_permanente-claims-20240315.xlsx
└── README.md                # This file
```

## Prerequisites

### Snowflake Requirements
- Snowflake account with appropriate privileges
- Warehouse for running queries
- Privileges to create: databases, schemas, tables, stored procedures, Streamlit apps

### Local Requirements
- [Snow CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index) installed and configured
- Default connection configured in Snow CLI (`snow connection test`)
- Python 3.x (for sample data generation only)

### Python Packages (for sample data generation)
```bash
pip install pandas numpy openpyxl scikit-learn
```

## Deployment

### Quick Start

```bash
# 1. Clone or download the project
cd streamlit_fieldmatch

# 2. Make deploy script executable
chmod +x deploy.sh

# 3. Run deployment
./deploy.sh
```

### What the Deploy Script Does

1. **Creates Database & Schema**: `CLAIMSIQ.PROCESSOR`
2. **Creates Mappings Table**: `MAPPINGS_LIST` with SRC, TARGET, DESCRIPTION columns
3. **Loads Initial Mappings**: Reads from `mappings.csv` using MERGE (upsert)
4. **Deploys Stored Procedure**: Reads from `mapping_proc.sql`
5. **Deploys Streamlit App**: Uses Snow CLI to deploy the application
6. **Outputs App URL**: Provides link to access the application

### Deployment Output Example

```
╔════════════════════════════════════════════════════════════╗
║              ClaimsIQ - Snowflake Deployment               ║
╚════════════════════════════════════════════════════════════╝

✓ Snowflake CLI found
📦 Step 1: Setting up database and schema...
✓ Database and schema ready
📋 Step 2: Creating mappings table...
✓ Mappings table ready
📊 Step 3: Loading field mappings data from mappings.csv...
✓ Field mappings loaded from mappings.csv
🔧 Step 4: Deploying field matcher stored procedure from mapping_proc.sql...
✓ Stored procedure deployed from mapping_proc.sql
🚀 Step 5: Deploying Streamlit app...
✓ Streamlit app deployed

╔════════════════════════════════════════════════════════════╗
║                    Deployment Complete!                     ║
╚════════════════════════════════════════════════════════════╝

📍 Database: CLAIMSIQ
📍 Schema:   PROCESSOR
📍 App:      CLAIMSIQ_APP
🌐 Open Snowsight and navigate to Streamlit to view the app
```

## Configuration

### Deploy Script Variables

Edit `deploy.sh` to customize:

```bash
DATABASE="CLAIMSIQ"           # Target database name
SCHEMA="PROCESSOR"            # Target schema name
WAREHOUSE="COMPUTE_WH"        # Warehouse for queries (update this!)
```

### Snowflake Project Configuration

The `snowflake.yml` file configures the Streamlit deployment:

```yaml
definition_version: 2
entities:
  claimsiq_app:
    type: streamlit
    identifier:
      name: claimsiq_app
    title: "ClaimsIQ - Healthcare Field Mapping"
    query_warehouse: COMPUTE_WH
    main_file: streamlit_app.py
    env_file: environment.yml
```

## Usage Guide

### Processing a New File

#### Step 1: Upload File
1. Go to **📤 Upload & Map File** tab
2. Upload your CSV, TXT, or Excel file
3. Review the data preview and detected columns
4. Click **Continue to Column Mapping**

#### Step 2: Map Columns
1. **Select Matching Method**:
   - **Stored Procedure**: Uses TF-IDF similarity (default)
   - **LLM (Cortex AI)**: Uses AI for semantic matching
2. If using LLM, select a model and click **Run LLM Matching**
3. Adjust the **Confidence Threshold** slider
4. Review auto-suggested mappings
5. Manually adjust any incorrect mappings
6. Click **➕ Add** to save new mappings for future use
7. Resolve any duplicate mapping warnings
8. Click **Continue** when ready

#### Step 3: Review & Create Table
1. Enter a **Table Name** (auto-generated from filename)
2. Review the **Column Mapping Summary**
3. Preview the transformed data
4. Click **Create Table Now**

#### Step 4: Complete
1. View success confirmation
2. Preview the created table data
3. Upload another file or view tables

### Managing Field Mappings

1. Go to **📝 Edit Mappings** tab
2. Click **🔄 Refresh Data** to load current mappings
3. Use filters to find specific mappings
4. Edit directly in the data grid
5. Add new mappings using the form at the bottom
6. Click **💾 Save Changes** to persist edits
7. Use **📥 Download as CSV** to export mappings

### Testing the Matching Algorithm

1. Go to **🔍 Test Matches** tab
2. Enter field names in the text area (one per line)
3. Or use **Quick Test Examples** buttons
4. Adjust **Number of matches** and **Minimum threshold**
5. Click **🚀 Run Field Matching Test**
6. Review detailed similarity scores
7. Download results as CSV

### Configuring LLM Settings

1. Go to **🤖 LLM Settings** tab
2. Edit the prompt template in the text area
3. Use placeholders:
   - `{source_list}` - Source column names
   - `{target_list}` - Target field names
4. Preview the prompt with sample data
5. Click **💾 Save Changes** or **🔄 Reset to Default**

## Field Mappings

### Mappings CSV Format

The `mappings.csv` file defines source-to-target field mappings:

```csv
SRC,TARGET,DESCRIPTION
POLICY_EFF_DT,Policy Effective Date,Date when the insurance policy became effective
CLM_CTRL_NBR,Claim Number/Claim Control Number,Unique identifier for the claim in the system
BILLED_AMT,Billed Amount,Total amount billed by the provider for services
```

| Column | Description |
|--------|-------------|
| `SRC` | Source field name (database column name format) |
| `TARGET` | Standardized target field name (human-readable) |
| `DESCRIPTION` | Explanation of what the field contains |

### Pre-loaded Field Categories

| Category | Target Fields |
|----------|---------------|
| **Policy** | Policy Effective Date, Group Name, Product Type |
| **Insured** | Insured First Name, Insured Last Name, Insured DOB |
| **Claimant** | Claimant First Name, Claimant Last Name, Claimant DOB |
| **Claim** | Claim Number/Claim Control Number, Beginning Service Date, Ending Service Date, Processed Date |
| **Diagnosis** | Primary ICD, Secondary ICD |
| **Procedure** | CPT Code, HCPCS Code, Revenue Code, Modifier Code |
| **Provider** | NPI?, Payee Name, Payee Address, Payee TIN |
| **Pharmacy** | Rx Name, Rx Quantity, Rx Days Supply, Rx Date Filled |
| **Amounts** | Billed Amount, Copay Amount, Deductible Amount, Coinsurance Amount, Allowed Amount, Net Amount, Ineligible Amount, COB Amount, Other Reduced Amount, Denied Amount, Paid Amount |
| **Other** | Service Line/Claim Type |

### Adding New Mappings

**Method 1: Via CSV**
```bash
# Edit mappings.csv
echo "NEW_COL,Target Field Name,Description of the field" >> mappings.csv
# Re-run deployment
./deploy.sh
```

**Method 2: Via Application UI**
1. Go to **📝 Edit Mappings** tab
2. Use the **➕ Add New Mapping** form
3. Click **Save Changes**

**Method 3: Via SQL**
```sql
INSERT INTO CLAIMSIQ.PROCESSOR.MAPPINGS_LIST (SRC, TARGET, DESCRIPTION)
VALUES ('NEW_COL', 'Target Field Name', 'Description of the field');
```

## Matching Algorithms

### Stored Procedure: field_matcher_advanced

The stored procedure combines multiple similarity algorithms:

| Algorithm | Weight | Description |
|-----------|--------|-------------|
| Exact Match | 40% | Case-insensitive exact string comparison |
| Substring Match | 20% | Checks if one string contains the other |
| Sequence Similarity | 20% | difflib SequenceMatcher ratio |
| Word Overlap | 20% | Jaccard similarity of word sets |
| TF-IDF Cosine | 30% boost | Semantic similarity using n-grams |

**Final Score Calculation:**
```
Basic Score = (Exact × 0.4) + (Substring × 0.2) + (Sequence × 0.2) + (Word Overlap × 0.2)
Final Score = (Basic Score × 0.7) + (TF-IDF Score × 0.3)
```

### Procedure Signature

```sql
CALL CLAIMSIQ.PROCESSOR.field_matcher_advanced(
    input_fields ARRAY,      -- Array of field names to match
    top_n INTEGER,           -- Number of top matches per field (default: 3)
    min_threshold FLOAT      -- Minimum score threshold (default: 0.1)
)
```

### Return Columns

| Column | Type | Description |
|--------|------|-------------|
| INPUT_FIELD | STRING | The input field name being matched |
| SRC_FIELD | STRING | The matched source field from mappings |
| TARGET_FIELD | STRING | The standardized target field |
| COMBINED_SCORE | FLOAT | Overall confidence score (0-1) |
| EXACT_SCORE | FLOAT | Exact match component |
| SUBSTRING_SCORE | FLOAT | Substring match component |
| SEQUENCE_SCORE | FLOAT | Sequence similarity component |
| WORD_OVERLAP | FLOAT | Word overlap component |
| TFIDF_SCORE | FLOAT | TF-IDF cosine similarity |
| MATCH_RANK | INTEGER | Rank among matches for this input |

## LLM Integration

### Supported Cortex AI Models

The available LLM models are **fetched dynamically** from Snowflake using:

```sql
SHOW MODELS IN SNOWFLAKE.MODELS;

SELECT "name" AS model_name
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "model_type" = 'CORTEX_BASE'
ORDER BY "name";
```

This ensures you always see the models available in your specific Snowflake region. Common models include:

| Model | Description |
|-------|-------------|
| `llama3.1-70b` | Meta's Llama 3.1 70B (widely available) |
| `llama3.1-8b` | Meta's Llama 3.1 8B (faster) |
| `mistral-large2` | Mistral AI's large model v2 |
| `mistral-7b` | Mistral 7B model |
| `claude-3-5-sonnet` | Anthropic Claude 3.5 Sonnet |
| `snowflake-llama-3.3-70b` | Snowflake's optimized Llama |
| `deepseek-r1` | DeepSeek R1 reasoning model |

**Note:** The dropdown will show only models available in your region.

### LLM Matching Features

- **Semantic Understanding**: Recognizes healthcare abbreviations (DOB, NPI, CPT, ICD, Rx)
- **Pattern Recognition**: Understands naming conventions (amt=Amount, dt=Date)
- **Duplicate Prevention**: Ensures each target is mapped only once
- **Confidence Scoring**: Returns confidence levels for each mapping
- **Customizable Prompts**: Edit the prompt template in LLM Settings tab

### Default Prompt Template

The LLM prompt can be customized in the **🤖 LLM Settings** tab. The default template:
- Lists all source columns and target fields
- Instructs the LLM to match based on semantic meaning
- Requires JSON output with confidence scores
- Enforces one-to-one target mapping

## Sample Data Generation

### Generate Test Files

```bash
python3 generate_sample_data.py
```

### Generated Files

| File | Format | Records | Column Style | Claim Type |
|------|--------|---------|--------------|------------|
| `anthem_bluecross-claims-20240115.csv` | CSV | ~1,000 | `UPPERCASE_UNDERSCORES` | Medical |
| `unitedhealth-claims-20240201.csv` | CSV | ~1,000 | `CamelCase` | Pharmacy |
| `cigna_healthcare-claims-20240215.xlsx` | Excel | ~1,000 | `Spaces In Names` | Mixed |
| `aetna_dental-claims-20240301.csv` | CSV | ~1,000 | `ABBREVIATED_CAPS` | Dental |
| `kaiser_permanente-claims-20240315.xlsx` | Excel | ~1,000 | `CamelCase` | Vision |

### Sample Data Contents

- **Demographics**: Patient/subscriber names, dates of birth
- **Identifiers**: Claim numbers, NPI numbers, TINs
- **Dates**: Service dates, processed dates, fill dates
- **Codes**: ICD-10, CPT, HCPCS, CDT, Revenue codes
- **Amounts**: Billed, allowed, deductible, copay, coinsurance, paid
- **Provider Info**: Names, addresses, tax IDs

## Troubleshooting

### Deployment Issues

**Snow CLI not found**
```bash
pip install snowflake-cli
snow connection add  # Configure connection
snow connection test # Verify connection
```

**Permission errors**
```sql
-- Grant necessary privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE your_role;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE your_role;
```

**Missing files error**
- Ensure `mappings.csv` exists in the same directory as `deploy.sh`
- Ensure `mapping_proc.sql` exists in the same directory as `deploy.sh`

### Application Issues

**Auto-mappings not appearing**
1. Check the Debug panel in Step 2
2. Verify stored procedure exists:
   ```sql
   SHOW PROCEDURES LIKE 'field_matcher_advanced' IN SCHEMA CLAIMSIQ.PROCESSOR;
   ```
3. Verify mappings exist:
   ```sql
   SELECT COUNT(*) FROM CLAIMSIQ.PROCESSOR.MAPPINGS_LIST;
   ```

**Low confidence scores**
- Add more source field variations to mappings
- Lower the confidence threshold slider
- Try LLM matching for better semantic understanding

**LLM matching fails**
- Verify Cortex AI is enabled for your account
- Check the Debug panel for error messages
- Try a different model (some may not be available in all regions)

**Duplicate mapping warnings**
- Review the highlighted rows in the mapping interface
- Manually select the correct mapping for each target
- LLM matching automatically resolves duplicates by confidence score

### Sample Data Generation Issues

**Module not found errors**
```bash
pip install pandas numpy openpyxl scikit-learn
```

**Python version issues**
- Requires Python 3.x
- Use `python3` instead of `python` if needed

## License

This project is provided as-is for healthcare data processing workflows.

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review the Debug panel in the application
3. Verify Snowflake permissions and connectivity
