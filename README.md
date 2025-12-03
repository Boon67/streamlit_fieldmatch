# ClaimsIQ - Healthcare Field Mapping Application

A Streamlit in Snowflake (SiS) application for intelligent field mapping in healthcare claims data processing.

## Overview

ClaimsIQ uses the `field_matcher_advanced` stored procedure to intelligently match source field names to standardized target fields. It combines multiple similarity algorithms (exact match, substring, sequence matching, word overlap, and TF-IDF) to provide accurate field mapping suggestions with confidence scores.

## Features

### 📤 Upload & Map File (Primary Tab)
- **File Upload**: Upload CSV or TXT files for processing
- **Intelligent Auto-Mapping**: Automatically suggests target fields using the `field_matcher_advanced` stored procedure
- **Confidence Threshold Slider**: Adjust the threshold (0.0-1.0) to control auto-mapping sensitivity
- **Visual Confidence Indicators**: Color-coded scores (🟢 High, 🟡 Medium, 🔴 Low)
- **Full Schema Table Creation**: Creates tables with ALL target columns, populating mapped fields and leaving unmapped as NULL
- **Debug Panel**: View the SQL executed and raw results for troubleshooting

### 📝 Edit Mappings
- **View & Edit**: Browse and modify existing field mappings
- **Filter & Search**: Filter by target field or search source fields
- **Add New Mappings**: Add source fields mapped to existing targets
- **Bulk Operations**: Delete mappings by target field, export to CSV
- **Summary Statistics**: View total mappings, unique targets, and unique sources

### 🔍 Test Matches
- **Test Field Matching**: Enter field names to test the matching algorithm
- **Configurable Parameters**: Adjust top_n results and minimum threshold
- **Detailed Results**: View all similarity scores (exact, substring, sequence, word overlap, TF-IDF)
- **Example Sets**: Pre-loaded test cases for quick testing

## Files

| File | Description |
|------|-------------|
| `streamlit_app.py` | Main Streamlit application |
| `environment.yml` | Python dependencies for Snowflake |
| `mapping_proc.sql` | `field_matcher_advanced` stored procedure |
| `deploy.sh` | Automated deployment script using Snow CLI |
| `snowflake.yml` | Snowflake project configuration |

## Deployment

### Prerequisites

- Snowflake account with appropriate privileges
- [Snow CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index) installed and configured
- Default connection configured in Snow CLI

### Deploy with Snow CLI

1. **Make the deploy script executable:**
   ```bash
   chmod +x deploy.sh
   ```

2. **Run the deployment:**
   ```bash
   ./deploy.sh
   ```

The script will:
- Create the `CLAIMSIQ` database and `PROCESSOR` schema
- Create the `MAPPINGS_LIST` table with initial field mappings
- Deploy the `field_matcher_advanced` stored procedure
- Deploy the Streamlit application

### Configuration

Update these variables in `deploy.sh` if needed:

```bash
DATABASE="CLAIMSIQ"
SCHEMA="PROCESSOR"
WAREHOUSE="COMPUTE_WH"  # Update with your warehouse name
```

## Usage

### Processing a New File

1. **Upload**: Go to "Upload & Map File" tab and upload your CSV/TXT file
2. **Review Headers**: Confirm the extracted column headers
3. **Adjust Threshold**: Use the slider to set the confidence threshold for auto-mapping
4. **Map Columns**: Review and adjust the suggested mappings
5. **Create Table**: Approve the mapping and create the target table

### Managing Mappings

1. Go to "Edit Mappings" tab
2. Click "Load Data" to view current mappings
3. Use the data editor to modify mappings
4. Add new source→target mappings using the form
5. Click "Save Changes" to persist to Snowflake

### Testing the Algorithm

1. Go to "Test Matches" tab
2. Enter field names (one per line or comma-separated)
3. Adjust parameters (top_n, min_threshold)
4. Click "Run Field Matching Test"
5. Review the detailed similarity scores

## Field Mappings

The application comes pre-loaded with 39 healthcare/claims field mappings including:

| Category | Fields |
|----------|--------|
| **Policy** | Policy Effective Date, Group Name, Product Type |
| **Insured** | First Name, Last Name, DOB |
| **Claimant** | First Name, Last Name, DOB |
| **Claim** | Claim Number, Service Dates, Processed Date |
| **Diagnosis** | Primary ICD, Secondary ICD |
| **Procedure** | CPT Code, HCPCS Code, Revenue Code, Modifier Code |
| **Amounts** | Billed, Copay, Deductible, Coinsurance, Allowed, Net, Paid, Denied |
| **Pharmacy** | Rx Name, Quantity, Days Supply, Date Filled |
| **Payee** | Name, Address, TIN |

## Stored Procedure: field_matcher_advanced

The core matching logic uses multiple similarity algorithms:

1. **Exact Match** (40% weight): Case-insensitive exact string match
2. **Substring Match** (20% weight): Checks if one string contains the other
3. **Sequence Similarity** (20% weight): Uses difflib's SequenceMatcher
4. **Word Overlap** (20% weight): Jaccard similarity of word sets
5. **TF-IDF Cosine Similarity** (30% of final): Semantic similarity using n-grams

Final score = (Basic similarity × 70%) + (TF-IDF × 30%)

### Procedure Signature

```sql
CALL CLAIMSIQ.PROCESSOR.field_matcher_advanced(
    input_fields ARRAY,      -- Array of field names to match
    top_n INTEGER,           -- Number of top matches to return per field
    min_threshold FLOAT      -- Minimum score threshold
)
```

### Returns

| Column | Description |
|--------|-------------|
| INPUT_FIELD | The input field name |
| SRC_FIELD | The matched source field from mappings |
| TARGET_FIELD | The standardized target field |
| COMBINED_SCORE | Overall confidence score (0-1) |
| EXACT_SCORE | Exact match score |
| SUBSTRING_SCORE | Substring match score |
| SEQUENCE_SCORE | Sequence similarity score |
| WORD_OVERLAP | Word overlap score |
| TFIDF_SCORE | TF-IDF cosine similarity |
| MATCH_RANK | Rank among top matches |

## Requirements

- Snowflake account
- Warehouse for running queries
- Privileges to create databases, schemas, tables, stored procedures, and Streamlit apps
- Snow CLI for deployment

## Troubleshooting

### Auto-mappings not showing
- Check the Debug panel in Step 2 to see the SQL executed and raw results
- Verify the stored procedure exists: `SHOW PROCEDURES LIKE 'field_matcher_advanced' IN SCHEMA CLAIMSIQ.PROCESSOR`
- Check that mappings exist: `SELECT * FROM CLAIMSIQ.PROCESSOR.MAPPINGS_LIST`

### Low confidence scores
- Add more source field variations to the mappings table
- Adjust the threshold slider to be more permissive
- Check that source field names are similar to your input data

### Deployment errors
- Ensure Snow CLI is configured with a valid connection
- Verify warehouse exists and you have USAGE privilege
- Check that you have CREATE privileges on the database/schema
