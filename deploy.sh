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
    TARGET VARCHAR
);
" || true
echo "✓ Mappings table ready"
echo ""

# Step 3: Load initial data using MERGE
echo "📊 Step 3: Loading field mappings data..."
snow sql -q "
MERGE INTO ${DATABASE}.${SCHEMA}.MAPPINGS_LIST AS target
USING (
    SELECT * FROM VALUES
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
    AS source (src, target)
) AS source
ON target.src = source.src AND target.target = source.target
WHEN NOT MATCHED THEN
    INSERT (src, target) VALUES (source.src, source.target);
" || true
echo "✓ Field mappings loaded"
echo ""

# Step 4: Deploy the stored procedure
echo "🔧 Step 4: Deploying field matcher stored procedure..."
snow sql -q "
CREATE OR REPLACE PROCEDURE ${DATABASE}.${SCHEMA}.field_matcher_advanced(
    input_fields ARRAY,
    top_n INTEGER DEFAULT 3,
    min_threshold FLOAT DEFAULT 0.1
)
RETURNS TABLE (
    input_field STRING,
    src_field STRING,
    target_field STRING,
    combined_score FLOAT,
    exact_score FLOAT,
    substring_score FLOAT,
    sequence_score FLOAT,
    word_overlap FLOAT,
    tfidf_score FLOAT,
    match_rank INTEGER
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy', 'scikit-learn', 'snowflake-snowpark-python')
HANDLER = 'run'
AS
\$\$
import re
import numpy as np
from difflib import SequenceMatcher
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, IntegerType

def preprocess_text(text):
    if not text:
        return ''
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    return ' '.join(text.split())

def calculate_similarity_scores(input_text, src_fields, src_to_target_map):
    input_clean = preprocess_text(input_text)
    scores = []
    
    for src_field in src_fields:
        src_clean = preprocess_text(src_field)
        exact_score = 1.0 if input_clean == src_clean else 0.0
        substring_score = 1.0 if (input_clean in src_clean or src_clean in input_clean) else 0.0
        sequence_score = SequenceMatcher(None, input_clean, src_clean).ratio()
        
        input_words = set(input_clean.split()) if input_clean else set()
        src_words = set(src_clean.split()) if src_clean else set()
        
        if len(input_words.union(src_words)) > 0:
            word_overlap = len(input_words.intersection(src_words)) / len(input_words.union(src_words))
        else:
            word_overlap = 0.0
        
        combined_score = (
            exact_score * 0.4 +
            substring_score * 0.2 +
            sequence_score * 0.2 +
            word_overlap * 0.2
        )
        
        scores.append({
            'src_field': src_field,
            'target_field': src_to_target_map.get(src_field, 'UNMAPPED'),
            'combined_score': combined_score,
            'exact_score': exact_score,
            'substring_score': substring_score,
            'sequence_score': sequence_score,
            'word_overlap': word_overlap,
            'tfidf_score': 0.0
        })
    
    return scores

def add_tfidf_scores(input_text, scores, src_fields):
    try:
        tfidf = TfidfVectorizer(lowercase=True, ngram_range=(1, 3), max_features=1000)
        preprocessed_src_fields = [preprocess_text(field) for field in src_fields]
        preprocessed_input = preprocess_text(input_text)
        tfidf_matrix = tfidf.fit_transform(preprocessed_src_fields)
        input_vector = tfidf.transform([preprocessed_input])
        cosine_scores = cosine_similarity(input_vector, tfidf_matrix)[0]
        
        for i, score_dict in enumerate(scores):
            score_dict['tfidf_score'] = float(cosine_scores[i])
            score_dict['combined_score'] = (
                score_dict['combined_score'] * 0.7 + 
                cosine_scores[i] * 0.3
            )
    except Exception as e:
        for score_dict in scores:
            score_dict['tfidf_score'] = 0.0
    
    return scores

def run(session, input_fields, top_n, min_threshold):
    mappings_query = 'SELECT SRC, TARGET FROM mappings_list ORDER BY SRC'
    mappings_result = session.sql(mappings_query).collect()
    
    src_fields = [row[0] for row in mappings_result]
    src_to_target_map = {row[0]: row[1] for row in mappings_result}
    
    results = []
    
    for input_field in input_fields:
        field_scores = calculate_similarity_scores(input_field, src_fields, src_to_target_map)
        field_scores = add_tfidf_scores(input_field, field_scores, src_fields)
        field_scores = sorted(field_scores, key=lambda x: x['combined_score'], reverse=True)
        filtered_scores = [s for s in field_scores if s['combined_score'] >= min_threshold][:top_n]
        
        for rank, score_dict in enumerate(filtered_scores, 1):
            results.append([
                input_field,
                score_dict['src_field'],
                score_dict['target_field'],
                round(score_dict['combined_score'], 4),
                round(score_dict['exact_score'], 4),
                round(score_dict['substring_score'], 4),
                round(score_dict['sequence_score'], 4),
                round(score_dict['word_overlap'], 4),
                round(score_dict['tfidf_score'], 4),
                rank
            ])
    
    schema = StructType([
        StructField('input_field', StringType()),
        StructField('src_field', StringType()),
        StructField('target_field', StringType()),
        StructField('combined_score', FloatType()),
        StructField('exact_score', FloatType()),
        StructField('substring_score', FloatType()),
        StructField('sequence_score', FloatType()),
        StructField('word_overlap', FloatType()),
        StructField('tfidf_score', FloatType()),
        StructField('match_rank', IntegerType())
    ])
    
    return session.create_dataframe(results, schema)
\$\$;
" || true
echo "✓ Stored procedure deployed"
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
