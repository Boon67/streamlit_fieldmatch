-- Corrected Field Matcher Procedure - Matches SRC fields, Returns TARGET fields
CREATE OR REPLACE PROCEDURE field_matcher_advanced(
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
$$
import re
import numpy as np
from difflib import SequenceMatcher
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, IntegerType

def preprocess_text(text):
    """Clean and normalize text for better matching"""
    if not text:
        return ""
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    return ' '.join(text.split())

def calculate_similarity_scores(input_text, src_fields, src_to_target_map):
    """Calculate various similarity scores for the input text against all source fields"""
    input_clean = preprocess_text(input_text)
    scores = []
    
    for src_field in src_fields:
        src_clean = preprocess_text(src_field)
        
        # 1. Exact match score
        exact_score = 1.0 if input_clean == src_clean else 0.0
        
        # 2. Substring match score
        substring_score = 1.0 if (input_clean in src_clean or src_clean in input_clean) else 0.0
        
        # 3. Sequence similarity (difflib)
        sequence_score = SequenceMatcher(None, input_clean, src_clean).ratio()
        
        # 4. Word overlap score
        input_words = set(input_clean.split()) if input_clean else set()
        src_words = set(src_clean.split()) if src_clean else set()
        
        if len(input_words.union(src_words)) > 0:
            word_overlap = len(input_words.intersection(src_words)) / len(input_words.union(src_words))
        else:
            word_overlap = 0.0
        
        # Initial combined score (before TF-IDF)
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
            'tfidf_score': 0.0  # Will be updated later
        })
    
    return scores

def add_tfidf_scores(input_text, scores, src_fields):
    """Add TF-IDF cosine similarity to the scores"""
    try:
        # Initialize TF-IDF vectorizer
        tfidf = TfidfVectorizer(
            lowercase=True,
            ngram_range=(1, 3),
            max_features=1000
        )
        
        # Prepare text data - preprocess source fields
        preprocessed_src_fields = [preprocess_text(field) for field in src_fields]
        preprocessed_input = preprocess_text(input_text)
        
        # Fit vectorizer on source fields (not target fields)
        tfidf_matrix = tfidf.fit_transform(preprocessed_src_fields)
        
        # Transform input text
        input_vector = tfidf.transform([preprocessed_input])
        
        # Calculate cosine similarity
        cosine_scores = cosine_similarity(input_vector, tfidf_matrix)[0]
        
        # Update scores with TF-IDF
        for i, score_dict in enumerate(scores):
            score_dict['tfidf_score'] = float(cosine_scores[i])
            # Update combined score: 70% basic similarity + 30% semantic similarity
            score_dict['combined_score'] = (
                score_dict['combined_score'] * 0.7 + 
                cosine_scores[i] * 0.3
            )
            
    except Exception as e:
        # If TF-IDF fails, use original scores
        for score_dict in scores:
            score_dict['tfidf_score'] = 0.0
    
    return scores

def run(session, input_fields, top_n, min_threshold):
    # Get both SRC and TARGET fields from mappings table
    mappings_query = """
    SELECT SRC, TARGET FROM mappings_list ORDER BY SRC
    """
    
    # Execute the query and get results
    mappings_result = session.sql(mappings_query).collect()
    
    # Create lists and mapping dictionary
    src_fields = [row[0] for row in mappings_result]
    src_to_target_map = {row[0]: row[1] for row in mappings_result}
    
    results = []
    
    # Process each input field
    for input_field in input_fields:
        # Calculate similarity scores against SOURCE fields
        field_scores = calculate_similarity_scores(input_field, src_fields, src_to_target_map)
        
        # Add TF-IDF scores (calculated against SOURCE fields)
        field_scores = add_tfidf_scores(input_field, field_scores, src_fields)
        
        # Sort by combined score
        field_scores = sorted(field_scores, key=lambda x: x['combined_score'], reverse=True)
        
        # Filter by minimum threshold and get top N
        filtered_scores = [s for s in field_scores if s['combined_score'] >= min_threshold][:top_n]
        
        # Add to results with ranking
        for rank, score_dict in enumerate(filtered_scores, 1):
            results.append([
                input_field,                                    # input_field
                score_dict['src_field'],                       # src_field (what was matched)
                score_dict['target_field'],                    # target_field (what gets returned)
                round(score_dict['combined_score'], 4),        # combined_score
                round(score_dict['exact_score'], 4),           # exact_score
                round(score_dict['substring_score'], 4),       # substring_score
                round(score_dict['sequence_score'], 4),        # sequence_score
                round(score_dict['word_overlap'], 4),          # word_overlap
                round(score_dict['tfidf_score'], 4),           # tfidf_score
                rank                                           # match_rank
            ])
    
    # Define schema for the DataFrame
    schema = StructType([
        StructField("input_field", StringType()),
        StructField("src_field", StringType()),
        StructField("target_field", StringType()),
        StructField("combined_score", FloatType()),
        StructField("exact_score", FloatType()),
        StructField("substring_score", FloatType()),
        StructField("sequence_score", FloatType()),
        StructField("word_overlap", FloatType()),
        StructField("tfidf_score", FloatType()),
        StructField("match_rank", IntegerType())
    ])
    
    # Create and return Snowpark DataFrame
    return session.create_dataframe(results, schema)
$$;