# Healthcare Field Mappings Manager
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Initialize Snowflake session
session = get_active_session()

st.title("ClaimsIQ 🏥")
st.write("Healthcare claims field mapping and data processing")

# Create tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📤 Upload & Map File", "📝 Edit Mappings", "🔍 Test Matches", "📊 View Tables", "🤖 LLM Settings"])

# Initialize session state for data management
if 'mappings_data' not in st.session_state:
    st.session_state.mappings_data = None
if 'changes_made' not in st.session_state:
    st.session_state.changes_made = False

# Default LLM prompt template
DEFAULT_LLM_PROMPT = """You are a healthcare data expert helping to map source columns to target schema fields.

SOURCE COLUMNS (from uploaded file):
{source_list}

TARGET FIELDS (standard schema):
{target_list}

TASK: For each source column, determine the best matching target field based on:
1. Semantic meaning (e.g., "DOB" matches "Date of Birth", "Claimant DOB")
2. Common healthcare abbreviations (e.g., "CPT", "ICD", "NPI", "Rx")
3. Field naming patterns (e.g., "amt" = "Amount", "dt" = "Date")

IMPORTANT RULES:
- Each target field can only be mapped ONCE. If multiple source columns could match the same target, choose the BEST match and set others to "NONE".
- Be conservative - only suggest matches you're confident about.
- Use "NONE" for columns that don't have a clear match.

RESPONSE FORMAT: Return ONLY a JSON object with source columns as keys and objects containing "target" (best match or "NONE" if no good match) and "confidence" (0.0-1.0). Example:
{{"column_name": {{"target": "Target Field Name", "confidence": 0.85}}}}"""

# Initialize LLM prompt in session state
if 'llm_prompt_template' not in st.session_state:
    st.session_state.llm_prompt_template = DEFAULT_LLM_PROMPT

def load_mappings_data():
    """Load current mappings data from Snowflake"""
    try:
        df = session.sql("SELECT SRC, TARGET FROM PROCESSOR.mappings_list ORDER BY TARGET, SRC").to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame(columns=['SRC', 'TARGET'])

def get_available_llm_models():
    """Get list of available Cortex LLM models from Snowflake"""
    try:
        # Query available models from Snowflake
        session.sql("SHOW MODELS IN SNOWFLAKE.MODELS").collect()
        models_df = session.sql("""
            SELECT "name" AS model_name
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            WHERE "model_type" = 'CORTEX_BASE'
            ORDER BY "name"
        """).to_pandas()
        
        if models_df is not None and len(models_df) > 0:
            return models_df['MODEL_NAME'].tolist()
        else:
            # Fallback to common models if query returns empty
            return ["llama3.1-70b", "mistral-large2", "mistral-7b"]
    except Exception as e:
        # Fallback to common models if query fails
        return ["llama3.1-70b", "mistral-large2", "mistral-7b"]

def get_unique_targets():
    """Get list of unique target field names"""
    try:
        targets = session.sql("SELECT DISTINCT TARGET FROM PROCESSOR.mappings_list ORDER BY TARGET").to_pandas()
        return targets['TARGET'].tolist()
    except Exception as e:
        st.error(f"Error loading target fields: {str(e)}")
        return []

def save_changes(updated_df, original_df):
    """Save changes to Snowflake table"""
    try:
        # Find records to delete (in original but not in updated)
        original_pairs = set(zip(original_df['SRC'], original_df['TARGET']))
        updated_pairs = set(zip(updated_df['SRC'], updated_df['TARGET']))
        
        to_delete = original_pairs - updated_pairs
        to_insert = updated_pairs - original_pairs
        
        # Delete removed records
        for src, target in to_delete:
            # Use parameterized queries to prevent SQL injection
            delete_sql = "DELETE FROM PROCESSOR.mappings_list WHERE SRC = ? AND TARGET = ?"
            session.sql(delete_sql, params=[src, target]).collect()
        
        # Insert new records
        for src, target in to_insert:
            insert_sql = "INSERT INTO PROCESSOR.mappings_list (SRC, TARGET) VALUES (?, ?)"
            session.sql(insert_sql, params=[src, target]).collect()
        
        return True, len(to_delete), len(to_insert)
    except Exception as e:
        return False, 0, 0, str(e)

def test_field_matches(field_list, top_n=3, min_threshold=0.1):
    """Test field matches using the stored procedure"""
    try:
        # Escape single quotes in field names
        escaped_fields = [field.replace("'", "''") for field in field_list]
        formatted_fields = "['" + "', '".join(escaped_fields) + "']"
        
        # Call the stored procedure with full schema path
        sql_query = f"""
        CALL CLAIMSIQ.PROCESSOR.field_matcher_advanced(
            {formatted_fields}, 
            {top_n}, 
            {min_threshold}
        )
        """
        
        result = session.sql(sql_query).to_pandas()
        return result, None
    except Exception as e:
        return None, str(e)

def normalize_column_names(df):
    """Normalize column names from stored procedure results"""
    if df is None or len(df) == 0:
        return df
    
    # Expected column order from the stored procedure:
    # input_field, src_field, target_field, combined_score, exact_score, 
    # substring_score, sequence_score, word_overlap, tfidf_score, match_rank
    expected_columns = [
        'INPUT_FIELD', 'SRC_FIELD', 'TARGET_FIELD', 'COMBINED_SCORE', 
        'EXACT_SCORE', 'SUBSTRING_SCORE', 'SEQUENCE_SCORE', 'WORD_OVERLAP', 
        'TFIDF_SCORE', 'MATCH_RANK'
    ]
    
    # First, strip quotes from column names (Snowflake sometimes returns '"COLUMN_NAME"')
    new_columns = []
    for col in df.columns:
        col_str = str(col).strip('"\'').upper()
        new_columns.append(col_str)
    df.columns = new_columns
    
    # Check if columns are numeric indices (0, 1, 2...) - this happens with some stored procedure results
    if len(df.columns) > 0 and (df.columns[0] == '0' or str(df.columns[0]).isdigit()):
        # Map by position
        if len(df.columns) == len(expected_columns):
            df.columns = expected_columns
            return df
        else:
            # Try to map as many as we can
            pos_columns = []
            for i, col in enumerate(df.columns):
                if i < len(expected_columns):
                    pos_columns.append(expected_columns[i])
                else:
                    pos_columns.append(f'COL_{i}')
            df.columns = pos_columns
            return df
    
    # Map column names to expected format (already uppercase from above)
    column_mapping = {}
    for col in df.columns:
        if col == 'INPUT_FIELD':
            column_mapping[col] = 'INPUT_FIELD'
        elif col == 'SRC_FIELD':
            column_mapping[col] = 'SRC_FIELD'
        elif col == 'TARGET_FIELD':
            column_mapping[col] = 'TARGET_FIELD'
        elif col == 'COMBINED_SCORE':
            column_mapping[col] = 'COMBINED_SCORE'
        elif col == 'EXACT_SCORE':
            column_mapping[col] = 'EXACT_SCORE'
        elif col == 'SUBSTRING_SCORE':
            column_mapping[col] = 'SUBSTRING_SCORE'
        elif col == 'SEQUENCE_SCORE':
            column_mapping[col] = 'SEQUENCE_SCORE'
        elif col == 'WORD_OVERLAP':
            column_mapping[col] = 'WORD_OVERLAP'
        elif col == 'TFIDF_SCORE':
            column_mapping[col] = 'TFIDF_SCORE'
        elif col == 'MATCH_RANK':
            column_mapping[col] = 'MATCH_RANK'
    
    # Rename columns to standardized uppercase names
    df_renamed = df.rename(columns=column_mapping)
    
    return df_renamed

# TAB 2: Edit Mappings
with tab2:
    st.markdown("""
    **Manage field mappings** used by the auto-mapping algorithm. 
    Add new source field variations to improve future matching accuracy.
    """)
    
    # Action bar with buttons
    st.markdown("### 🔧 Actions")
    action_col1, action_col2, action_col3 = st.columns([1, 1, 2])

    with action_col1:
        if st.button("🔄 Refresh Data", type="primary", use_container_width=True):
            st.session_state.mappings_data = load_mappings_data()
            st.session_state.changes_made = False
            st.rerun()

    with action_col2:
        save_disabled = not st.session_state.changes_made
        if st.button("💾 Save Changes", disabled=save_disabled, use_container_width=True):
            if st.session_state.mappings_data is not None and st.session_state.changes_made:
                original_data = load_mappings_data()
                result = save_changes(st.session_state.mappings_data, original_data)
                if result[0]:
                    st.success(f"✅ Changes saved! Deleted: {result[1]}, Added: {result[2]} records")
                    st.session_state.changes_made = False
                    st.session_state.mappings_data = load_mappings_data()
                    st.rerun()
                else:
                    st.error(f"❌ Error saving changes: {result[3] if len(result) > 3 else 'Unknown error'}")
    
    with action_col3:
        if st.session_state.changes_made:
            st.warning("⚠️ You have unsaved changes!")

    # Load initial data if not loaded
    if st.session_state.mappings_data is None:
        st.session_state.mappings_data = load_mappings_data()

    st.write("---")

    # Display current data
    if st.session_state.mappings_data is not None and len(st.session_state.mappings_data) > 0:
        st.markdown("### 📋 Current Mappings")
        
        # Group by target field for better organization
        targets = st.session_state.mappings_data['TARGET'].unique()
        
        # Filter options
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_targets = st.multiselect(
                "Filter by Target Fields (leave empty to show all)",
                options=sorted(targets),
                default=[]
            )
        
        with col2:
            search_term = st.text_input("🔍 Search Source Fields", placeholder="Enter search term...")
        
        # Filter data
        filtered_data = st.session_state.mappings_data.copy()
        if selected_targets:
            filtered_data = filtered_data[filtered_data['TARGET'].isin(selected_targets)]
        if search_term:
            filtered_data = filtered_data[
                filtered_data['SRC'].str.contains(search_term, case=False, na=False)
            ]
        
        # Display data with editing capabilities
        st.write(f"Showing {len(filtered_data)} of {len(st.session_state.mappings_data)} total mappings")
        
        # Create editable dataframe
        edited_data = st.data_editor(
            filtered_data,
            use_container_width=True,
            num_rows="dynamic",  # Allow adding/deleting rows
            column_config={
                "SRC": st.column_config.TextColumn(
                    "Source Field Name",
                    help="The original field name from source systems",
                    required=True
                ),
                "TARGET": st.column_config.SelectboxColumn(
                    "Target Field Name",
                    help="Standardized healthcare field name",
                    options=get_unique_targets(),
                    required=True
                )
            },
            key="data_editor"
        )
        
        # Check if changes were made
        if not edited_data.equals(filtered_data):
            st.session_state.changes_made = True
            # Update the main data with changes (merge with unfiltered data)
            if search_term or selected_targets:
                # If filtering is active, we need to merge changes back to main dataset
                main_data = st.session_state.mappings_data.copy()
                # Remove old filtered records
                if selected_targets:
                    main_data = main_data[~main_data['TARGET'].isin(selected_targets)]
                if search_term:
                    main_data = main_data[~main_data['SRC'].str.contains(search_term, case=False, na=False)]
                # Add back edited records
                st.session_state.mappings_data = pd.concat([main_data, edited_data], ignore_index=True)
            else:
                st.session_state.mappings_data = edited_data
        
        # Display summary statistics
        st.subheader("📊 Summary Statistics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Mappings", len(st.session_state.mappings_data))
        
        with col2:
            st.metric("Unique Target Fields", len(st.session_state.mappings_data['TARGET'].unique()))
        
        with col3:
            st.metric("Unique Source Fields", len(st.session_state.mappings_data['SRC'].unique()))

    else:
        st.info("No data loaded. Click 'Load Data' to begin editing.")

    # Add new mapping section
    st.subheader("➕ Add New Mapping")

    with st.form("add_mapping_form"):
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            new_src = st.text_input("New Source Field Name", placeholder="e.g., Patient_Name")
        
        with col2:
            available_targets = get_unique_targets()
            if available_targets:
                new_target = st.selectbox("Target Field Name", options=available_targets)
            else:
                new_target = None
                st.warning("No target fields available. Please load data first.")
        
        with col3:
            st.write("")  # Spacing
            add_button = st.form_submit_button("Add Mapping", type="primary")
        
        if add_button and new_src and new_target:
            # Check if mapping already exists
            if st.session_state.mappings_data is not None:
                existing = st.session_state.mappings_data[
                    (st.session_state.mappings_data['SRC'] == new_src) & 
                    (st.session_state.mappings_data['TARGET'] == new_target)
                ]
                
                if len(existing) > 0:
                    st.error("❌ This mapping already exists!")
                else:
                    # Add new mapping
                    new_row = pd.DataFrame({'SRC': [new_src], 'TARGET': [new_target]})
                    st.session_state.mappings_data = pd.concat([st.session_state.mappings_data, new_row], ignore_index=True)
                    st.session_state.changes_made = True
                    st.success(f"✅ Added mapping: {new_src} → {new_target}")
                    st.rerun()
            else:
                st.error("❌ Please load data first before adding mappings.")

    # Bulk operations
    st.subheader("🔧 Bulk Operations")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Delete by Source Field**")
        # Get unique source fields, excluding those where SRC == TARGET (base mappings)
        if st.session_state.mappings_data is not None:
            # Filter out rows where SRC equals TARGET (these are base mappings that shouldn't be deleted)
            deletable_sources = st.session_state.mappings_data[
                st.session_state.mappings_data['SRC'] != st.session_state.mappings_data['TARGET']
            ]['SRC'].unique().tolist()
            deletable_sources.sort()
        else:
            deletable_sources = []
        
        sources_to_delete = st.multiselect(
            "Select source fields to delete (base mappings where SRC=TARGET are protected)",
            options=deletable_sources
        )
        
        if st.button("🗑️ Delete Selected Sources", type="secondary"):
            if sources_to_delete and st.session_state.mappings_data is not None:
                original_count = len(st.session_state.mappings_data)
                st.session_state.mappings_data = st.session_state.mappings_data[
                    ~st.session_state.mappings_data['SRC'].isin(sources_to_delete)
                ]
                deleted_count = original_count - len(st.session_state.mappings_data)
                st.session_state.changes_made = True
                st.success(f"✅ Deleted {deleted_count} mappings")
                st.rerun()

    with col2:
        st.write("**Export Data**")
        if st.session_state.mappings_data is not None:
            csv_data = st.session_state.mappings_data.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv_data,
                file_name="healthcare_field_mappings.csv",
                mime="text/csv"
            )

    # Note: Unsaved changes warning is shown in the action bar at the top

# TAB 3: Test Matches
with tab3:
    st.markdown("""
    **Test the field matching algorithm** to see how it matches your field names to target fields.
    This helps you understand the matching confidence before processing files.
    """)
    
    # Configuration section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.write("**Field Names to Test**")
        # Text area for bulk input
        field_input = st.text_area(
            "Enter field names (one per line):",
            placeholder="Patient Name\nBirth Date\nPolicy Start Date\ntotal_amt\nDiagnosis Code\nProvider NPI",
            height=150,
            help="Enter each field name on a separate line"
        )
        
        if 'test_fields' not in st.session_state:
            st.session_state.test_fields = []
        
        col_a, col_b = st.columns([3, 1])
        with col_a:
            new_field = st.text_input("Add field name:", placeholder="e.g., Patient_DOB", key="new_field_input")
        with col_b:
            if st.button("➕ Add") and new_field:
                if new_field not in st.session_state.test_fields:
                    st.session_state.test_fields.append(new_field)
                    st.rerun()
        
        # Display current test fields
        if st.session_state.test_fields:
            st.write("**Current test fields:**")
            for i, field in enumerate(st.session_state.test_fields):
                col_x, col_y = st.columns([4, 1])
                with col_x:
                    st.write(f"• {field}")
                with col_y:
                    if st.button("🗑️", key=f"remove_{i}", help=f"Remove {field}"):
                        st.session_state.test_fields.remove(field)
                        st.rerun()
    
    with col2:
        st.write("**Algorithm Parameters**")
        
        top_n = st.slider(
            "Number of matches per field",
            min_value=1,
            max_value=10,
            value=3,
            help="How many top matches to return for each input field"
        )
        
        min_threshold = st.slider(
            "Minimum confidence threshold",
            min_value=0.0,
            max_value=1.0,
            value=0.1,
            step=0.05,
            help="Minimum similarity score to include in results"
        )
        
        st.write("**Quick Test Examples**")
        example_sets = {
            "Healthcare Common": ["Patient Name", "DOB", "Policy Number", "Claim Amount", "Provider ID"],
            "Variations & Typos": ["Patien_Name", "Birth_Dt", "Plcy_Start", "Tot_Amt", "Diag_Code"],
            "Abbreviations": ["Pt_Name", "DOB", "Pol_Eff", "Billed_Amt", "NPI"]
        }
        
        for name, fields in example_sets.items():
            if st.button(f"Load: {name}", key=f"load_{name}"):
                st.session_state.test_fields = fields
                st.rerun()
        
        if st.button("🗑️ Clear All", type="secondary"):
            st.session_state.test_fields = []
            st.rerun()
    
    # Prepare field list
    test_field_list = []
    
    # Add fields from text area
    if field_input.strip():
        text_fields = [line.strip() for line in field_input.split('\n') if line.strip()]
        test_field_list.extend(text_fields)
    
    # Add fields from manual input
    test_field_list.extend(st.session_state.test_fields)
    
    # Remove duplicates while preserving order
    test_field_list = list(dict.fromkeys(test_field_list))
    
    # Test execution
    st.write("---")
    
    if test_field_list:
        st.markdown(f"### 🧪 Ready to Test ({len(test_field_list)} fields)")
        
        # Show fields in a compact format
        with st.expander("View fields to test", expanded=False):
            for field in test_field_list:
                st.write(f"• {field}")
        
        # Action row
        action_col1, action_col2 = st.columns([2, 1])
        
        with action_col1:
            test_button = st.button(
                "🚀 Run Field Matching Test", 
                type="primary",
                use_container_width=True
            )
        
        with action_col2:
            # Show SQL that would be executed
            escaped_fields = [field.replace("'", "''") for field in test_field_list]
            formatted_fields = "['" + "', '".join(escaped_fields) + "']"
            sql_preview = f"""CALL CLAIMSIQ.PROCESSOR.field_matcher_advanced(
    {formatted_fields},
    {top_n},
    {min_threshold}
);"""
            
            with st.expander("📄 View SQL"):
                st.code(sql_preview, language="sql")
    else:
        st.info("👆 **Enter field names above** to test the matching algorithm")
        test_button = False
    
    # Execute test
    if test_button and test_field_list:
        with st.spinner("Running field matching algorithm..."):
            result_df, error = test_field_matches(test_field_list, top_n, min_threshold)
        
        if error:
            st.error(f"❌ Error executing test: {error}")
        elif result_df is not None and len(result_df) > 0:
            # Normalize column names
            result_df = normalize_column_names(result_df)
            
            # Verify we have the essential columns
            required_cols = ['INPUT_FIELD', 'TARGET_FIELD', 'COMBINED_SCORE']
            missing_cols = [col for col in required_cols if col not in result_df.columns]
            
            if missing_cols:
                st.error(f"❌ Missing required columns: {missing_cols}")
                st.write("**Available columns:**", result_df.columns.tolist())
                st.write("**Raw data sample:**")
                st.dataframe(result_df.head())
            else:
                st.success(f"✅ Test completed! Found {len(result_df)} matches")
                
                # Display results
                st.subheader("📊 Matching Results")
                
                # Results summary
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Matches", len(result_df))
                
                with col2:
                    high_conf = len(result_df[result_df['COMBINED_SCORE'] >= 0.8])
                    st.metric("High Confidence (≥0.8)", high_conf)
                
                with col3:
                    med_conf = len(result_df[(result_df['COMBINED_SCORE'] >= 0.5) & (result_df['COMBINED_SCORE'] < 0.8)])
                    st.metric("Medium Confidence (0.5-0.8)", med_conf)
                
                with col4:
                    low_conf = len(result_df[result_df['COMBINED_SCORE'] < 0.5])
                    st.metric("Low Confidence (<0.5)", low_conf)
                
                # Format results for better display
                display_df = result_df.copy()
                
                # Add confidence level column
                def get_confidence_level(score):
                    if score >= 0.8:
                        return "🟢 High"
                    elif score >= 0.5:
                        return "🟡 Medium"
                    else:
                        return "🔴 Low"
                
                display_df['CONFIDENCE_LEVEL'] = display_df['COMBINED_SCORE'].apply(get_confidence_level)
                
                # Round scores for display (only if columns exist)
                score_columns = ['COMBINED_SCORE', 'EXACT_SCORE', 'SUBSTRING_SCORE', 'SEQUENCE_SCORE', 'WORD_OVERLAP', 'TFIDF_SCORE']
                existing_score_cols = [col for col in score_columns if col in display_df.columns]
                for col in existing_score_cols:
                    display_df[col] = display_df[col].round(3)
                
                # Create column config dynamically based on available columns
                column_config = {
                    "INPUT_FIELD": "Input Field",
                    "SRC_FIELD": "Matched Source",
                    "TARGET_FIELD": "Target Field",
                    "COMBINED_SCORE": st.column_config.NumberColumn("Combined Score", format="%.3f"),
                    "CONFIDENCE_LEVEL": "Confidence"
                }
                
                # Add optional columns if they exist
                optional_configs = {
                    "MATCH_RANK": "Rank",
                    "EXACT_SCORE": st.column_config.NumberColumn("Exact", format="%.3f"),
                    "SUBSTRING_SCORE": st.column_config.NumberColumn("Substring", format="%.3f"),
                    "SEQUENCE_SCORE": st.column_config.NumberColumn("Sequence", format="%.3f"),
                    "WORD_OVERLAP": st.column_config.NumberColumn("Word Overlap", format="%.3f"),
                    "TFIDF_SCORE": st.column_config.NumberColumn("TF-IDF", format="%.3f")
                }
                
                for col, config in optional_configs.items():
                    if col in display_df.columns:
                        column_config[col] = config
                
                # Display results table
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    column_config=column_config
                )
                
                # Download results
                csv_results = result_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results as CSV",
                    data=csv_results,
                    file_name=f"field_matching_results_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
                # Analysis insights
                st.subheader("💡 Analysis Insights")
                
                # Group by input field to show best matches
                best_matches = result_df.loc[result_df.groupby('INPUT_FIELD')['COMBINED_SCORE'].idxmax()]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Best Matches Summary:**")
                    for _, row in best_matches.iterrows():
                        confidence = "🟢" if row['COMBINED_SCORE'] >= 0.8 else "🟡" if row['COMBINED_SCORE'] >= 0.5 else "🔴"
                        st.write(f"{confidence} `{row['INPUT_FIELD']}` → `{row['TARGET_FIELD']}` ({row['COMBINED_SCORE']:.3f})")
                
                with col2:
                    st.write("**Recommendations:**")
                    low_matches = best_matches[best_matches['COMBINED_SCORE'] < 0.5]
                    if len(low_matches) > 0:
                        st.write("🔴 **Fields needing attention:**")
                        for _, row in low_matches.iterrows():
                            st.write(f"• `{row['INPUT_FIELD']}` (score: {row['COMBINED_SCORE']:.3f})")
                        st.write("Consider adding these as new source mappings or checking for typos.")
                    else:
                        st.write("✅ All fields have good matches!")
                        st.write("The algorithm performed well on your test data.")
        
        else:
            st.warning("⚠️ No matches found. Try lowering the minimum threshold or check your field names.")
    
    elif not test_field_list:
        st.info("👆 Enter some field names above to test the matching algorithm")

# TAB 1: Upload & Map File
with tab1:
    st.subheader("📤 Upload & Map File to Target Schema")
    st.write("Upload a data file, map its columns to the target schema, and create a new table")
    
    # Initialize session state for file mapping workflow
    if 'upload_step' not in st.session_state:
        st.session_state.upload_step = 1  # 1=upload, 2=map, 3=review, 4=complete
    if 'uploaded_file_data' not in st.session_state:
        st.session_state.uploaded_file_data = None
    if 'uploaded_file_name' not in st.session_state:
        st.session_state.uploaded_file_name = None
    if 'column_mappings' not in st.session_state:
        st.session_state.column_mappings = {}
    if 'auto_mappings' not in st.session_state:
        st.session_state.auto_mappings = {}
    
    def reset_upload_workflow():
        """Reset the upload workflow to start fresh"""
        st.session_state.upload_step = 1
        st.session_state.uploaded_file_data = None
        st.session_state.uploaded_file_name = None
        st.session_state.column_mappings = {}
        st.session_state.auto_mappings = {}
        st.session_state.auto_mappings_sql = None
        st.session_state.auto_mappings_raw_result = None
        st.session_state.matching_method_used = None
    
    def get_auto_mappings(source_columns, show_debug=False):
        """Get automatic mapping suggestions for source columns using field_matcher_advanced"""
        try:
            if not source_columns:
                return {}, None, None
            
            # Escape single quotes in field names
            escaped_fields = [field.replace("'", "''") for field in source_columns]
            formatted_fields = "['" + "', '".join(escaped_fields) + "']"
            
            # Call the stored procedure to get best matches (top 1 match per field)
            # Use fully qualified name: DATABASE.SCHEMA.PROCEDURE
            sql_query = f"""CALL CLAIMSIQ.PROCESSOR.field_matcher_advanced(
    {formatted_fields}, 
    1, 
    0.1
)"""
            
            result = session.sql(sql_query).to_pandas()
            raw_result = result.copy() if result is not None else None
            result = normalize_column_names(result)
            
            # Create mapping dictionary - use TARGET_FIELD as the suggested mapping
            mappings = {}
            if result is not None and len(result) > 0:
                # Check for required columns
                if 'INPUT_FIELD' in result.columns and 'TARGET_FIELD' in result.columns:
                    for _, row in result.iterrows():
                        input_field = str(row['INPUT_FIELD'])
                        target_field = str(row['TARGET_FIELD'])
                        score = row.get('COMBINED_SCORE', 0)
                        src_field = str(row.get('SRC_FIELD', ''))
                        
                        # Store the mapping with target field and confidence score
                        # Use exact input field name as key
                        mappings[input_field] = {
                            'target': target_field,
                            'score': float(score) if score else 0.0,
                            'src_match': src_field
                        }
                        
                        # Also store with lowercase key for case-insensitive lookup
                        mappings[input_field.lower()] = {
                            'target': target_field,
                            'score': float(score) if score else 0.0,
                            'src_match': src_field
                        }
            
            return mappings, sql_query, raw_result
        except Exception as e:
            error_msg = str(e)
            st.error(f"❌ Could not get auto-mappings from field_matcher_advanced: {error_msg}")
            return {}, None, None
    
    def get_llm_recommendations(source_columns, target_fields, model_name="llama3.1-70b"):
        """Use Snowflake Cortex LLM to get intelligent field mapping recommendations"""
        try:
            if not source_columns or not target_fields:
                return {}, None, None
            
            # Build the prompt for the LLM using the template from session state
            source_list = "\n".join([f"- {col}" for col in source_columns])
            target_list = "\n".join([f"- {field}" for field in target_fields])
            
            # Get the prompt template from session state
            prompt_template = st.session_state.get('llm_prompt_template', DEFAULT_LLM_PROMPT)
            
            # Format the prompt with source and target lists
            prompt = prompt_template.format(source_list=source_list, target_list=target_list)

            # Escape the prompt for SQL
            escaped_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
            
            # Call Snowflake Cortex COMPLETE function with selected model
            sql_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model_name}',
                '{escaped_prompt}'
            ) as response
            """
            
            result = session.sql(sql_query).collect()
            
            if not result or len(result) == 0:
                return {}, sql_query, "No response from LLM"
            
            response_text = result[0][0]
            
            # Parse the JSON response
            import json
            import re
            
            # Try to extract JSON from the response (LLM might include extra text)
            json_match = re.search(r'\{[\s\S]*\}', response_text)
            if json_match:
                try:
                    llm_mappings = json.loads(json_match.group())
                    
                    # First pass: collect all mappings with their confidence scores
                    raw_mappings = {}
                    for src_col, match_info in llm_mappings.items():
                        if isinstance(match_info, dict):
                            target = match_info.get('target', 'NONE')
                            confidence = float(match_info.get('confidence', 0))
                        else:
                            target = str(match_info)
                            confidence = 0.5
                        
                        if target and target != 'NONE' and target in target_fields:
                            raw_mappings[src_col] = {
                                'target': target,
                                'score': confidence,
                                'src_match': 'LLM'
                            }
                    
                    # Second pass: resolve duplicates - keep only the best match for each target
                    # Group by target field
                    target_to_sources = {}
                    for src_col, mapping_info in raw_mappings.items():
                        target = mapping_info['target']
                        if target not in target_to_sources:
                            target_to_sources[target] = []
                        target_to_sources[target].append((src_col, mapping_info))
                    
                    # For each target, keep only the source with highest confidence
                    mappings = {}
                    for target, source_list in target_to_sources.items():
                        if len(source_list) == 1:
                            # No duplicates, keep as-is
                            src_col, mapping_info = source_list[0]
                            mappings[src_col] = mapping_info
                            mappings[src_col.lower()] = mapping_info
                        else:
                            # Duplicates found - select the one with highest confidence
                            best_match = max(source_list, key=lambda x: x[1]['score'])
                            src_col, mapping_info = best_match
                            mappings[src_col] = mapping_info
                            mappings[src_col.lower()] = mapping_info
                            # Note: other sources that mapped to this target are now unmapped
                    
                    return mappings, sql_query, response_text
                except json.JSONDecodeError:
                    return {}, sql_query, f"Failed to parse JSON: {response_text}"
            else:
                return {}, sql_query, f"No JSON found in response: {response_text}"
                
        except Exception as e:
            error_msg = str(e)
            return {}, None, f"LLM Error: {error_msg}"
    
    def sanitize_table_name(filename):
        """Convert filename to valid Snowflake table name"""
        import re
        # Remove file extension
        name = filename.rsplit('.', 1)[0]
        # Replace invalid characters with underscores
        name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Ensure it starts with a letter
        if name and not name[0].isalpha():
            name = 'T_' + name
        # Uppercase and limit length
        name = name.upper()[:128]
        return name
    
    def create_mapped_table(df, column_mapping, table_name, schema="PROCESSOR"):
        """Create a new table with ALL target columns and insert mapped data"""
        try:
            # Get all target fields from the mappings table
            all_target_fields = get_unique_targets()
            
            if not all_target_fields:
                return False, "No target fields found in mappings table."
            
            # Build source to target mapping for columns that are mapped
            source_to_target = {}
            for src_col, target_col in column_mapping.items():
                if target_col and target_col != "(Ignored)":
                    source_to_target[src_col] = target_col
            
            if not source_to_target:
                return False, "No columns mapped. Please map at least one column."
            
            # Create table with ALL target column names (all VARCHAR for simplicity)
            column_defs = ", ".join([f'"{col}" VARCHAR' for col in all_target_fields])
            create_sql = f'CREATE OR REPLACE TABLE {schema}."{table_name}" ({column_defs})'
            session.sql(create_sql).collect()
            
            # Prepare data for insertion - create dataframe with ALL target columns
            df_mapped = pd.DataFrame(index=range(len(df)))
            
            # Add all target columns, populated with mapped data or NULL
            for target_col in all_target_fields:
                # Find if any source column maps to this target
                source_col = None
                for src, tgt in source_to_target.items():
                    if tgt == target_col:
                        source_col = src
                        break
                
                if source_col and source_col in df.columns:
                    # Map the source data to this target column
                    df_mapped[target_col] = df[source_col].astype(str).replace('nan', None).replace('None', None)
                else:
                    # No mapping - column will be NULL
                    df_mapped[target_col] = None
            
            # Insert data using Snowpark
            snowpark_df = session.create_dataframe(df_mapped)
            snowpark_df.write.mode("append").save_as_table(f'{schema}."{table_name}"')
            
            mapped_count = len(source_to_target)
            total_cols = len(all_target_fields)
            return True, f"Successfully created table {schema}.{table_name} with {len(df_mapped)} rows ({mapped_count} mapped columns, {total_cols - mapped_count} NULL columns)"
        except Exception as e:
            return False, str(e)
    
    # Progress indicator
    steps = ["1️⃣ Upload File", "2️⃣ Map Columns", "3️⃣ Review & Approve", "4️⃣ Complete"]
    current_step = st.session_state.upload_step
    
    # Show progress bar
    progress_cols = st.columns(4)
    for i, (col, step) in enumerate(zip(progress_cols, steps)):
        with col:
            if i + 1 < current_step:
                st.success(step)
            elif i + 1 == current_step:
                st.info(step)
            else:
                st.write(step)
    
    st.write("---")
    
    # Reset button
    if current_step > 1:
        if st.button("🔄 Start Over", type="secondary"):
            reset_upload_workflow()
            st.rerun()
    
    # STEP 1: Upload File
    if current_step == 1:
        st.subheader("Step 1: Upload Your Data File")
        
        # Instructions box
        st.info("📁 **Upload a file** containing your claims data. The first row should contain column headers.")
        
        uploaded_file = st.file_uploader(
            "Choose a CSV, TXT, or Excel file",
            type=['csv', 'txt', 'xls', 'xlsx'],
            help="Supported formats: CSV, TXT (tab/pipe/comma delimited), XLS, XLSX"
        )
        
        if uploaded_file is not None:
            try:
                file_extension = uploaded_file.name.lower().split('.')[-1]
                
                # Handle Excel files
                if file_extension in ['xls', 'xlsx']:
                    # Read Excel file
                    df = pd.read_excel(uploaded_file, engine='openpyxl')
                    
                    # If multiple sheets, let user select (for now, use first sheet)
                    # Future enhancement: add sheet selector
                else:
                    # Read text-based files (CSV, TXT)
                    content = uploaded_file.getvalue().decode('utf-8')
                    
                    # Detect delimiter
                    first_line = content.split('\n')[0]
                    if '\t' in first_line:
                        delimiter = '\t'
                    elif '|' in first_line:
                        delimiter = '|'
                    else:
                        delimiter = ','
                    
                    # Parse as DataFrame
                    from io import StringIO
                    df = pd.read_csv(StringIO(content), delimiter=delimiter)
                
                # File loaded success banner with Continue button
                success_col, button_col = st.columns([3, 1])
                with success_col:
                    st.success(f"✅ **File loaded successfully!** — {uploaded_file.name}")
                with button_col:
                    if st.button("➡️ Continue", type="primary", use_container_width=True, key="continue_top"):
                        st.session_state.uploaded_file_data = df
                        st.session_state.uploaded_file_name = uploaded_file.name
                        st.session_state.upload_step = 2
                        # Get auto-mappings with debug info (default to stored procedure)
                        mappings, sql_query, raw_result = get_auto_mappings(list(df.columns))
                        st.session_state.auto_mappings = mappings
                        st.session_state.auto_mappings_sql = sql_query
                        st.session_state.auto_mappings_raw_result = raw_result
                        st.session_state.matching_method_used = "Pattern Match/ML"
                        st.rerun()
                
                # Quick stats in columns
                stat_col1, stat_col2, stat_col3 = st.columns(3)
                with stat_col1:
                    st.metric("Rows", f"{len(df):,}")
                with stat_col2:
                    st.metric("Columns", len(df.columns))
                with stat_col3:
                    st.metric("Format", file_extension.upper())
                
                # Show file preview
                with st.expander("📋 **Data Preview** (first 10 rows)", expanded=True):
                    st.dataframe(df.head(10), use_container_width=True)
                
                # Show detected columns
                with st.expander(f"📊 **Detected Columns** ({len(df.columns)} columns)", expanded=False):
                    cols = st.columns(4)
                    for i, col in enumerate(df.columns):
                        with cols[i % 4]:
                            st.write(f"• {col}")
                
                st.write("---")
                
                # Bottom continue button for users who scroll
                if st.button("➡️ Continue to Column Mapping", type="primary", use_container_width=True, key="continue_bottom"):
                    st.session_state.uploaded_file_data = df
                    st.session_state.uploaded_file_name = uploaded_file.name
                    st.session_state.upload_step = 2
                    # Get auto-mappings with debug info (default to stored procedure)
                    mappings, sql_query, raw_result = get_auto_mappings(list(df.columns))
                    st.session_state.auto_mappings = mappings
                    st.session_state.auto_mappings_sql = sql_query
                    st.session_state.auto_mappings_raw_result = raw_result
                    st.session_state.matching_method_used = "Stored Procedure"
                    st.rerun()
                    
            except Exception as e:
                st.error(f"❌ Error reading file: {str(e)}")
        else:
            # Show placeholder when no file uploaded
            st.markdown("""
            <div style="border: 2px dashed #666; border-radius: 10px; padding: 40px; text-align: center; margin: 20px 0;">
                <p style="font-size: 18px; color: #888;">Drag and drop a file above, or click to browse</p>
                <p style="color: #666;">Supported: CSV, TXT, XLS, XLSX</p>
            </div>
            """, unsafe_allow_html=True)
    
    # STEP 2: Map Columns
    elif current_step == 2:
        st.subheader("Step 2: Map Source Columns to Target Schema")
        
        df = st.session_state.uploaded_file_data
        auto_mappings = st.session_state.auto_mappings
        target_fields = get_unique_targets()
        
        if df is None:
            st.error("No file data found. Please go back and upload a file.")
            reset_upload_workflow()
            st.rerun()
        
        # Top navigation bar with file info and buttons
        top_col1, top_col2, top_col3 = st.columns([3, 1, 1])
        with top_col1:
            st.info(f"📁 **File:** {st.session_state.uploaded_file_name} — {len(df):,} rows, {len(df.columns)} columns")
        with top_col2:
            if st.button("⬅️ Back", use_container_width=True, key="back_top"):
                st.session_state.upload_step = 1
                st.rerun()
        with top_col3:
            # This will be enabled/disabled based on validation below, using a placeholder
            top_continue_placeholder = st.empty()
        
        st.markdown("""
        **Instructions:** For each source column, select the corresponding target field from the dropdown.
        Columns set to "(Ignored)" will not be included in the output table.
        Use the **➕ Add** button to save new mappings for future use.
        """)
        
        # Matching method selection
        st.markdown("#### 🔧 Matching Method")
        method_col1, method_col2, method_col3 = st.columns([2, 2, 1])
        with method_col1:
            matching_method = st.radio(
                "Select matching method",
                options=["Pattern Match/ML", "LLM (Cortex AI)"],
                horizontal=True,
                help="**Pattern Match/ML**: Uses field_matcher_advanced with TF-IDF similarity matching. **LLM**: Uses Snowflake Cortex AI for intelligent semantic matching.",
                key="matching_method"
            )
        
        # LLM model selection (only shown when LLM is selected)
        with method_col2:
            if matching_method == "LLM (Cortex AI)":
                # Fetch available Cortex LLM models from Snowflake in realtime
                llm_models = get_available_llm_models()
                selected_model = st.selectbox(
                    "Select LLM Model",
                    options=llm_models,
                    index=0,
                    help="Available Cortex AI models in your region. List is fetched dynamically from Snowflake.",
                    key="llm_model_select"
                )
        
        with method_col3:
            if matching_method == "LLM (Cortex AI)":
                if st.button("🤖 Run LLM Matching", type="primary", use_container_width=True):
                    with st.spinner(f"Calling {selected_model}..."):
                        llm_mappings, llm_sql, llm_response = get_llm_recommendations(list(df.columns), target_fields, selected_model)
                        st.session_state.auto_mappings = llm_mappings
                        st.session_state.auto_mappings_sql = llm_sql
                        st.session_state.auto_mappings_raw_result = llm_response
                        st.session_state.matching_method_used = "LLM"
                        st.session_state.llm_model_used = selected_model
                    st.rerun()
        
        # Update auto_mappings based on method (for display purposes)
        if matching_method == "LLM (Cortex AI)" and st.session_state.get('matching_method_used') != "LLM":
            st.info("👆 Select a model and click **Run LLM Matching** to use Cortex AI for column matching")
        
        # Threshold slider for auto-mapping
        col_slider, col_info = st.columns([2, 1])
        with col_slider:
            mapping_threshold = st.slider(
                "Auto-mapping confidence threshold",
                min_value=0.0,
                max_value=1.0,
                value=0.3,
                step=0.05,
                help="Only auto-select target fields when confidence score is above this threshold. Lower = more auto-mappings, Higher = stricter matching."
            )
        with col_info:
            st.write("")  # Spacing
            if auto_mappings:
                above_threshold = sum(1 for m in auto_mappings.values() if m.get('score', 0) >= mapping_threshold)
                st.metric("Matches above threshold", f"{above_threshold} / {len(df.columns)}")
        
        # Show auto-mapping status
        method_used = st.session_state.get('matching_method_used', 'Pattern Match/ML')
        if auto_mappings:
            matched_count = sum(1 for m in auto_mappings.values() if m.get('score', 0) >= mapping_threshold)
            if method_used == "LLM":
                llm_model_used = st.session_state.get('llm_model_used', 'Cortex AI')
                st.success(f"✅ Auto-matched {matched_count} of {len(df.columns)} columns using **{llm_model_used}** (threshold: {mapping_threshold:.0%})")
            else:
                st.success(f"✅ Auto-matched {matched_count} of {len(df.columns)} columns using **field_matcher_advanced** (threshold: {mapping_threshold:.0%})")
        else:
            st.info("ℹ️ No auto-mappings available. Please map columns manually or run LLM matching.")
        
        # Debug expander for auto-mapping details
        with st.expander("🔍 Debug: Auto-Mapping Details", expanded=False):
            st.write(f"**Matching Method Used:** {method_used}")
            if method_used == "LLM":
                llm_model_used = st.session_state.get('llm_model_used', 'unknown')
                st.write(f"**LLM Model:** {llm_model_used}")
            
            # Show SQL query
            sql_query = st.session_state.get('auto_mappings_sql', None)
            if sql_query:
                st.write("**SQL Query Executed:**")
                st.code(sql_query, language="sql")
            else:
                st.write("No SQL query available.")
            
            # Show raw results
            raw_result = st.session_state.get('auto_mappings_raw_result', None)
            if raw_result is not None:
                if method_used == "LLM":
                    st.write("**LLM Response:**")
                    if isinstance(raw_result, str):
                        st.text(raw_result)
                    else:
                        st.write(raw_result)
                elif hasattr(raw_result, '__len__') and len(raw_result) > 0:
                    st.write("**Raw Results from Pattern Match/ML:**")
                    st.write(f"Columns returned: {list(raw_result.columns)}")
                    st.dataframe(raw_result, use_container_width=True)
            else:
                st.write("No results available.")
            
            # Show parsed mappings
            if auto_mappings:
                st.write("**Parsed Mappings:**")
                mapping_debug = []
                for input_col, mapping_info in auto_mappings.items():
                    mapping_debug.append({
                        "Input Column": input_col,
                        "Suggested Target": mapping_info.get('target', ''),
                        "Matched Source": mapping_info.get('src_match', ''),
                        "Confidence Score": f"{mapping_info.get('score', 0):.4f}"
                    })
                st.dataframe(pd.DataFrame(mapping_debug), use_container_width=True)
        
        # Add skip option to target fields
        target_options = ["(Ignored)"] + target_fields
        
        # Create mapping form
        st.write("---")
        
        # First pass: collect all current selections to detect duplicates
        column_mappings = {}
        for i, src_col in enumerate(df.columns):
            # Get the current selection from session state if it exists
            key = f"map_{i}_{mapping_threshold}"
            if key in st.session_state:
                column_mappings[src_col] = st.session_state[key]
            else:
                # Use auto-suggestion logic for initial value
                auto_suggestion = auto_mappings.get(src_col, None)
                if auto_suggestion is None:
                    auto_suggestion = auto_mappings.get(src_col.lower(), {})
                suggested_target = auto_suggestion.get('target', None) if auto_suggestion else None
                suggested_score = auto_suggestion.get('score', 0) if auto_suggestion else 0
                
                if suggested_target and suggested_target in target_options and suggested_score >= mapping_threshold:
                    column_mappings[src_col] = suggested_target
                else:
                    column_mappings[src_col] = "(Ignored)"
        
        # Detect duplicate target mappings
        target_counts = {}
        for src, tgt in column_mappings.items():
            if tgt != "(Ignored)":
                if tgt not in target_counts:
                    target_counts[tgt] = []
                target_counts[tgt].append(src)
        
        duplicate_targets = {tgt: srcs for tgt, srcs in target_counts.items() if len(srcs) > 1}
        
        # Show duplicate warning if any
        if duplicate_targets:
            st.error(f"⚠️ **Duplicate mappings detected!** {len(duplicate_targets)} target field(s) have multiple source columns mapped:")
            for tgt, srcs in duplicate_targets.items():
                st.write(f"  • **{tgt}** ← {', '.join(srcs)}")
        
        # Header row
        header_col1, header_col2, header_col3, header_col4 = st.columns([2, 2, 1, 1])
        with header_col1:
            st.write("**Source Column**")
        with header_col2:
            st.write("**Target Field**")
        with header_col3:
            st.write("**Confidence**")
        with header_col4:
            st.write("**Add to Mappings**")
        
        # Second pass: render the UI
        for i, src_col in enumerate(df.columns):
            # Check if this row has a duplicate
            current_target = column_mappings.get(src_col, "(Ignored)")
            is_duplicate = current_target in duplicate_targets
            
            # Use container with highlight for duplicates
            if is_duplicate:
                st.markdown(f'<div style="background-color: rgba(255, 100, 100, 0.2); padding: 5px; border-radius: 5px; border-left: 4px solid #ff4444;">', unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
            
            # Get auto-suggested mapping - try exact match first, then lowercase
            auto_suggestion = auto_mappings.get(src_col, None)
            if auto_suggestion is None:
                auto_suggestion = auto_mappings.get(src_col.lower(), {})
            suggested_target = auto_suggestion.get('target', None) if auto_suggestion else None
            suggested_score = auto_suggestion.get('score', 0) if auto_suggestion else 0
            src_match = auto_suggestion.get('src_match', '') if auto_suggestion else ''
            
            with col1:
                if is_duplicate:
                    st.write(f"⚠️ **{src_col}**")
                else:
                    st.write(f"**{src_col}**")
                # Show sample values
                sample_vals = df[src_col].dropna().head(3).tolist()
                sample_str = ", ".join([str(v)[:30] for v in sample_vals])
                st.caption(f"Sample: {sample_str}...")
            
            with col2:
                # Find default index - only auto-select if score meets threshold
                if suggested_target and suggested_target in target_options and suggested_score >= mapping_threshold:
                    default_idx = target_options.index(suggested_target)
                else:
                    default_idx = 0  # Skip by default if no good match or below threshold
                
                selected_target = st.selectbox(
                    f"Target for {src_col}",
                    options=target_options,
                    index=default_idx,
                    key=f"map_{i}_{mapping_threshold}",  # Include threshold in key to force refresh on change
                    label_visibility="collapsed"
                )
                column_mappings[src_col] = selected_target
                
                # Show what source field it matched to - only if above threshold
                if src_match and suggested_target and suggested_score >= mapping_threshold:
                    st.caption(f"Matched: '{src_match}'")
                
                # Show duplicate warning on this row
                if is_duplicate:
                    other_sources = [s for s in duplicate_targets[current_target] if s != src_col]
                    st.caption(f"🔴 Also mapped by: {', '.join(other_sources)}")
            
            if is_duplicate:
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col3:
                # Only show confidence if score meets threshold
                if suggested_target and suggested_score >= mapping_threshold:
                    # Show confidence with color-coded indicator and percentage
                    if suggested_score >= 0.8:
                        st.success(f"🟢 {suggested_score:.1%}")
                    elif suggested_score >= 0.5:
                        st.warning(f"🟡 {suggested_score:.1%}")
                    else:
                        st.error(f"🔴 {suggested_score:.1%}")
                else:
                    st.write("—")
            
            with col4:
                # Add to mappings button - only show if a target is selected (not Ignored)
                if selected_target and selected_target != "(Ignored)":
                    if st.button("➕ Add", key=f"add_mapping_{i}", help=f"Add '{src_col}' → '{selected_target}' to mappings"):
                        try:
                            # Check if mapping already exists
                            check_sql = f"""
                            SELECT COUNT(*) as cnt FROM PROCESSOR.MAPPINGS_LIST 
                            WHERE SRC = '{src_col.replace("'", "''")}' 
                            AND TARGET = '{selected_target.replace("'", "''")}'
                            """
                            existing = session.sql(check_sql).collect()[0][0]
                            
                            if existing > 0:
                                st.warning(f"⚠️ Mapping already exists!")
                            else:
                                # Insert new mapping
                                insert_sql = f"""
                                INSERT INTO PROCESSOR.MAPPINGS_LIST (SRC, TARGET) 
                                VALUES ('{src_col.replace("'", "''")}', '{selected_target.replace("'", "''")}')
                                """
                                session.sql(insert_sql).collect()
                                st.success(f"✅ Added! Refreshing mappings...")
                                # Rerun auto-mapping to pick up the new mapping
                                mappings, sql_query, raw_result = get_auto_mappings(list(df.columns))
                                st.session_state.auto_mappings = mappings
                                st.session_state.auto_mappings_sql = sql_query
                                st.session_state.auto_mappings_raw_result = raw_result
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)[:50]}")
                else:
                    st.write("—")
        
        st.write("---")
        
        # Summary section with prominent styling
        st.markdown("### 📊 Mapping Summary")
        
        mapped_count = sum(1 for v in column_mappings.values() if v != "(Ignored)")
        skipped_count = len(column_mappings) - mapped_count
        
        summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
        with summary_col1:
            st.metric("🎚️ Confidence Threshold", f"{mapping_threshold:.0%}")
        with summary_col2:
            st.metric("✅ Columns Mapped", mapped_count)
        with summary_col3:
            st.metric("⏭️ Columns Ignored", skipped_count)
        with summary_col4:
            st.metric("📁 Total Columns", len(column_mappings))
        
        # Recalculate duplicates for validation
        final_target_counts = {}
        for src, tgt in column_mappings.items():
            if tgt != "(Ignored)":
                if tgt not in final_target_counts:
                    final_target_counts[tgt] = []
                final_target_counts[tgt].append(src)
        final_duplicates = {tgt: srcs for tgt, srcs in final_target_counts.items() if len(srcs) > 1}
        has_duplicates = len(final_duplicates) > 0
        proceed_disabled = mapped_count == 0 or has_duplicates
        
        # Update the top continue button now that we have validation info
        with top_continue_placeholder:
            if st.button("➡️ Continue", type="primary", disabled=proceed_disabled, use_container_width=True, key="continue_top_step2"):
                st.session_state.column_mappings = column_mappings
                st.session_state.upload_step = 3
                st.rerun()
        
        # Validation message
        if mapped_count == 0:
            st.warning("⚠️ **No columns mapped!** Please select at least one target field to continue.")
        elif has_duplicates:
            st.error(f"⚠️ **Cannot proceed!** {len(final_duplicates)} target field(s) have duplicate mappings. Please resolve duplicates first.")
        else:
            st.success(f"✅ **Ready to proceed!** {mapped_count} column(s) will be mapped to the target schema.")
        
        st.write("---")
        
        # Bottom navigation buttons
        nav_col1, nav_col2 = st.columns(2)
        with nav_col1:
            if st.button("⬅️ Back to Upload", use_container_width=True, key="back_bottom"):
                st.session_state.upload_step = 1
                st.rerun()
        with nav_col2:
            if st.button("➡️ Review & Create Table", type="primary", disabled=proceed_disabled, use_container_width=True, key="continue_bottom_step2"):
                st.session_state.column_mappings = column_mappings
                st.session_state.upload_step = 3
                st.rerun()
    
    # STEP 3: Review & Approve
    elif current_step == 3:
        st.subheader("Step 3: Review and Approve Mapping")
        
        df = st.session_state.uploaded_file_data
        column_mappings = st.session_state.column_mappings
        
        if df is None or not column_mappings:
            st.error("Missing data. Please start over.")
            reset_upload_workflow()
            st.rerun()
        
        # Table configuration section
        st.markdown("### 🗄️ Table Configuration")
        
        config_col1, config_col2 = st.columns(2)
        with config_col1:
            default_table_name = sanitize_table_name(st.session_state.uploaded_file_name)
            table_name = st.text_input(
                "📝 Table Name",
                value=default_table_name,
                help="The name of the table to create in Snowflake"
            )
        
        with config_col2:
            schema_name = st.text_input(
                "📂 Schema",
                value="PROCESSOR",
                help="The schema where the table will be created"
            )
        
        # Show full table path
        st.info(f"📍 **Full table path:** `CLAIMSIQ.{schema_name}.{table_name}`")
        
        st.write("---")
        
        # Show mapping summary in expander
        st.markdown("### 📋 Column Mapping Summary")
        
        mapping_data = []
        for src, tgt in column_mappings.items():
            if tgt != "(Ignored)":
                mapping_data.append({
                    "Source Column": src,
                    "Target Column": tgt,
                    "Status": "✅ Mapped"
                })
            else:
                mapping_data.append({
                    "Source Column": src,
                    "Target Column": "—",
                    "Status": "⏭️ Ignored"
                })
        
        mapping_df = pd.DataFrame(mapping_data)
        
        # Count mapped vs ignored
        mapped_count = sum(1 for m in mapping_data if m["Status"] == "✅ Mapped")
        ignored_count = len(mapping_data) - mapped_count
        
        tab_mapped, tab_ignored = st.tabs([f"✅ Mapped ({mapped_count})", f"⏭️ Ignored ({ignored_count})"])
        
        with tab_mapped:
            mapped_df = mapping_df[mapping_df["Status"] == "✅ Mapped"]
            if len(mapped_df) > 0:
                st.dataframe(mapped_df, use_container_width=True, hide_index=True)
            else:
                st.write("No columns mapped")
        
        with tab_ignored:
            ignored_df = mapping_df[mapping_df["Status"] == "⏭️ Ignored"]
            if len(ignored_df) > 0:
                st.dataframe(ignored_df[["Source Column"]], use_container_width=True, hide_index=True)
            else:
                st.write("No columns ignored")
        
        # Preview transformed data
        st.write("---")
        st.markdown("### 👀 Data Preview")
        
        # Create preview with mapped columns
        preview_cols = {src: tgt for src, tgt in column_mappings.items() if tgt != "(Ignored)"}
        if preview_cols:
            preview_df = df[list(preview_cols.keys())].head(5).copy()
            preview_df.columns = [preview_cols[col] for col in preview_df.columns]
            st.write("First 5 rows with mapped column names:")
            st.dataframe(preview_df, use_container_width=True)
        
        # Stats
        st.write("---")
        st.markdown("### 📊 Summary")
        
        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        with stat_col1:
            st.metric("📄 Total Rows", f"{len(df):,}")
        with stat_col2:
            st.metric("✅ Mapped Columns", len(preview_cols))
        with stat_col3:
            all_targets = get_unique_targets()
            st.metric("🎯 Target Columns", len(all_targets) if all_targets else 0)
        with stat_col4:
            st.metric("🗄️ Target Table", table_name)
        
        st.write("---")
        
        # Final confirmation and action buttons
        st.markdown("### 🚀 Ready to Create Table?")
        
        st.warning(f"""
        **Please confirm:**
        - Table `{schema_name}.{table_name}` will be created (or replaced if it exists)
        - **{len(df):,}** rows will be inserted
        - **{len(preview_cols)}** source columns will be mapped
        - All **{len(all_targets) if all_targets else 0}** target columns will be created (unmapped columns will be NULL)
        """)
        
        # Navigation buttons - prominent
        action_col1, action_col2, action_col3 = st.columns([1, 1, 2])
        
        with action_col1:
            if st.button("⬅️ Back to Mapping", use_container_width=True):
                st.session_state.upload_step = 2
                st.rerun()
        
        with action_col2:
            if st.button("🔄 Start Over", use_container_width=True):
                reset_upload_workflow()
                st.rerun()
        
        with action_col3:
            if st.button("✅ Create Table Now", type="primary", use_container_width=True):
                with st.spinner("Creating table and inserting data..."):
                    success, message = create_mapped_table(
                        df, 
                        column_mappings, 
                        table_name, 
                        schema_name
                    )
                
                if success:
                    st.session_state.created_table_name = f"{schema_name}.{table_name}"
                    st.session_state.created_row_count = len(df)
                    st.session_state.upload_step = 4
                    st.rerun()
                else:
                    st.error(f"❌ Error creating table: {message}")
    
    # STEP 4: Complete
    elif current_step == 4:
        # Success banner
        st.markdown("""
        <div style="background: linear-gradient(135deg, #1a472a 0%, #2d5a3d 100%); 
                    padding: 30px; border-radius: 10px; text-align: center; margin-bottom: 20px;">
            <h1 style="color: white; margin: 0;">🎉 Success!</h1>
            <p style="color: #90EE90; font-size: 18px; margin-top: 10px;">Your table has been created successfully</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Table details
        table_name = st.session_state.get('created_table_name', '')
        row_count = st.session_state.get('created_row_count', 0)
        
        detail_col1, detail_col2 = st.columns(2)
        with detail_col1:
            st.metric("📍 Table Created", table_name)
        with detail_col2:
            st.metric("📊 Rows Inserted", f"{row_count:,}")
        
        st.write("---")
        
        # Quick actions section
        st.markdown("### 🚀 What's Next?")
        
        action_col1, action_col2, action_col3 = st.columns(3)
        
        with action_col1:
            st.markdown("**Preview the data**")
            if st.button("👀 View Table Data", use_container_width=True):
                st.session_state.show_preview = True
        
        with action_col2:
            st.markdown("**Upload more files**")
            if st.button("📤 Upload Another File", type="primary", use_container_width=True):
                reset_upload_workflow()
                st.rerun()
        
        with action_col3:
            st.markdown("**Manage tables**")
            st.info("Go to **View Tables** tab to inspect and manage your tables")
        
        # Show preview if requested
        if st.session_state.get('show_preview', False):
            st.write("---")
            st.markdown(f"### 📋 Preview: `{table_name}`")
            try:
                preview = session.sql(f'SELECT * FROM {table_name} LIMIT 20').to_pandas()
                st.dataframe(preview, use_container_width=True)
                
                # Download option
                csv_data = preview.to_csv(index=False)
                st.download_button(
                    label="📥 Download Preview as CSV",
                    data=csv_data,
                    file_name=f"{table_name.replace('.', '_')}_preview.csv",
                    mime="text/csv"
                )
            except Exception as e:
                st.error(f"Error previewing table: {str(e)}")
        
        st.write("---")
        
        # SQL reference
        with st.expander("📝 SQL Query Reference", expanded=False):
            st.write("**Query to view all data:**")
            st.code(f'SELECT * FROM {table_name};', language='sql')
            
            st.write("**Query to count rows:**")
            st.code(f'SELECT COUNT(*) FROM {table_name};', language='sql')
            
            st.write("**Query to view column info:**")
            st.code(f"DESCRIBE TABLE {table_name};", language='sql')

# Help section (shown in all tabs)
st.write("---")
with st.expander("ℹ️ Help & Instructions"):
    st.markdown("""
    **Edit Mappings Tab:**
    - **Load Data**: Fetch current mappings from Snowflake
    - **Edit Existing**: Use the data editor to modify source field names or delete rows
    - **Add New**: Use the form to add new source→target mappings
    - **Filter/Search**: Use filters to find specific mappings quickly
    - **Save Changes**: Persist your edits to Snowflake
    - **Bulk Operations**: Delete multiple mappings or export data
    
    **Test Matches Tab:**
    - **Input Methods**: Enter field names via text area or add individually
    - **Quick Examples**: Load common test scenarios with preset buttons
    - **Algorithm Parameters**: Adjust matching sensitivity and result count
    - **Results Analysis**: View detailed scores and confidence levels
    - **Export Results**: Download matching results as CSV
    
    **Upload & Map File Tab:**
    - **Step 1 - Upload**: Upload a CSV or TXT file with headers
    - **Step 2 - Map**: Map each source column to a target field (auto-suggestions provided)
    - **Step 3 - Review**: Review the mapping and preview transformed data
    - **Step 4 - Complete**: Table is created with mapped data
    
    **View Tables Tab:**
    - **Browse Tables**: View all tables in the PROCESSOR schema
    - **Inspect Data**: Preview table contents and structure
    - **Table Details**: View row counts, column info, and creation dates
    
    **Matching Algorithm Details:**
    - **Exact Match (40%)**: Identical field names after normalization
    - **Substring Match (20%)**: One field contains the other
    - **Sequence Similarity (20%)**: Character-level similarity (handles typos)
    - **Word Overlap (20%)**: Word-based similarity (Jaccard coefficient)
    - **TF-IDF Enhancement (30%)**: Semantic similarity boost using machine learning
    
    **Tips:**
    - Target fields are standardized healthcare terms (not editable)
    - Source fields can be customized to match your data sources
    - Higher confidence scores indicate better matches
    - The file upload tab auto-suggests mappings based on the matching algorithm
    - Table names are automatically sanitized from the filename
    """)

# TAB 4: View Tables
with tab4:
    st.subheader("📊 View Tables")
    st.write("Browse and inspect tables created in the PROCESSOR schema")
    
    # Function to get list of tables
    def get_schema_tables():
        """Get list of tables in the PROCESSOR schema"""
        try:
            tables_query = """
            SELECT 
                TABLE_NAME,
                ROW_COUNT,
                BYTES,
                CREATED,
                LAST_ALTERED
            FROM CLAIMSIQ.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'PROCESSOR'
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY CREATED DESC
            """
            result = session.sql(tables_query).to_pandas()
            return result
        except Exception as e:
            st.error(f"Error fetching tables: {str(e)}")
            return None
    
    # Function to get table columns
    def get_table_columns(table_name):
        """Get column information for a table"""
        try:
            columns_query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                ORDINAL_POSITION
            FROM CLAIMSIQ.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'PROCESSOR'
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """
            result = session.sql(columns_query).to_pandas()
            return result
        except Exception as e:
            st.error(f"Error fetching columns: {str(e)}")
            return None
    
    # Function to preview table data
    def preview_table_data(table_name, limit=100):
        """Preview data from a table"""
        try:
            preview_query = f'SELECT * FROM PROCESSOR."{table_name}" LIMIT {limit}'
            result = session.sql(preview_query).to_pandas()
            return result
        except Exception as e:
            st.error(f"Error fetching data: {str(e)}")
            return None
    
    # Refresh button
    if st.button("🔄 Refresh Table List", type="primary"):
        st.rerun()
    
    # Get tables
    tables_df = get_schema_tables()
    
    if tables_df is not None and len(tables_df) > 0:
        st.write(f"**Found {len(tables_df)} tables in CLAIMSIQ.PROCESSOR**")
        
        # Display table list with metrics
        st.write("---")
        
        # Table selector
        table_names = tables_df['TABLE_NAME'].tolist()
        selected_table = st.selectbox(
            "Select a table to inspect",
            options=table_names,
            index=0
        )
        
        if selected_table:
            # Get table info
            table_info = tables_df[tables_df['TABLE_NAME'] == selected_table].iloc[0]
            
            # Display table metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                row_count = table_info['ROW_COUNT'] if table_info['ROW_COUNT'] else 0
                st.metric("Rows", f"{row_count:,}")
            with col2:
                bytes_size = table_info['BYTES'] if table_info['BYTES'] else 0
                if bytes_size > 1024*1024:
                    size_str = f"{bytes_size/(1024*1024):.2f} MB"
                elif bytes_size > 1024:
                    size_str = f"{bytes_size/1024:.2f} KB"
                else:
                    size_str = f"{bytes_size} B"
                st.metric("Size", size_str)
            with col3:
                created = table_info['CREATED']
                if created:
                    st.metric("Created", str(created)[:10])
                else:
                    st.metric("Created", "—")
            with col4:
                altered = table_info['LAST_ALTERED']
                if altered:
                    st.metric("Last Modified", str(altered)[:10])
                else:
                    st.metric("Last Modified", "—")
            
            st.write("---")
            
            # Tabs for table details
            detail_tab1, detail_tab2 = st.tabs(["📋 Data Preview", "🔧 Column Schema"])
            
            with detail_tab1:
                st.write(f"**Data Preview for `{selected_table}`**")
                
                # Row limit selector
                preview_limit = st.slider("Preview rows", min_value=10, max_value=500, value=100, step=10)
                
                # Get preview data
                preview_df = preview_table_data(selected_table, preview_limit)
                
                if preview_df is not None and len(preview_df) > 0:
                    st.dataframe(preview_df, use_container_width=True)
                    
                    # Download button
                    csv_data = preview_df.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download {selected_table} (first {len(preview_df)} rows)",
                        data=csv_data,
                        file_name=f"{selected_table}.csv",
                        mime="text/csv"
                    )
                elif preview_df is not None:
                    st.info("Table is empty")
            
            with detail_tab2:
                st.write(f"**Column Schema for `{selected_table}`**")
                
                # Get columns
                columns_df = get_table_columns(selected_table)
                
                if columns_df is not None and len(columns_df) > 0:
                    # Display columns in a nice format
                    st.dataframe(
                        columns_df,
                        use_container_width=True,
                        column_config={
                            "COLUMN_NAME": st.column_config.TextColumn("Column Name"),
                            "DATA_TYPE": st.column_config.TextColumn("Data Type"),
                            "IS_NULLABLE": st.column_config.TextColumn("Nullable"),
                            "ORDINAL_POSITION": st.column_config.NumberColumn("Position")
                        }
                    )
                    
                    st.write(f"**Total Columns:** {len(columns_df)}")
            
            st.write("---")
            
            # Danger zone - delete table
            # Don't allow deletion of MAPPINGS_LIST (system table)
            if selected_table.upper() == "MAPPINGS_LIST":
                st.info("ℹ️ The MAPPINGS_LIST table is a system table and cannot be deleted.")
            else:
                with st.expander("⚠️ Danger Zone", expanded=False):
                    st.warning("**Delete this table permanently**")
                    st.write(f"This will permanently delete the table `{selected_table}` and all its data.")
                    
                    # Use checkbox confirmation instead of text input
                    # Use table name in key so it resets when selecting different table
                    confirm_delete = st.checkbox(
                        f"I confirm I want to delete **{selected_table}**",
                        key=f"delete_confirm_{selected_table}"
                    )
                    
                    if st.button("🗑️ Delete Table", type="secondary", disabled=not confirm_delete):
                        try:
                            delete_sql = f'DROP TABLE IF EXISTS PROCESSOR."{selected_table}"'
                            session.sql(delete_sql).collect()
                            # Clear the checkbox state before rerun
                            if f"delete_confirm_{selected_table}" in st.session_state:
                                del st.session_state[f"delete_confirm_{selected_table}"]
                            st.success(f"✅ Table `{selected_table}` deleted successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error deleting table: {str(e)}")
    
    else:
        st.info("No tables found in CLAIMSIQ.PROCESSOR schema. Upload and map a file to create your first table!")

# TAB 5: LLM Settings
with tab5:
    st.subheader("🤖 LLM Settings")
    st.write("Configure the LLM prompt template used for intelligent field mapping.")
    
    st.markdown("""
    **About the Prompt Template:**
    - The prompt is sent to the selected Cortex AI model when you use LLM matching
    - Use `{source_list}` as a placeholder for the source column names
    - Use `{target_list}` as a placeholder for the target field names
    - The LLM should return a JSON object mapping source columns to targets with confidence scores
    """)
    
    st.write("---")
    
    # Action buttons
    action_col1, action_col2, action_col3 = st.columns([1, 1, 2])
    
    with action_col1:
        if st.button("🔄 Reset to Default", type="secondary", use_container_width=True):
            st.session_state.llm_prompt_template = DEFAULT_LLM_PROMPT
            st.success("✅ Prompt reset to default!")
            st.rerun()
    
    with action_col2:
        if st.button("💾 Save Changes", type="primary", use_container_width=True, key="save_llm_prompt"):
            # The prompt is automatically saved via session state when edited
            st.success("✅ Prompt template saved!")
    
    with action_col3:
        st.write("")  # Spacer
    
    st.write("---")
    
    # Editable prompt template
    st.markdown("### 📝 Prompt Template")
    
    # Show current prompt in a text area
    edited_prompt = st.text_area(
        "Edit the LLM prompt template:",
        value=st.session_state.llm_prompt_template,
        height=400,
        help="Edit the prompt sent to the LLM. Use {source_list} and {target_list} as placeholders.",
        key="llm_prompt_editor"
    )
    
    # Update session state if changed
    if edited_prompt != st.session_state.llm_prompt_template:
        st.session_state.llm_prompt_template = edited_prompt
        st.info("📝 Prompt modified. Click **Save Changes** to confirm.")
    
    # Show placeholder info
    st.write("---")
    st.markdown("### 📋 Available Placeholders")
    
    placeholder_data = [
        {"Placeholder": "{source_list}", "Description": "List of source column names from the uploaded file (one per line with bullet points)"},
        {"Placeholder": "{target_list}", "Description": "List of target field names from the mappings table (one per line with bullet points)"},
    ]
    st.table(pd.DataFrame(placeholder_data))
    
    # Preview section
    st.write("---")
    st.markdown("### 👁️ Prompt Preview")
    
    with st.expander("Preview prompt with sample data", expanded=False):
        # Sample data for preview
        sample_sources = ["PAT_FNAME", "PAT_LNAME", "PAT_BIRTH_DT", "CLM_NBR", "BILLED_AMT"]
        sample_targets = ["Claimant First Name", "Claimant Last Name", "Claimant DOB", "Claim Number/Claim Control Number", "Billed Amount"]
        
        sample_source_list = "\n".join([f"- {col}" for col in sample_sources])
        sample_target_list = "\n".join([f"- {field}" for field in sample_targets])
        
        try:
            preview_prompt = st.session_state.llm_prompt_template.format(
                source_list=sample_source_list,
                target_list=sample_target_list
            )
            st.code(preview_prompt, language="text")
        except KeyError as e:
            st.error(f"❌ Invalid placeholder in prompt: {e}")
        except Exception as e:
            st.error(f"❌ Error previewing prompt: {e}")
    
    # Tips section
    st.write("---")
    st.markdown("### 💡 Tips for Better Results")
    st.markdown("""
    - **Be specific** about the expected JSON output format
    - **Include examples** of common abbreviations in your domain
    - **Set clear rules** about handling duplicates and uncertain matches
    - **Specify confidence thresholds** in the prompt if needed
    - **Test with different models** - some models follow instructions better than others
    """)