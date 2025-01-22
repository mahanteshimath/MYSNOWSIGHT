import re
import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time
from datetime import datetime
from PIL import Image
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache configuration settings
@st.cache_data(ttl=3600)
def get_config():
    return {
        "page_title": "MYSNOWSIGHT",
        "page_icon": "  ",
        "layout": "wide",
        "initial_sidebar_state": "expanded"
    }

# Initialize app configuration
st.set_page_config(**get_config())

# Cache Snowflake connection
@st.cache_resource
def create_snowflake_connection(account, role, warehouse, database, schema, user, password):
    try:
        conn = snowflake.connector.connect(
            account=account,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            user=user,
            password=password,
            client_session_keep_alive=True
        )
        st.toast("Connection to Snowflake successful!", icon='🎉')
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

# Cache database operations
@st.cache_data(ttl=300)
def get_database_list(conn):
    cursor = conn.cursor()
    try:
        db_list = cursor.execute("SHOW DATABASES")
        return [db[1] for db in db_list]
    finally:
        cursor.close()

@st.cache_data(ttl=300)
def get_schema_list(conn, database):
    cursor = conn.cursor()
    try:
        sch_list = cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return [sch[1] for sch in sch_list]
    finally:
        cursor.close()

# Utility functions
def execute_query_with_retry(conn, query, max_retries=3):
    for attempt in range(max_retries):
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed (attempt {attempt + 1}): {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(1)  # Wait before retry
        finally:
            cursor.close()

def load_file_to_dataframe(file):
    file_extension = file.name.split('.')[-1].lower()
    if file_extension in ['xls', 'xlsx']:
        return pd.read_excel(file)
    return pd.read_csv(file, encoding='utf-8')

# Main app sections
def sidebar():
    with st.sidebar:
        st.markdown("[![LinkedIn](https://cdn2.iconfinder.com/data/icons/social-media-2285/512/1_Linkedin_unofficial_colored_svg-48.png)](https://www.linkedin.com/in/mahantesh-hiremath/) Connect with me")
        st.sidebar.header("Snowflake Credentials")
        return {
            "account": st.sidebar.text_input("Account"),
            "role": st.sidebar.text_input("Role"),
            "warehouse": st.sidebar.text_input("Warehouse"),
            "database": st.sidebar.text_input("Database"),
            "schema": st.sidebar.text_input("Schema"),
            "user": st.sidebar.text_input("User"),
            "password": st.sidebar.text_input("Password", type="password")
        }

def parallel_execute_tab(conn):
    st.title("Parallel Query Execution")
    st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)
    
    queries = st.text_area("Input queries separated by ;", height=200)
    if st.button("Execute Queries"):
        query_list = [q.strip() for q in queries.split(';') if q.strip()]
        if query_list:
            with st.spinner("Executing queries in parallel..."):
                with ThreadPoolExecutor(max_workers=min(len(query_list), 5)) as executor:
                    results = list(executor.map(lambda q: execute_query_with_retry(conn, q), query_list))
                st.success("All queries executed successfully!")
                for idx, result in enumerate(results):
                    st.write(f"Query {idx + 1} result:", result)

def load_file_tab(conn, credentials):
    st.title("Upload File to Snowflake")
    st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)
    
    file = st.file_uploader('Upload file', type=['xls', 'xlsx', 'csv', 'txt'])
    if file:
        data = load_file_to_dataframe(file)
        st.subheader('Preview')
        st.write(data.head())
        
        table_name = st.text_input('Enter target table name')
        if st.button('Save to Snowflake'):
            try:
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=data,
                    table_name=table_name,
                    database=credentials['database'],
                    schema=credentials['schema'],
                    auto_create_table=True
                )
                st.success(f'Data loaded successfully: {nrows} rows')
            except Exception as e:
                logger.error(f"File upload failed: {str(e)}")
                st.error(f"Error: {str(e)}")

def document_ai_tab(conn, credentials):
    st.title('Document AI: Upload invoices and ask questions')
    st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)
    
    left, right = st.columns(2)
    
    with right:
        uploaded_file = st.file_uploader("Upload image", type=["jpg", "jpeg", "png"])
        if uploaded_file:
            image = Image.open(uploaded_file)
            st.image(image, caption="Uploaded Image.", use_column_width=True)
    
    with left:
        google_api_key = st.text_input("INPUT GOOGLE_API_KEY", type="password")
        if google_api_key:
            genai.configure(api_key=google_api_key)
        
        st.header("Invoice Reader Application")
        input_text = st.text_input("Ask about invoice:")
        
        if st.button("Submit") and uploaded_file and google_api_key:
            try:
                image_data = {
                    "mime_type": uploaded_file.type,
                    "data": uploaded_file.getvalue()
                }
                
                model = genai.GenerativeModel('gemini-1.5-flash')
                response = model.generate_content([
                    "You are an expert in understanding invoices.",
                    image_data,
                    input_text
                ])
                
                st.subheader("Answer:")
                st.write(response.text)
                
                # Save to Snowflake
                data = pd.DataFrame({
                    'FILENAME': [uploaded_file.name],
                    'QUESTION': [input_text],
                    'RESPONSE': [response.text],
                    'TIMESTAMP': [datetime.now()]
                })
                
                write_pandas(
                    conn=conn,
                    df=data,
                    table_name='INVOICE_DATA',
                    database=credentials['database'],
                    schema=credentials['schema'],
                    auto_create_table=True
                )
                
            except Exception as e:
                logger.error(f"Document AI processing failed: {str(e)}")
                st.error(f"Error processing document: {str(e)}")

def generate_ddl_tab(conn):
    st.title("Generate DDL")
    st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)
    
    chk_box = st.selectbox("Generate DDL for entire DB?", options=["YES", "NO"], index=1)
    
    if conn:
        cursor = conn.cursor()
        db_names = get_database_list(conn)
        db_name = st.selectbox("Select Database", db_names)
        
        if chk_box == 'NO' and db_name:
            sch_names = get_schema_list(conn, db_name)
            sch_name = st.selectbox("Select Schema", sch_names)
            
            if sch_name:
                entity_types = [
                    "Dynamic Table", "Event Table", "File Format", "Function",
                    "Iceberg Table", "Masking Policy", "Password Policy", "Pipe",
                    "Procedure", "Row Access Policy", "Sequence", "Session Policy",
                    "Stream", "Table", "Tag", "Task", "View"
                ]
                entity_type = st.selectbox("Select Object Type", entity_types)
                
                if entity_type:
                    try:
                        if entity_type in ['Function', 'Procedure']:
                            ent_list = cursor.execute(f"SHOW USER {entity_type}S IN SCHEMA {db_name}.{sch_name}")
                            ent_names = [ent[8] for ent in ent_list]
                        else:
                            ent_list = cursor.execute(f"SHOW {re.sub('Policy', 'Policie', entity_type)}S IN SCHEMA {db_name}.{sch_name}")
                            ent_names = [ent[1] for ent in ent_list]
                        
                        selected_entities = st.multiselect(f"Select {entity_type}s", ent_names)
                        
                        if selected_entities and st.button('Generate DDL'):
                            ddl_statements = []
                            for entity_name in selected_entities:
                                ent_name = re.sub("(.*?) RETURN.*", "\\1", entity_name)
                                ent_type = 'Policy' if 'Policy' in entity_type else re.sub(" ", "_", entity_type)
                                ddl_query = f"SELECT GET_DDL('{ent_type}', '{db_name}.{sch_name}.{ent_name}', true)"
                                result = cursor.execute(ddl_query).fetchone()
                                ddl_statements.append(result[0])
                            
                            combined_ddl = "\n\n-------------------------------------------------------------------------------------------\n\n".join(ddl_statements)
                            st.code(combined_ddl, language="sql")
                            
                    except Exception as e:
                        logger.error(f"DDL generation failed: {str(e)}")
                        st.error(f"Error generating DDL: {str(e)}")
        
        elif chk_box == 'YES' and db_name and st.button('Generate DDL'):
            try:
                ddl_query = f"SELECT GET_DDL('DATABASE', '{db_name}', true)"
                result = cursor.execute(ddl_query).fetchone()
                st.code(result[0], language="sql")
            except Exception as e:
                logger.error(f"Database DDL generation failed: {str(e)}")
                st.error(f"Error generating database DDL: {str(e)}")

def replicate_database_tab():
    st.title("Replicate Database")
    st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("Source Snowflake Credentials")
        source_creds = {
            "account": st.text_input("Source Account"),
            "role": st.text_input("Source Role"),
            "warehouse": st.text_input("Source Warehouse"),
            "database": st.text_input("Source Database"),
            "schema": st.text_input("Source Schema"),
            "user": st.text_input("Source User"),
            "password": st.text_input("Source Password", type="password")
        }
        
        if st.button("Test Source Connection"):
            source_conn = create_snowflake_connection(**source_creds)
    
    with col2:
        st.header("Destination Snowflake Credentials")
        dest_creds = {
            "account": st.text_input("Destination Account"),
            "role": st.text_input("Destination Role"),
            "warehouse": st.text_input("Destination Warehouse"),
            "database": st.text_input("Destination Database"),
            "schema": st.text_input("Destination Schema"),
            "user": st.text_input("Destination User"),
            "password": st.text_input("Destination Password", type="password")
        }
        
        if st.button("Test Destination Connection"):
            dest_conn = create_snowflake_connection(**dest_creds)
    
    if st.button("Replicate Data"):
        try:
            source_conn = create_snowflake_connection(**source_creds)
            dest_conn = create_snowflake_connection(**dest_creds)
            
            if source_conn and dest_conn:
                # Get table structure
                source_cursor = source_conn.cursor()
                ddl_query = f'''
                    DECLARE
                        CUR CURSOR FOR SELECT CONCAT_WS('.',TABLE_CATALOG,TABLE_SCHEMA,'"'||TABLE_NAME||'"') AS NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE = 'BASE TABLE' 
                        AND TABLE_NAME NOT LIKE '%TEMP_VIEW_DEFS%'
                        AND TABLE_SCHEMA='{source_creds["schema"]}' 
                        AND IS_TEMPORARY='NO'
                        ORDER BY CREATED ASC;
                    BEGIN
                        CREATE OR REPLACE TEMPORARY TABLE TEMP_VIEW_DEFS(VIEW_NAME TEXT, DEFINITION TEXT);
                        
                        FOR rec IN CUR DO   
                            EXECUTE IMMEDIATE REPLACE(
                                'INSERT INTO TEMP_VIEW_DEFS(VIEW_NAME, DEFINITION)
                                SELECT ''<VIEW_NAME>'', GET_DDL(''TABLE'', ''<VIEW_NAME>'')',
                                '<VIEW_NAME>',
                                rec.NAME
                            );
                        END FOR;
                        
                        LET rs RESULTSET := (SELECT LISTAGG(DEFINITION,' ') DDL FROM TEMP_VIEW_DEFS);
                        RETURN TABLE(rs);
                    END;
                '''
                
                ddl_result = source_cursor.execute(ddl_query).fetchone()[0]
                dest_cursor = dest_conn.cursor()
                
                # Create database and schema in destination
                setup_ddl = f"""
                    BEGIN 
                        create or replace database {source_creds['database']}; 
                        create or replace schema {source_creds['database']}.{source_creds['schema']}; 
                        USE {source_creds['database']}.{source_creds['schema']}; 
                        {ddl_result} 
                    END;
                """
                
                dest_cursor.execute(setup_ddl)
                st.success("Database structure replicated successfully!")
                
                # Get list of tables to replicate
                table_query = f"""
                    BEGIN 
                        SHOW TABLES IN {source_creds['database']}.{source_creds['schema']};
                        LET A := SQLID;
                        LET rs RESULTSET := (SELECT * FROM TABLE(RESULT_SCAN(:A)) WHERE "kind" = 'TABLE');
                        RETURN TABLE(rs);
                    END;
                """
                
                source_tables = source_cursor.execute(table_query)
                table_names = [table[1] for table in source_tables]
                
                # Replicate data for each table
                total_tables = len(table_names)
                st.info(f"Starting replication of {total_tables} tables...")
                progress_bar = st.progress(0)
                
                for idx, table_name in enumerate(table_names):
                    try:
                        full_table_name = f'"{source_creds["database"]}"."{source_creds["schema"]}"."{table_name}"'
                        query = f"SELECT * FROM {full_table_name}"
                        
                        # Read data in chunks to handle large tables
                        for chunk in pd.read_sql(query, source_conn, chunksize=10000):
                            success, nchunks, nrows, _ = write_pandas(
                                conn=dest_conn,
                                df=chunk,
                                table_name=table_name,
                                database=dest_creds['database'],
                                schema=dest_creds['schema'],
                                auto_create_table=True
                            )
                        
                        st.success(f"Replicated table: {table_name}")
                        progress_bar.progress((idx + 1) / total_tables)
                    
                    except Exception as e:
                        logger.error(f"Error replicating table {table_name}: {str(e)}")
                        st.error(f"Failed to replicate table {table_name}: {str(e)}")
                
                st.success(f"Database replication completed! {total_tables} tables replicated.")
                st.balloons()
                
        except Exception as e:
            logger.error(f"Database replication failed: {str(e)}")
            st.error(f"Error during replication: {str(e)}")

def test_data_generator_tab(conn):
    st.title("Test Data Generator")
    st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)
    
    if not conn:
        st.warning("Please connect to Snowflake first")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        table_name = st.text_input("Enter table name")
        num_rows = st.number_input("Number of rows to generate", min_value=1, value=1000)
        
        column_specs = []
        num_columns = st.number_input("Number of columns", min_value=1, value=3)
        
        for i in range(num_columns):
            st.subheader(f"Column {i+1}")
            col_name = st.text_input(f"Column name", key=f"col_name_{i}")
            col_type = st.selectbox(
                "Data type",
                ["INTEGER", "FLOAT", "STRING", "DATE", "TIMESTAMP", "BOOLEAN"],
                key=f"col_type_{i}"
            )
            
            if col_type in ["INTEGER", "FLOAT"]:
                min_val = st.number_input(f"Minimum value", key=f"min_{i}")
                max_val = st.number_input(f"Maximum value", key=f"max_{i}")
                column_specs.append({
                    "name": col_name,
                    "type": col_type,
                    "min": min_val,
                    "max": max_val
                })
            elif col_type in ["DATE", "TIMESTAMP"]:
                start_date = st.date_input(f"Start date", key=f"start_{i}")
                end_date = st.date_input(f"End date", key=f"end_{i}")
                column_specs.append({
                    "name": col_name,
                    "type": col_type,
                    "start": start_date,
                    "end": end_date
                })
            else:
                column_specs.append({
                    "name": col_name,
                    "type": col_type
                })
    
    with col2:
        if st.button("Generate Test Data"):
            try:
                data = {}
                for spec in column_specs:
                    if spec["type"] == "INTEGER":
                        data[spec["name"]] = np.random.randint(spec["min"], spec["max"], num_rows)
                    elif spec["type"] == "FLOAT":
                        data[spec["name"]] = np.random.uniform(spec["min"], spec["max"], num_rows)
                    elif spec["type"] == "STRING":
                        data[spec["name"]] = [f"str_{i}" for i in range(num_rows)]
                    elif spec["type"] in ["DATE", "TIMESTAMP"]:
                        start = pd.Timestamp(spec["start"])
                        end = pd.Timestamp(spec["end"])
                        date_range = pd.date_range(start=start, end=end, periods=num_rows)
                        data[spec["name"]] = date_range
                    elif spec["type"] == "BOOLEAN":
                        data[spec["name"]] = np.random.choice([True, False], num_rows)
                
                df = pd.DataFrame(data)
                st.write("Preview of generated data:")
                st.write(df.head())
                
                if st.button("Save to Snowflake"):
                    success, nchunks, nrows, _ = write_pandas(
                        conn=conn,
                        df=df,
                        table_name=table_name,
                        database=credentials['database'],
                        schema=credentials['schema'],
                        auto_create_table=True
                    )
                    st.success(f"Generated {nrows} rows of test data and saved to {table_name}")
            
            except Exception as e:
                logger.error(f"Test data generation failed: {str(e)}")
                st.error(f"Error generating test data: {str(e)}")

def main():
    credentials = sidebar()
    conn = None
    
    if st.sidebar.button("Connect"):
        conn = create_snowflake_connection(**credentials)
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "PARALLEL EXECUTE",
        "LOAD FILE",
        "DOCUMENT AI",
        "GENERATE DDL",
        "REPLICATE DATABASE",
        "TEST DATA GENERATOR"
    ])
    
    with tab1:
        parallel_execute_tab(conn)
    with tab2:
        load_file_tab(conn, credentials)
    with tab3:
        document_ai_tab(conn, credentials)
    with tab4:
        generate_ddl_tab(conn)
    with tab5:
        replicate_database_tab()
    with tab6:
        test_data_generator_tab(conn)
    
    # Footer
    st.markdown(
        """
        <div style='position: fixed; bottom: 0; width: 100%; text-align: center; padding: 10px; background: white;'>
            Developed with ❤️ by <a href="https://bit.ly/atozaboutdata" target="_blank">MAHANTESH HIREMATH</a>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()