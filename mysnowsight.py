import re
import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time
from datetime import datetime
import os
import pathlib
import textwrap
from PIL import Image
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(
  page_title="MYSNOWSIGHT",
  page_icon="  ",
  layout="wide",
  initial_sidebar_state="expanded",
) 

tab1, tab2, tab3 , tab4,tab5 = st.tabs(["PARALLEL EXECUTE", "LOAD FILE", "DOCUMENT AI", "GENERATE DDL","REPLICATE DATABASE"])

# Sidebar for Snowflake credentials
with st.sidebar:
    st.markdown("[![Foo](https://cdn2.iconfinder.com/data/icons/social-media-2285/512/1_Linkedin_unofficial_colored_svg-48.png)](https://www.linkedin.com/in/mahantesh-hiremath/) Connect me.")   
    st.sidebar.header("Snowflake Credentials")
    account = st.sidebar.text_input("Account")
    role = st.sidebar.text_input("Role")
    warehouse = st.sidebar.text_input("Warehouse")
    database = st.sidebar.text_input("Database")
    schema = st.sidebar.text_input("Schema")
    user = st.sidebar.text_input("User")
    password = st.sidebar.text_input("Password", type="password")	


# Create a Snowflake connection function
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
        st.toast("Connection to Snowflake successfully!", icon='🎉')
        time.sleep(.5)
        st.balloons()
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")    
    return conn

with tab1:

                # Function to execute queries in parallel
                def execute_queries(queries):
                    with ThreadPoolExecutor(max_workers=len(queries)) as executor:
                        results = list(executor.map(execute_single_query, queries))
                    return results

                # Function to execute a single query
                def execute_single_query(query):
                    connection = create_snowflake_connection(account, role, warehouse, database, schema, user, password)
                    cursor = connection.cursor()
                    start_time = time.time()

                    try:
                        cursor.execute(query)
                        result = cursor.fetchall()
                        elapsed_time = time.time() - start_time
                        return f"Query: {query}\nResult: {result}\nTime taken: {elapsed_time:.2f} seconds"
                    except connection.connector.errors.ProgrammingError as e:
                        return f"Query: {query}\nError: {str(e)}\nTime taken: {elapsed_time:.2f} seconds"
                    finally:
                        cursor.close()
                        connection.close()

                # Streamlit UI




                # Connect button
                def main():
                    if st.sidebar.button("Connect"):
                        #st.sidebar.set_visible(False)
                        connection = create_snowflake_connection(account, role, warehouse, database, schema, user, password)
                    st.title("My Snowsight")
                    #link to  YouTube channel
                    # st.markdown(" 👉 [🎥Visit my YouTube channel for more details](https://bit.ly/atozaboutdata)")
                    st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)


                    # Text area to input queries
                    queries = st.text_area("Input queries separated by ; to execute all in parallel",height=200)
                    query_list = [q.strip() for q in queries.split(';') if q.strip()]

                    if st.button("Execute Queries"):

                        if query_list:
                            with st.spinner("Executing all queries..."):

                                results = execute_queries(query_list)
                                st.success("All queries executed!")
                                for res in results:
                                    st.write(res)

                if __name__ == "__main__":
                    main()
with tab2:
            def main():
                st.title('Upload File to Snowflake')
                # st.markdown(" 👉 [🎥Visit my YouTube channel for more details](https://bit.ly/atozaboutdata)")
                st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)
                file = st.file_uploader('Upload file', type=['xls', 'xlsx', 'csv', 'txt'])

                if file is not None:
                    # Read the file
                    file_extension = file.name.split('.')[-1]
                    if file_extension.lower() in ['xls', 'xlsx', 'csv', 'txt']:
                        data = pd.read_excel(file) if file_extension.lower() in ['xls', 'xlsx'] else pd.read_csv(file, encoding='latin-1')

                        st.subheader('Preview of Uploaded Data')
                        st.write(data.head())

                        # Save data to Snowflake
                        conn = create_snowflake_connection(account, role, warehouse, database, schema, user, password)
                        if conn:
                            st.info('Connected to Snowflake!')

                            table_name = st.text_input('Enter table name in Snowflake')

                            if st.button('Save to Snowflake'):
                                try:
                                    data_f=pd.DataFrame(data)
                                    success, nchunks, nrows, _ = write_pandas(conn=conn,df=data_f,table_name=table_name,database=database,schema=schema,auto_create_table=True)
                                    
                                    st.success(f'Dataloaded to snowflake table: {table_name}  rows : {nrows}')
                                except Exception as e:
                                    st.error(f'Error: {str(e)}')
                        else:
                            st.error('Unable to connect to Snowflake. Please check your credentials.')

            if __name__ == '__main__':
                main()
with tab3:
            def main():
                def get_gemini_response(input,image,prompt):
                    model = genai.GenerativeModel('gemini-pro-vision')
                    response = model.generate_content([input,image[0],prompt])
                    return response.text
                
                def input_image_setup(uploaded_file):
                    # Check if a file has been uploaded
                    if uploaded_file is not None:
                        # Read the file into bytes
                        bytes_data = uploaded_file.getvalue()

                        image_parts = [
                            {
                                "mime_type": uploaded_file.type,  # Get the mime type of the uploaded file
                                "data": bytes_data
                            }
                        ]
                        return image_parts
                    else:
                        raise FileNotFoundError("No file uploaded")
                st.markdown('<p style="color: yellow;">👉🎥 Visit my YouTube channel for more details <a href="https://bit.ly/atozaboutdata">🎥click</a></p>', unsafe_allow_html=True)
                st.title('Document AI: Upload invoices and ask question')
                left, right= st.columns(2)
                with right:        
                        uploaded_file = st.file_uploader("upload image",type=["jpg", "jpeg", "png"])
                        image=""   
                        if uploaded_file is not None:
                            image = Image.open(uploaded_file)
                            st.image(image, caption="Uploaded Image.", use_column_width=True)
                with left:
                        google_api_key = st.text_input("INPUT GOOGLE_API_KEY",key="google_api_key")
                        genai.configure(api_key=google_api_key)  
                        st.header("Invoice reader Application")
                        input=st.text_input("Ask about invoice: ",key="input")
                        submit=st.button("Submit",key="submit")
                        input_prompt = """
                                    You are an expert in understanding invoices.
                                    You will receive input images or PDF as invoices &
                                    you will have to answer questions based on the input image
                                    """

                        if submit:
                            image_data = input_image_setup(uploaded_file)
                            response=get_gemini_response(input_prompt,image_data,input)
                            st.subheader("Answer: ")
                            st.write(response)
                            # current_timestamp = pd.Timestamp.now()  # Correct usage of datetime module
                            date_time =datetime.fromtimestamp(time.time()) 

                        # Dataframe creation with file details, question, response, and timestamp
                            data_to_save = pd.DataFrame({
                                'FILENAME': [uploaded_file.name if uploaded_file else None],
                                'QUESTION': [input],
                                'RESPONSE': [response],
                                'TIMESTAMP':[date_time]
                            })
                            st.subheader('Preview of Uploaded Data')
                            st.write(data_to_save.head())
                            table_name='INVOICE_DATA'
                            write_pandas(conn=create_snowflake_connection(account, role, warehouse, database, schema, user, password),df=data_to_save,table_name=table_name,database=database,schema=schema,auto_create_table=True)

            if __name__ == '__main__':
                main()
with tab4:
            def main():
                  st.title(":balloon: :balloon: Generate DDL :balloon: :balloon:")
                  st.write(":balloon: :balloon: This is to Generate DDL :balloon: :balloon:")
                  if all([account, role, warehouse, database, schema, user, password]):
                    conn = create_snowflake_connection(account, role, warehouse, database, schema, user, password)
                    cursor = conn.cursor()
                    chk_box = st.selectbox("Do you want to Generate DDL for entire DB?",options=["YES","NO"] , index=1)
                    
                                     
                         
                    if conn and chk_box=='NO':
                                st.info('Connected to Snowflake!')
                                db_list = cursor.execute("SHOW DATABASES")
                                db_names = [db[1] for db in db_list]

                                db_name = st.selectbox("Select Database", db_names, key=f"selected_dbnames")

                                if db_name:
                                    sch_list = cursor.execute(f"SHOW SCHEMAS IN DATABASE {db_name}")
                                    sch_names = [sch[1] for sch in sch_list]
                                    sch_name = st.selectbox("Select Schema", sch_names, index=0, key=f"schemaname_list")
                                    if sch_name:
                                        entity_types = [
                                            "Dynamic Table", "Event Table", "File Format", "Function",
                                            "Iceberg Table", "Masking Policy", "Password Policy", "Pipe",
                                            "Procedure", "Row Access Policy", "Sequence", "Session Policy",
                                            "Stream", "Table", "Tag", "Task", "View"
                                        ]
                                        entity_type = st.selectbox("Select Object Type", entity_types)

                                        if entity_type:
                                            if entity_type =='Function' or entity_type =='Procedure':
                                                 ent_list = cursor.execute(f"SHOW USER {entity_type}S IN SCHEMA {db_name}.{sch_name}")
                                                 ent_names = [ent[8] for ent in ent_list]  
                                                 
                                            else:
                                                 ent_list = cursor.execute(f"SHOW {re.sub('Policy','Policie',entity_type)}S IN SCHEMA {db_name}.{sch_name}")
                                                 ent_names = [ent[1] for ent in ent_list]

                                            # ent_names.insert(0, "ALL")
                                            
                                            selected_entities = st.multiselect(f"Select {entity_type}s", ent_names, key=f"selected_entity_list")
                                            if selected_entities:
                                                
                                                if 'Policy' in entity_type:
                                                    ent_type = 'Policy'
                                                else:
                                                    ent_type = re.sub(" ", "_", entity_type)
                                                if st.button('Generate DDL'):
                                                    ddl_statements = []
                                                    for entity_name in selected_entities:
                                                        ent_name = re.sub("(.*?) RETURN.*", "\\1", entity_name)
                                                        ddl_query = f"SELECT GET_DDL('{ent_type}', '{db_name}.{sch_name}.{ent_name}', true) AS DDL"
                                                        df = cursor.execute(ddl_query)
                                                        ddl_statements.append(df.fetchone()[0])

                                                    combined_ddl = "\n\n-------------------------------------------------------------------------------------------\n\n".join(ddl_statements)
                                                    st.write("### Generate DDL")
                                                    language = "PYTHON" if "python" in combined_ddl.lower() else "SQL"
                                                    st.code(combined_ddl, language=language)
                    if conn and chk_box=='YES':
                                st.info('Connected to Snowflake!')
                                db_list = cursor.execute("SHOW DATABASES")
                                db_names = [db[1] for db in db_list]

                                db_name = st.selectbox("Select Database", db_names, key=f"selected_dbnames")

                                if db_name and st.button('Generate DDL'):
                                     ddl_statements = []
                                     ddl_query = f"SELECT GET_DDL('DATABASE', '{db_name}', true) AS DDL"
                                     df = cursor.execute(ddl_query)
                                     ddl_statements.append(df.fetchone()[0])
                                     combined_ddl = "\n\n-------------------------------------------------------------------------------------------\n\n".join(ddl_statements)
                                     st.write("### Generate DDL")
                                     language = "PYTHON" if "python" in combined_ddl.lower() else "SQL"
                                     st.code(combined_ddl, language=language)        
                            # Close Snowflake cursor and connection
                    cursor.close()
                                    
                                
            if __name__ == '__main__':
                main()                
               
with tab5:
            def main():
                    st.title(":balloon: :balloon: Replicate databse from one snowflake to another :balloon: :balloon:")
                    def test_connection(account, role, warehouse, database, schema, user, password):
                        try:
                            conn = create_snowflake_connection(account, role, warehouse, database, schema, user, password)
                            if conn:
                                st.success("Connection successful!")
                                return conn
                            else:
                                st.error("Connection failed. Please check your credentials.")
                                return None
                        except Exception as e:
                            st.error(f"Error testing connection: {str(e)}")
                            return None

                    col1, col2 = st.columns(2)
                    with col1:
                            st.header("Source Snowflake Credentials")
                            source_account = st.text_input("Source Account")
                            source_role = st.text_input("Source Role")
                            source_warehouse = st.text_input("Source Warehouse")
                            source_database = st.text_input("Source Database")
                            source_schema = st.text_input("Source Schema")
                            source_user = st.text_input("Source User")
                            source_password = st.text_input("Source Password", type="password")

                            if st.button("Test Source Connection"):
                                source_conn = test_connection(source_account, source_role, source_warehouse, source_database, source_schema, source_user, source_password)
                                if source_conn:
                                    st.toast("Source Connection to Snowflake successfully!", icon='🎉')
                                    time.sleep(0.5)
                                    st.balloons()
                                    # Generate DDL for source database
                                    cursor = source_conn.cursor()
                                    ddl = []
                                    ddl_q = f"SELECT GET_DDL('DATABASE', '{source_database}', true) AS DDL"
                                    df_q = cursor.execute(ddl_q)
                                    ddl.append(df_q.fetchone()[0])
                                    src_combined_ddl = "\n\n-------------------------------------------------------------------------------------------\n\n".join(ddl)
                                    st.write("### Generate DDL")
                                    language = "PYTHON" if "python" in combined_ddl.lower() else "SQL"
                                    st.code(src_combined_ddl, language=language)    

                    with col2:
                            st.header("Destination Snowflake Credentials")
                            dest_account = st.text_input("Destination Account")
                            dest_role = st.text_input("Destination Role")
                            dest_warehouse = st.text_input("Destination Warehouse")
                            dest_database = st.text_input("Destination Database")
                            dest_schema = st.text_input("Destination Schema")
                            dest_user = st.text_input("Destination User")
                            dest_password = st.text_input("Destination Password", type="password")

                            if st.button("Test Destination Connection"):
                                dest_conn = test_connection(dest_account, dest_role, dest_warehouse, dest_database, dest_schema, dest_user, dest_password)
                                if dest_conn:
                                    st.toast("Destination Connection to Snowflake successfully!", icon='🎉')
                                    time.sleep(0.5)
                                    cursor = dest_conn.cursor()
                                    ddl = []
                                    ddl_q = f"SELECT CURRENT_ACCOUNT() CURRENT_ACCOUNT"
                                    ddl_q = f"{src_combined_ddl}"
                                    df_q = cursor.execute(ddl_q)
                                    ddl.append(df_q.fetchone()[0])
                                    combined_ddl = "\n\n-------------------------------------------------------------------------------------------\n\n".join(ddl)
                                    st.write("### Generate DDL")
                                    language = "PYTHON" if "python" in combined_ddl.lower() else "SQL"
                                    st.code(combined_ddl, language=language)
            
            
            if __name__ == '__main__':
                main()                
                
# Adding a footer
footer="""<style>
a:link , a:visited{
color: blue;
background-color: transparent;
text-decoration: underline;
}

a:hover,  a:active {
color: red;
background-color: transparent;
text-decoration: underline;
}

.footer {
position: fixed;
left: 0;
bottom: 0;
width: 100%;
background-color: white;
color: black;
text-align: center;
}
</style>
<div class="footer">
<p>Developed with ❤️ by <a style='display: inline; text-align: center;' href="https://bit.ly/atozaboutdata" target="_blank">MAHANTESH HIREMATH</a></p>
</div>
"""
st.markdown(footer,unsafe_allow_html=True)
