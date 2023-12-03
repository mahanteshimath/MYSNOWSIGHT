import streamlit as st
import pandas as pd
import snowflake.connector
import snowflake.snowpark
from snowflake.snowpark.session import Session
from snowflake.connector.pandas_tools import write_pandas
import time
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(
  page_title="MYSNOWSIGHT",
  page_icon="  ",
  layout="wide",
  initial_sidebar_state="expanded",
) 

tab1, tab2 = st.tabs(["PARALLEL EXECUTE", "LOAD FILE"])

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
                # # Create a Snowflake connection function
                # def create_snowflake_connection(account, role, warehouse, database, schema, user, password):
                #     try:
                #         conn = snowflake.connector.connect(
                #             account=account,
                #             role=role,
                #             warehouse=warehouse,
                #             database=database,
                #             schema=schema,
                #             user=user,
                #             password=password,
                #             client_session_keep_alive=True
                #         )
                #         st.success("Connected to Snowflake successfully!")
                #         st.balloons()
                #     except Exception as e:
                #         st.error(f"Error connecting to Snowflake: {str(e)}")    
                #     return conn

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
                    st.markdown(" 👉 [🎥Visit my YouTube channel for more details](https://bit.ly/atozaboutdata)")

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


                # # Adding a footer
                # footer="""<style>
                # a:link , a:visited{
                # color: blue;
                # background-color: transparent;
                # text-decoration: underline;
                # }

                # a:hover,  a:active {
                # color: red;
                # background-color: transparent;
                # text-decoration: underline;
                # }

                # .footer {
                # position: fixed;
                # left: 0;
                # bottom: 0;
                # width: 100%;
                # background-color: white;
                # color: black;
                # text-align: center;
                # }
                # </style>
                # <div class="footer">
                # <p>Developed with ❤️ by <a style='display: inline; text-align: center;' href="https://bit.ly/atozaboutdata" target="_blank">MAHANTESH HIREMATH</a></p>
                # </div>
                # """
                # st.markdown(footer,unsafe_allow_html=True)

with tab2:
            def main():
                st.title('Upload Files to Snowflake')

                # File upload section
                st.header('Upload your file')
                file = st.file_uploader('Upload file', type=['xls', 'xlsx', 'csv', 'txt'])

                if file is not None:
                    # Read the file
                    file_extension = file.name.split('.')[-1]
                    if file_extension.lower() in ['xls', 'xlsx', 'csv', 'txt']:
                        data = pd.read_excel(file) if file_extension.lower() in ['xls', 'xlsx'] else pd.read_csv(file)

                        st.subheader('Preview of Uploaded Data')
                        st.write(data.head())

                        # Save data to Snowflake
                        conn = create_snowflake_connection(account, role, warehouse, database, schema, user, password)
                        if conn:
                            st.info('Connected to Snowflake!')

                            table_name = st.text_input('Enter table name in Snowflake')

                            if st.button('Save to Snowflake'):
                                try:
                                    success, nchunks, nrows, _ = write_pandas(conn, data, table_name)
                                    
                                    st.success(f'Dataloaded to snowflake table: {table_name}  rows : {nrows}')
                                except Exception as e:
                                    st.error(f'Error: {str(e)}')
                        else:
                            st.error('Unable to connect to Snowflake. Please check your credentials.')

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
