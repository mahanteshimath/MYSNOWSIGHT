import streamlit as st
import pandas as pd
import snowflake.connector
import time
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(
  page_title="MYSNOWSIGHT",
  page_icon="  ",
  layout="wide",
  initial_sidebar_state="expanded",
) 

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
        st.success("Connected to Snowflake successfully!")
        st.balloons()
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")    
    return conn

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