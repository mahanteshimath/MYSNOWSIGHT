import streamlit as st
import pandas as pd
import time
from snowflake.connector import connect
from snowflake.connector.connection import SnowflakeConnection
from concurrent.futures import ThreadPoolExecutor



# Function to execute queries in parallel
def execute_queries(queries):
    with ThreadPoolExecutor(max_workers=len(queries)) as executor:
        results = list(executor.map(execute_single_query, queries))
    return results



# Function to execute a single query
def execute_single_query(query):
    connection = connect(**st.secrets["snowflake"], client_session_keep_alive=True)
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
def main():
    st.title("Type queries ; seperated to execute all in paralell")

    # Add link to your YouTube channel
    st.markdown("[Visit my YouTube channe for more details](https://bit.ly/atozaboutdata)")

    # Text area to input queries
    queries = st.text_area("Enter your queries (separated by ;) :", height=200)
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


TTL = 24 * 60 * 60