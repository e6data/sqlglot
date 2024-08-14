import streamlit as st
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
import sqlglot
import sqlparse
from sqlglot import exp, parse_one


def convert_query(query, from_sql, to_sql):
    converted_query = None
    if query:
        converted_query = sqlglot.transpile(query, read=from_sql, write=to_sql, identify=True)[0]
    return converted_query


def extract_db_and_Table_names(query, from_sql):
    tables_list = []
    if query:
        for table in parse_one(query, from_sql).find_all(exp.Table):
            if table.db:
                tables_list.append(f"{table.db}.{table.name}")
            else:
                tables_list.append(f"{table.name}")
    return tables_list


def process_row(alias, query, from_sql, to_sql):
    converted_query = convert_query(query, from_sql, to_sql)
    list_of_tables = extract_db_and_Table_names(query, from_sql)
    return alias, query, converted_query, list_of_tables


# Setting up Streamlit page
st.set_page_config(page_title="Query Converter", layout="centered", initial_sidebar_state="auto")
st.title("Query Converter with sqlGlot")

# Mode selection
mode = st.selectbox("Select Mode", ["Single Query", "CSV Mode"])

if mode == "Single Query":
    # Initialize chat history in Streamlit session state
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Dropdown for selecting From SQL and To SQL
    from_sql = st.selectbox("From SQL",
                            ["snowflake", "databricks", "athena", "presto", "postgres", "bigquery", "E6", "trino"])
    to_sql = st.selectbox("To SQL",
                          ["snowflake", "databricks", "athena", "presto", "postgres", "bigquery", "E6", "trino"])

    if from_sql and to_sql:
        with st.form("from_sql_query"):
            from_sql_query = st.text_area("From SQL Query")
            submit_button = st.form_submit_button("Submit")
            if submit_button:
                from_sql_query = sqlparse.format(from_sql_query, reindent=True)
                converted_query = convert_query(from_sql_query, from_sql, to_sql)
                converted_query = sqlparse.format(converted_query, reindent=True)
                converted_query = f"```sql \n{converted_query}\n```"
                list_of_tables = extract_db_and_Table_names(from_sql_query, from_sql)
                list_of_tables = f"```list of tables present in the query: \n{list_of_tables}\n```"
                # Append to session state for history tracking
                st.session_state.messages.append({
                    "role": "User",
                    "content": f"From SQL: {from_sql}, To SQL: {to_sql}, Original Query: {from_sql_query}"
                })
                st.session_state.messages.append({
                    "role": "Assistant", "content": f"Response: \n{converted_query}\n\n\n{list_of_tables}\n"
                })
        # # Display chat history
        # for message in st.session_state.messages:
        #     with st.expander(message["role"]):
        #         st.write(message["content"])
        if st.session_state.messages:
            latest_message = st.session_state.messages[-1]
            with st.expander(latest_message["role"]):
                st.write(latest_message["content"])

elif mode == "CSV Mode":
    st.info("Note: The CSV file must contain columns named 'QUERY_TEXT' and 'UNQ_ALIAS'.")
    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

    from_sql = st.selectbox("From SQL",
                            ["snowflake", "databricks", "athena", "presto", "postgres", "bigquery", "E6", "trino"],
                            key="csv_from_sql")
    to_sql = st.selectbox("To SQL",
                          ["snowflake", "databricks", "athena", "presto", "postgres", "bigquery", "E6", "trino"],
                          key="csv_to_sql")

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        queries = df["QUERY_TEXT"].tolist()
        aliases = df["UNQ_ALIAS"].tolist()

        if st.button("Process CSV"):
            start_time = time.time()
            results = []
            batch_size = 5

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                future_to_request = {}

                for i in range(0, len(queries), batch_size):
                    batch_queries = queries[i:i + batch_size]
                    batch_aliases = aliases[i:i + batch_size]

                    for j, (alias, query) in enumerate(zip(batch_aliases, batch_queries)):
                        if j > 0:
                            time.sleep(1)  # delay to avoid server throttling
                        future = executor.submit(process_row, alias, query, from_sql, to_sql)
                        futures.append(future)
                        future_to_request[future] = (alias, query)

                for future in as_completed(futures):
                    alias, original_query = future_to_request[future]
                    try:
                        alias, original_query, converted_query, list_of_tables = future.result()
                        results.append((alias, original_query, converted_query, list_of_tables))
                    except Exception as e:
                        results.append((alias, original_query, str(e),))

            result_df = pd.DataFrame(results,
                                     columns=["UNQ_ALIAS", "Original_Query", "Converted_Queries", "list_of_tables"])
            response_csv = result_df.to_csv(index=False)
            b64 = base64.b64encode(response_csv.encode()).decode()
            href = f'<a href="data:file/csv;base64,{b64}" download="processed_results.csv">Download Processed Results CSV</a>'
            st.markdown(href, unsafe_allow_html=True)

            total_time = time.time() - start_time
            st.write(f"Total time taken for this whole CSV to generate is {total_time:.2f}s")
