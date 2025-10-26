import copy
import json
import os
import sqlite3
import time
import traceback
from typing import Optional, List, Tuple, Dict, Any

import pandas as pd
from utilities import ProcessInfo, ProcessesInfo
import streamlit as st
import uuid
import yaml
from langsmith.wrappers import wrap_openai
from openai import OpenAI

from config import (
    ERROR_LOGS, OPENAI_API_KEY, SQL_EXAMPLES,
    Models, OpenAIModelsTemperatures,
    Database
)
from keyword_extraction import convert_sql_functions_to_openai_tools
from prompts import TABLE_IDENTIFIER_PROMPT, SQL_QUERY_GENERATION_PROMPT, TABLE_DETAILS_PATTERN_IN_SQL_QUERY_GENERATION_SYSTEM_PROMPT, TABLE_DETAILS_SEPARATOR_IN_SQL_QUERY_GENERATION_SYSTEM_PROMPT
from tool_creation import FunctionToOpenaiToolConverter
from web_app_utilities import parse_result_string


openai_client = OpenAI(api_key=OPENAI_API_KEY)
openai_client = wrap_openai(openai_client)


class YamlDumper(yaml.Dumper):
    def represent_scalar(self, tag, value, style=None):
        if isinstance(value, str) and "\n" in value:
            style = "|"
        return super().represent_scalar(tag, value, style)    

def execute_sql_query(sql_query: str, query_explanation: str, stop: bool = False) -> pd.DataFrame:
    """
    Executes a SQL query on an SQLite database and returns the result as a DataFrame.

    Parameters
    ----------
    sql_query : str
        The SQL query to be executed.
    query_explanation : str
        A description explaining the purpose of the SQL query.
    stop : bool, optional
        A flag to stop the execution of the function. Defaults to False.
        If True, it indicates the last iteration.

    Returns
    -------
    pd.DataFrame
        A Pandas DataFrame containing the query results.
        Returns an empty DataFrame if an error occurs or if there are no results.

    Raises
    ------
    Exception
        If there is an error in executing the SQL query, an error message is printed,
        and an empty DataFrame is returned.

    Examples
    --------
    >>> query = "SELECT * FROM users WHERE age > 30"
    >>> explanation = "Retrieve users older than 30"
    >>> execute_sql_query(query, explanation)
         id    name    age
    0    1  Alice     34
    1    2    Bob     45
    """
    db_file = Database.ANALYTICAL_DATA
    
    try:
        with sqlite3.connect(db_file) as connection:  
            cursor = connection.cursor()
            
            cursor.execute(sql_query)
            result = cursor.fetchall()
            
            # Ensure cursor.description exists (handles empty results)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Convert to DataFrame
            result_df = pd.DataFrame(result, columns=columns)

            return result_df

    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return pd.DataFrame()  
    
def get_table_comments_of_rel_tables(rel_tables: List[str]) -> Dict[str, str]:
    """Fetches table descriptions for the given related tables."""
    
    db_path = Database.METADATA
    table_comments: Dict[str, str] = {}

    if not rel_tables:  # Handle empty list case early
        return table_comments

    query = """
        SELECT DISTINCT "table_name", "table_description" 
        FROM "market_tables"
        WHERE "table_name" IN ({})
    """.format(",".join("?" * len(rel_tables)))  

    try:
        with sqlite3.connect(db_path) as connection:  # Auto-closes connection
            cursor = connection.cursor()
            cursor.execute(query, rel_tables)
            table_comments = {table_name: table_description for table_name, table_description in cursor.fetchall()}
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    
    return table_comments

def get_table_info(market: str) -> Dict[str, Dict[str, str]]:
    """Fetch table schemas and comments for a given market."""

    db_file = Database.METADATA
    table_info: Dict[str, Dict[str, str]] = {}

    query = """
        SELECT "table_name", "table_schema", "table_description"
        FROM "market_tables"
        WHERE "market" = ?
    """

    try:
        with sqlite3.connect(db_file) as connection:
            cursor = connection.cursor()
            cursor.execute(query, (market,))
            rows = cursor.fetchall()

            for table_name, table_schema, table_description in rows:
                table_info[table_name] = {
                    f"Schema for {table_name} table": table_schema,
                    f"Summary description for {table_name} table": table_description or "No comment available."
                }

    except sqlite3.Error as e:
        print(f"Database error: {e}")

    return table_info

def get_relevant_tables(user_input: str, market: str, processes_info: Optional[ProcessesInfo] = None):
    if processes_info:
        get_relevant_tables_id = uuid.uuid4()
        processes_info.processes[get_relevant_tables_id] = ProcessInfo(
                heading="Retrieving Relevant Tables from the Relational Database Based on the Question"
            )
    formatted_SQL_prompt = copy.deepcopy(TABLE_IDENTIFIER_PROMPT)
    table_info = get_table_info(market)

    formatted_SQL_prompt[1]['content'] = formatted_SQL_prompt[1]['content'].format(user_input=user_input,
                                                                                   table_info=table_info)
    response = openai_client.chat.completions.create(
        model = Models.OPENAI_GPT_4O_MINI,
        messages = formatted_SQL_prompt,
        response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "relevant_tables",
            "schema": {
                "type": "object",
                "properties": {
                    "relevant_tables": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": ["relevant_tables"],
                "additionalProperties": False
            },
            "strict": True
         }
        },

        temperature = OpenAIModelsTemperatures.OPENAI_GPT_4O_TEMPERATURE,
    )

    answer = response.choices[0].message.content

    token_usage = response.usage
    cost = ((token_usage.completion_tokens/1000000)*15 + (token_usage.prompt_tokens/1000000)*5)*83.52
    if processes_info:
        processes_info.processes[get_relevant_tables_id].status = "✅"

    return answer, token_usage, cost

def get_table_schemas(table_names: List[str], processes_info: Optional[ProcessesInfo] = None) -> Tuple[Dict[str, str], str]:
    if processes_info:
        get_table_schemas_id = uuid.uuid4()
        processes_info.processes[get_table_schemas_id] = ProcessInfo(
                heading="Fetching Table Schemas for the Retrieved Relevant Tables"
            )

    db_file = Database.METADATA
    all_schemas = {}

    # Use a context manager for safe connection handling
    with sqlite3.connect(db_file) as connection:
        cursor = connection.cursor()
        
        query = """
            SELECT "table_schema"
            FROM "market_tables"
            WHERE "table_name" = ?
        """
        
        for table_name in table_names:
            try:
                cursor.execute(query, (table_name,))
                result = cursor.fetchone()
                if result:
                    all_schemas[table_name] = result[0]
                else:
                    print(f"No schema found for table {table_name}")
            except sqlite3.OperationalError as e:
                print(f"Error fetching schema for table {table_name}: {e}")

    # Generate formatted schema output
    formatted_schemas = "\n\n".join(
        f"Schema for '{table_name}' table:\n{table_schema}" for table_name, table_schema in all_schemas.items()
    )
    if processes_info:
        processes_info.processes[get_table_schemas_id].status = "✅"

    return all_schemas, formatted_schemas

def get_relevant_examples(sql_examples_path: str, target_relevant_tables: List[str]) -> List[Dict[str, Any]]:
    """
    Filters SQL examples from a YAML file based on relevant tables.

    Parameters
    ----------
    sql_examples_path : str
        Path to the YAML file containing SQL examples.
    target_relevant_tables : List[str]
        List of table names considered relevant.

    Returns
    -------
    List[Dict[str, Any]]
        A list of filtered SQL examples (dictionaries), excluding the 'relevant_tables' field,
        or an empty list if no matching examples are found.
    """
    try:
        with open(sql_examples_path, encoding="utf-8") as file:
            examples = yaml.safe_load(file) or []  # Handle empty or None YAML file
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"Error loading YAML file {sql_examples_path}: {e}")
        return []

    target_tables_set = set(target_relevant_tables)
    filtered_examples = []

    for example in examples:
        example_tables_set = set(example.get("relevant_tables", []))

        if target_tables_set >= example_tables_set:  # Check if all example tables are in target tables
            modified_example = {k: v for k, v in example.items() if k != "relevant_tables"}
            filtered_examples.append(modified_example)

    return filtered_examples

def format_sql_queries_and_results(all_results: List[List[Dict[str, Any]]]) -> str:
    """
    Formats SQL queries and their results into a structured string for use in a prompt.

    Parameters
    ----------
    all_results : List[List[Dict[str, Any]]]
        A list where each element is a list containing dictionaries.
        Each dictionary represents an SQL query and its corresponding result with keys:
        - 'sql_query' : str
            The SQL query.
        - 'result' : str
            The result of executing the query (string representation of a DataFrame).

    Returns
    -------
    str
        A formatted string containing SQL queries followed by their results.
    """
    formatted_output: List[str] = []

    for iteration_idx, iteration_results in enumerate(all_results, start=1):
        for query_idx, entry in enumerate(iteration_results, start=1):
            sql_query: str = entry.get("sql_query", "No SQL query available.")
            result: str = entry.get("result", "No result available.")

            formatted_output.append(f"SQL Query:\n{sql_query}\n")
            formatted_output.append(f"Result:\n{result}\n")

    return "\n".join(formatted_output)

def sql_queries_and_results_for_db(all_results: List[List[Dict[str, Any]]]) -> Tuple[str, str]:

    if all_results:
        # Extract SQL queries and results separately while maintaining iteration order
        sql_queries = [[entry["sql_query"] for entry in iteration] for iteration in all_results]
        sql_results = [[entry["result"] for entry in iteration] for iteration in all_results]

    else:
        sql_queries = []    
        sql_results = []

    # Convert list of lists to JSON str
    sql_queries_json = json.dumps(sql_queries, ensure_ascii=False)
    sql_results_json = json.dumps(sql_results, ensure_ascii=False)
    return sql_queries_json, sql_results_json

@st.fragment
def display_sql_results(sql_queries_json: str, sql_results_json: str) -> None:
    """Display SQL queries and their results in Streamlit."""
    sql_queries = json.loads(sql_queries_json) if sql_queries_json else []
    sql_results = json.loads(sql_results_json) if sql_results_json else []

    

    for iteration_idx, (queries, results) in enumerate(zip(sql_queries, sql_results), start=1):
        st.subheader(f"Iteration {iteration_idx}")  # Main iteration header

        for query_idx, (sql_query, sql_result) in enumerate(zip(queries, results), start=1):
            st.markdown(f"**{iteration_idx}.{query_idx}  SQL Query:**")
            st.code(sql_query, language="sql")

            # Convert result string back to DataFrame
            result_df = parse_result_string(sql_result)

            st.markdown(f"**{iteration_idx}.{query_idx}  Result:**")
            st.dataframe(result_df)  # Display as a Streamlit DataFrame



def generate_sql_query_multi_tables(
    user_input: str, 
    keywords: List[Dict[str, Any]], 
    market_type: str, 
    rel_tables: List[str]
) -> Tuple[List[List[Dict[str, Any]]], float, float, float]:

    stop = False
    sql_query_generation_time = 0
    sql_query_execution_time = 0  # Track only execution time
    query_gen_timer_start = time.time()  # Start generation timer
    
    # Fetch table schemas, descriptions, and examples
    table_schemas, _ = get_table_schemas(table_names=rel_tables, processes_info=None)
    table_descriptions = get_table_comments_of_rel_tables(rel_tables)
    examples = get_relevant_examples(SQL_EXAMPLES, rel_tables)
    yaml_examples = yaml.dump(examples, indent=4, Dumper=YamlDumper, default_flow_style=False, sort_keys=False)

    # Combine table schema & descriptions
    table_details = TABLE_DETAILS_SEPARATOR_IN_SQL_QUERY_GENERATION_SYSTEM_PROMPT.join([
        TABLE_DETAILS_PATTERN_IN_SQL_QUERY_GENERATION_SYSTEM_PROMPT.format(
            table_name=table,
            table_description=table_descriptions.get(table, "No description available."),
            table_schema=table_schemas.get(table, "No schema available.")
        )
        for table in rel_tables
    ])
    

    # Format SQL prompt
    formatted_SQL_prompt = copy.deepcopy(SQL_QUERY_GENERATION_PROMPT)
    params_of_execute_sql_query_function = FunctionToOpenaiToolConverter(execute_sql_query).function_signature.parameters
    formatted_SQL_prompt[0]['content'] = formatted_SQL_prompt[0]['content'].format(
        tool_name=execute_sql_query.__name__,
        param_sql_query=params_of_execute_sql_query_function[0].name,
        param_sql_query_explanation=params_of_execute_sql_query_function[1].name,
        param_stop_flag=params_of_execute_sql_query_function[2].name,
        sql_db_type="SQLITE3",
        sql_entity_wrapper="DOUBLE QUOTES",
        examples=yaml_examples,
        table_descriptions_and_schemas=table_details,
    )
    formatted_SQL_prompt[1]['content'] = formatted_SQL_prompt[1]['content'].format(
        question=user_input,
        keywords=json.dumps(keywords, indent=4)
    )
    functions = [execute_sql_query]
    tool_schemas = convert_sql_functions_to_openai_tools(functions) 
    all_results = []  # Store results as list of lists (per iteration)
    all_results_db = []
    for count in range(1, 6):  # Max 5 iterations
        if stop:
            break

        print(f"\n\nIteration {count} started")
        
        response = openai_client.chat.completions.create(
            model=Models.OPENAI_GPT_4O,
            messages=formatted_SQL_prompt,
            temperature=0,
            tools=tool_schemas,
            tool_choice="required"
        )
        
        # Stop the generation timer just before execution
        query_gen_timer_stop = time.time()
        gen_time = query_gen_timer_stop - query_gen_timer_start
        sql_query_generation_time += gen_time  # Accumulate total generation time

        tool_calls = response.choices[0].message.tool_calls
        # print(f"sql_tool_calls_llm_output: {tool_calls}")

        iteration_results = []
        iteration_results_db = []
        iteration_stop_signals = []

        for tool_call in tool_calls:
            arguments = json.loads(tool_call.function.arguments)
            function_name = tool_call.function.name
            
            # Start execution timer
            execution_start_time = time.time()
            result = eval(f"{function_name}(**{arguments})")  # Execute function
            execution_end_time = time.time()
            
            # Track execution time
            execution_time = execution_end_time - execution_start_time
            sql_query_execution_time += execution_time  

            # Restart generation timer for next iteration
            query_gen_timer_start = time.time()

            # Ensure result is serializable
            serializable_result_db = result.to_json(orient="records") if isinstance(result, pd.DataFrame) else json.dumps(result)
            serializable_result = format_dataframe(result) if isinstance(result, pd.DataFrame) and not result.empty else ""

            # print(f"SQL Query:\n{arguments['sql_query']}\nResult:\n{serializable_result}")

            # Store query-result pair for the iteration
            iteration_results_db.append({
                "sql_query": arguments['sql_query'],
                "result": serializable_result_db
            })
            iteration_results.append({
                "sql_query": arguments['sql_query'],
                "result": serializable_result
            })

            # Check stop signal
            iteration_stop_signals.append(arguments.get('stop') == True)

        # Append iteration results to the final result list
        all_results_db.append(iteration_results_db)
        all_results.append(iteration_results)

        # Stop if any stop signal is received
        if any(iteration_stop_signals):
            stop = True

        # Update assistant's response with iteration results
        formatted_SQL_prompt.append({
            'role': 'assistant',
            'content': json.dumps({'iteration_number': count, 'iteration_results': iteration_results}, indent=4)
        })

        print(f"\n\nIteration {count} ended")

        # Compute cost
        token_usage = response.usage
        cost = ((token_usage.completion_tokens / 1_000_000) * 10 + (token_usage.prompt_tokens / 1_000_000) * 2.5) * 83.52


    # print(f"all_results: {all_results}")

    return all_results_db, all_results, sql_query_generation_time, sql_query_execution_time, cost



def format_dataframe(
        data: pd.DataFrame,
        row_separator: str = "\n",
        col_separator: str = "\t",
        add_headers: bool = True,
        add_indices: bool = False,
) -> str:
    """
    Converts the given tabular data into a single string.

    :param data: Any pandas dataframe.
    :param row_separator: Consecutive rows will be separated by this string.  By default, it is new-line character.
    :param col_separator: Consecutive columns will be separated by this string.  By default, it is tab.
    :param add_headers: If True, adds the column headers as the first row.  By default, it is `True`.
    :param add_indices: If True, adds the corresponding index in the beginning of each row.  By default,
        it is `False`.

    :return: A well-formatted string representation of the tabular data.
    """
    if len(data.columns) == 0:
        return ""

    formatted_rows: list[str] = []

    if add_headers:
        formatted_rows.append(
            (col_separator if add_indices else "") +
            col_separator.join([str(col_name) for col_name in data.columns])
        )

    for index, row in data.iterrows():
        formatted_rows.append(
            ((str(index) + col_separator) if add_indices else "") +
            col_separator.join([str(cell_value) for cell_value in row])
        )

    return row_separator.join(formatted_rows)


def sql_pipeline(user_input: str, keywords: Optional[List[Dict[str, str]]], market_type: str, rel_tables: Optional[List[str]] = None, processes_info: Optional[ProcessesInfo] = None) -> Tuple[List[List[Dict[str, Any]]], float]:
    if processes_info:
        sql_pipeline_id = uuid.uuid4()
        processes_info.processes[sql_pipeline_id] = ProcessInfo(
                heading="Fetching Information from Structured Data",
                content=(
                    "Generating and executing a SQL query to retrieve the desired results from the relational database"
                )
            )
    db_file = Database.ANALYTICAL_DATA
    rel_tables_check = False
    relevant_tables_selection_time = 0
    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()
    error_list = []


    try:
        # If relevant tables are not provided, fetch them
        if not rel_tables:
            print("Fetching relevant tables...")
            timer_start = time.time()
            answer, token_usage, cost = get_relevant_tables(user_input, market=market_type, processes_info=None)
            timer_stop = time.time()
            relevant_tables_selection_time = timer_stop - timer_start
            rel_tables_check = True
            rel_tables = json.loads(answer).get("relevant_tables", [])

        print(f"relevant tables: {rel_tables}")

        # Generate and execute SQL queries
        all_results_db, all_results, sql_query_generation_time, sql_query_execution_time, cost = generate_sql_query_multi_tables(user_input, keywords, market_type, rel_tables)
        return all_results_db, all_results, sql_query_generation_time, sql_query_execution_time, relevant_tables_selection_time, rel_tables_check
        
    except Exception as e:
        error_message = (
            f"User Input: {user_input}\n"
            f"Generated Query and Results: {all_results}\n"
            f"Error: {repr(traceback.format_exc())}\n"
        )
        error_list.append(error_message)
        os.makedirs(ERROR_LOGS, exist_ok=True)

        # Log errors to file
        with open(f"{ERROR_LOGS}/sql_errors.txt", "a", encoding="utf-8") as f:
            f.writelines("\n".join(error_list) + "\n")

        return None, 0
    
    finally:
        cursor.close()
        connection.close()
        if processes_info:
            processes_info.processes[sql_pipeline_id].status = "✅"



        
