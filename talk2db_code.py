import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient
import nltk
import os
import random
import re
import json


# Database configuration
db_config = {
    "host": "localhost",
    "database": "demo",
    "user": "root",
    "password": "Abhitya1*"
}

# Create a reusable connection function
def create_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            return connection
    except Error as e:
        st.error(f"Database connection error: {e}")
        return None
    

# MongoDB Configuration
host = "localhost"         
port = 27017               
database_name = "project" 


def create_mongo_connection(host, port, database_name):
    """Create and return a connection to the MongoDB database."""
    try:
        client = MongoClient(host, port)
        db = client[database_name]
        #st.success(f"Connected to MongoDB database '{database_name}' on {host}:{port}")
        return db
    except Exception as e:
        st.error(f"Error connecting to MongoDB: {e}")
        return None



# Function to query data from MySQL database
def get_data_from_db(query):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        
        except Error as e:
            st.error(f"Error executing query: {e}")
            return None
        finally:
            cursor.close()
            connection.close()
    else:
        return None
    
    
def sql_get_table_names_and_columns(connection):
    """Fetch and return all table names and columns as separate lists."""
    if not connection or not connection.is_connected():
        print("No valid connection established.")
        return [], {}, {}

    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0].lower() for table in cursor.fetchall()]

        table_column_mapping = {}
        column_data_types = {}

        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            table_columns = [(column[0].lower(), column[1]) for column in cursor.fetchall()]
            table_column_mapping[table] = [col[0] for col in table_columns]
            column_data_types[table] = {col[0]: col[1] for col in table_columns}

        return tables, table_column_mapping, column_data_types

    except Error as e:
        print(f"Error: {e}")
        return [], {}, {}



def get_table_names_and_columns(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_info = {}

        for table in tables:
            table_name = table[0]
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            column_names = [column[0] for column in columns]
            table_info[table_name] = {"columns": column_names, "details": columns}

        return table_info, tables
    except Error as e:
        st.error(f"Error fetching table names: {e}")
        return {}
    

def get_collection_names_and_fields(database):
    """Fetch and return all collection names and their respective fields from the specified MongoDB database."""
    try:
        collections = database.list_collection_names()
        collection_info = {}

        for collection_name in collections:
            collection = database[collection_name]
            sample_document = collection.find_one()  # Get a sample document
            if sample_document:
                collection_info[collection_name] = {
                    'fields': list(sample_document.keys())
                }
            else:
                collection_info[collection_name] = {
                    'fields': []
                }

        return collection_info
    except Exception as e:
        st.error(f"Error fetching collection names or fields: {e}")
        return {}
    

def sample_get_collection_names_and_fields(database):
    """
    Fetch, print, and return all collection names, their respective fields, and the data types of those fields
    from the specified MongoDB database.
    """
    try:
        collections = database.list_collection_names()
        collection_info = {}

        for collection_name in collections:
            collection = database[collection_name]
            sample_document = collection.find_one()  # Get a sample document

            if sample_document:
                fields_with_types = {
                    field: type(value).__name__  # Get the type name as a string
                    for field, value in sample_document.items()
                }
                collection_info[collection_name] = {
                    'fields': fields_with_types
                }

                # Print field details
                #print("Fields and Data Types:")
                #for field, data_type in fields_with_types.items():
                    #print(f"  - {field}: {data_type}")
            else:
                collection_info[collection_name] = {
                    'fields': {}
                }
                #print("  No documents found in this collection.")

            #print("-" * 50)  # Separator for readability

        return collection_info
    except Exception as e:
        st.error(f"Error fetching collection names or fields: {e}")
        return {}


def generate_dynamic_queries(database_details, base_queries):
    """
    Generate queries by dynamically replacing placeholders with collection names and field names,
    ensuring specific field replacements based on context.
    
    Args:
        database_details (dict): Details of the database, including collections and field types.
        base_queries (list): List of base queries with placeholders like "collection_name" and "field_name". 
    
    Returns:
        dict: A dictionary of generated queries for each collection.
    """
    preferred_fields = {"price", "amount", "tuition", "quantity", "amt", "money"}  # Fields to prioritize for aggregate operations
    generated_queries = {}

    for collection_name, details in database_details.items():
        fields = list(details['fields'].keys())
        # Filter out fields with 'id' in their name
        fields = [field for field in fields if "id" not in field.lower()]
        numeric_fields = [field for field in fields if details['fields'][field] in ('int', 'float', 'decimal')]
        non_numeric_fields = [field for field in fields if details['fields'][field] not in ('int', 'float', 'decimal')]
        non_preferred_fields = [field for field in fields if field not in preferred_fields]

        collection_queries = set()

        for query in base_queries:
            random_numeric_value = random.randint(1, 20)
            query_with_collection = query.replace("collection_name", collection_name)
            field_placeholders = query_with_collection.count("field_name")
            if field_placeholders == 2:
                # Handle queries with two `field_name` placeholders
                if not numeric_fields:
                    # Skip the query if there are no numeric fields in the collection
                    continue

                replaced_query = query_with_collection
                if "numeric_value" in replaced_query:
                    replaced_query = replaced_query.replace("numeric_value", str(random_numeric_value))

                used_fields = set()

                # First `field_name` for aggregate operations
                first_field_replaced = False
                for field in numeric_fields:
                    if field in preferred_fields and field not in used_fields:
                        replaced_query = replaced_query.replace("field_name", field, 1)
                        used_fields.add(field)
                        first_field_replaced = True
                        
                if not first_field_replaced:
                    # If no preferred numeric field was found, skip this query
                    continue

                # Second `field_name` for other fields
                for field in non_numeric_fields:
                    if field not in preferred_fields and field not in used_fields:
                        replaced_query = replaced_query.replace("field_name", field, 1)
                        used_fields.add(field)

                collection_queries.add(replaced_query)

            elif field_placeholders == 1:
                # Handle queries with one `field_name`
                replaced_query = query_with_collection
                if "numeric_value" in replaced_query:
                    replaced_query = replaced_query.replace("numeric_value", str(random_numeric_value))

                if any(keyword in query.lower() for keyword in ["maximum", "minimum", "average", "total", "greater", "less", "not"]):
                    # Use only numeric fields for these aggregate terms
                    for field in numeric_fields:
                        replaced_query = replaced_query.replace("field_name", field, 1)
                        collection_queries.add(replaced_query)

                else:
                    # Replace with any field if no numeric constraint
                    for field in fields:
                        replaced_query = replaced_query.replace("field_name", field, 1)
                        collection_queries.add(replaced_query)

            else:
                # Directly append the query if no `field_name` placeholders exist
                replaced_query = query_with_collection
                if "numeric_value" in replaced_query:
                    replaced_query = replaced_query.replace("numeric_value", str(random_numeric_value))
                collection_queries.add(replaced_query)

        generated_queries[collection_name] = collection_queries

    return generated_queries



def generate_query_from_input(user_input, table_info):
    tokens = nltk.word_tokenize(user_input.lower())
    for table, table_data in table_info.items():
        columns = table_data["columns"]

        if re.search(table.lower(), user_input.lower()):
            if re.search(r"total|sum|sales", user_input, re.IGNORECASE):
                column = random.choice(columns)
                return f"SELECT {column}, SUM({column}) FROM {table} GROUP BY {column}"
            elif re.search(r"average|avg", user_input, re.IGNORECASE):
                column = random.choice(columns)
                return f"SELECT {column}, AVG({column}) FROM {table}"
            elif re.search(r"order by|sort", user_input, re.IGNORECASE):
                column = random.choice(columns)
                return f"SELECT * FROM {table} ORDER BY {column} DESC LIMIT 5"
            elif re.search(r"where", user_input, re.IGNORECASE):
                column = random.choice(columns)
                value = random.randint(1, 100)
                return f"SELECT * FROM {table} WHERE {column} = {value}"
            else:
                return f"SELECT * FROM {table} LIMIT 5"
    return None


def generate_sample_query(user_input, collection_info):

    base_queries = [
        "collection_name sorted by field_name",
        "collection_name sorted by field_name in descending order",
        #"total field_name per field_name in collection_name",
        "the minimum field_name spent by each field_name in collection_name",
        "the maximum field_name spent by field_name in collection_name",
        "the total field_name spent by each field_name in collection_name",
        "the average field_name spent by each field_name in collection_name",
        "the number of collection_name where field_name is greater than numeric_value",
        "the number of collection_name where field_name is less than numeric_value",
        "the number of collection_name where field_name is greater than or equal to numeric_value",
        "the number of collection_name where field_name is less than or equal to numeric_value",
        "the number of collection_name where field_name is not numeric_value",
        "count the number of collection_name",
        "collection_name where field_name is greater than or equal to numeric_value",
        "collection_name where field_name is less than or equal to numeric_value",
        "collection_name where field_name is greater than numeric_value",
        "collection_name where field_name is less than numeric_value",
        "collection_name where field_name is not numeric_value",
        "whats the maximum field_name in collection_name?",
        "whats the minimum field_name in collection_name?",
        "whats the average field_name in collection_name?",
        "whats the total field_name in collection_name?",
        "display collection_name"
    ]
    
    base_queries_group_by_agg = [
        #"total field_name per field_name in collection_name",
        "the minimum field_name spent by each field_name in collection_name",
        "the maximum field_name spent by field_name in collection_name",
        "the total field_name spent by each field_name in collection_name",
        "the average field_name spent by each field_name in collection_name"
    ]
    base_queries_order_by = [
        "collection_name sorted by field_name",
        "collection_name sorted by field_name in descending order"
    ]
    base_queries_count_documents = [
        "count the number of collection_name"
    ]
    base_queries_agg = [
        "whats the maximum field_name in collection_name?",
        "whats the minimum field_name in collection_name?",
        "whats the average field_name in collection_name?",
        "whats the total field_name in collection_name?"
    ]
    base_queries_where = [
        "collection_name where field_name is greater than or equal to numeric_value",
        "collection_name where field_name is less than or equal to numeric_value",
        "collection_name where field_name is greater than numeric_value",
        "collection_name where field_name is less than numeric_value",
        "collection_name where field_name is not numeric_value",
    ]
    base_queries_count_where = [
        "the number of collection_name where field_name is greater than numeric_value",
        "the number of collection_name where field_name is less than numeric_value",
        "the number of collection_name where field_name is greater than or equal to numeric_value",
        "the number of collection_name where field_name is less than or equal to numeric_value",
        "the number of collection_name where field_name is not numeric_value "
    ]

    tokens = user_input.lower().split(" ")
    if "example" in tokens or "sample" in tokens:
        for token in tokens:
            if token in collection_info:
                collection_name = token
        if "with" in tokens:
            if "group" in tokens:
                sample_queries = generate_dynamic_queries(collection_info, base_queries_group_by_agg)
            elif "order" in tokens:
                sample_queries = generate_dynamic_queries(collection_info, base_queries_order_by)
            elif "count" in tokens and "where" in tokens:
                sample_queries = generate_dynamic_queries(collection_info, base_queries_count_where)
            elif "where" in tokens:
                sample_queries = generate_dynamic_queries(collection_info, base_queries_where)
            elif "agg" in tokens or "aggregate" in tokens or "aggregation" in tokens:
                sample_queries = generate_dynamic_queries(collection_info, base_queries_agg)
            elif "count" in tokens:
                sample_queries = generate_dynamic_queries(collection_info, base_queries_count_documents)
        else:
            sample_queries = generate_dynamic_queries(collection_info, base_queries)
            #st.write("checkpoint 4")
            #st.write(f"{sample_queries}")
        collection_queries = sample_queries[collection_name]
        collection_queries = list(collection_queries)
        if "queries" in tokens:
            if len(collection_queries) >= 2:
                random_queries = random.sample(collection_queries, 2)
                #print(f"Two random queries for collection '{collection_name}':")
                return random_queries
                # for query in random_queries:
                #     st.markdown("<br>", unsafe_allow_html=True)
                #     st.write(f"{query}")
                #     st.markdown("<br>", unsafe_allow_html=True)
                #     handle_query(query, collection_info, database)
            else:
                random_queries = random.sample(collection_queries, 1)
                #print(f"Random query for collection '{collection_name}':")
                return random_queries
                # for query in random_queries:
                #     st.markdown("<br>", unsafe_allow_html=True)
                #     st.write(f"{query}")
                #     st.markdown("<br>", unsafe_allow_html=True)
                #     handle_query(query, collection_info, database)
        elif "query" in tokens:
            random_queries = random.sample(collection_queries, 1)
            #print(f"Random query for collection '{collection_name}':")
            return random_queries
            # for query in random_queries:
            #     st.markdown("<br>", unsafe_allow_html=True)
            #     st.write(f"{query}")
            #     st.markdown("<br>", unsafe_allow_html=True)
            #     handle_query(query, collection_info, database)
    # Process the user input to generate and execute a MongoDB query
    else:
        st.error(f"Sorry, I couldn't understand your query.")



def extract_table_and_column_with_condition(user_input, tables, table_column_mapping):
    """Extract table, columns, and conditions from the user input."""
    tokens = nltk.word_tokenize(user_input.lower())
    #st.write(f"Tokens: {tokens}")  # Debug: Show tokens

    table_name = None
    columns = []
    conditions = {}

    # Match tokens against table names
    for token in tokens:
        if token in tables:
            table_name = token
            break

    if table_name:
        # Match tokens against column names within the identified table
        for token in tokens:
            if token in table_column_mapping[table_name]:
                columns.append(token)

        # Parse WHERE clause if present
        if "where" in tokens:
            where_index = tokens.index("where")
            condition_tokens = tokens[where_index + 1:]

            # Map natural language operators to SQL syntax
            operator_mapping = {
                "is": "=",
                "equals": "=",
                "greater": ">",
                "less": "<",
                "not": "!=",
                "greater than": ">",
                "less than": "<",
            }

            if len(condition_tokens) >= 3:  # Expecting column, operator, value
                column = condition_tokens[0]

                # Identify multi-word operators
                potential_operator = " ".join(condition_tokens[1:3])
                operator = operator_mapping.get(potential_operator, None)

                if not operator:  # Fallback to single-word operator
                    operator = operator_mapping.get(condition_tokens[1], None)
                    value_start_index = 2
                else:
                    value_start_index = 3

                value = " ".join(condition_tokens[value_start_index:])

                # Ensure column exists in the table
                if column in table_column_mapping[table_name]:
                    conditions[column] = (operator, value)

    if not table_name:
        st.error("Error: No valid table found in the input.")
    # elif not columns:
    #     st.error(f"Error: No valid column found for table '{table_name}' in the input.")
    elif "where" in tokens and not conditions:
        st.error("Error: WHERE clause is incomplete or invalid.")

    return table_name, columns, conditions



def match_query_pattern(user_input, collection_info):
    """Match user input to predefined query patterns using NLTK tokenization."""
    if isinstance(user_input, dict):
        user_input = str(user_input)

    tokens = nltk.word_tokenize(user_input.lower())
    tokens = [token for token in tokens if token != '?']
    #st.write(f"Tokens: {tokens}")
    #st.write(f"Collection_info: {collection_info}")
    #st.write(f"checkpoint 6")
    collection_names = []
    for token in tokens:
        if token in collection_info:
            collection_names.append(token)
    # If there are two collections, proceed with join query logic
    if len(collection_names) == 2:
        collection_1 = collection_names[0]
        collection_2 = collection_names[1]

        fields_1 = collection_info[collection_1]['fields']
        fields_2 = collection_info[collection_2]['fields']

        # Find matching fields (for join)
        join_fields = [(field_1, field_2) for field_1 in fields_1 for field_2 in fields_2 if field_1 == field_2 and field_1 != "_id" and field_2 != "_id"]

        if join_fields:
            # Assuming we are joining based on matching fields
            local_field, foreign_field = join_fields[0]  # Pick the first matching field pair for simplicity

            # Construct MongoDB join query
            aggregation_query = [
            {
                "$lookup": {
                    "from": collection_2,  # Collection to join with
                    "localField": local_field,  # Local field (from collection_1)
                    "foreignField": foreign_field,  # Foreign field (from collection_2)
                    "as": "joined_data"  # Output field that will contain the joined data
                }
            },
            {
                "$project": {
                    "_id": 0,  # Exclude _id from the output
                    "joined_data._id": 0  # Exclude _id from the joined data array
                }
            }
        ]

            # Check if the query contains a 'where' condition
            if 'where' in tokens:
                condition_index = tokens.index("where")
                if condition_index + 3 <= len(tokens):  # Ensure there's enough content after "where"
                    collection_name = None
                    field_name = None
                    condition = None

                    # Example: "Show customers where the name is John"
                    #if tokens[0] in ["show", "list", "give", "fetch"]:
                        #collection_name = tokens[1]  # Assume the second token is the collection name
                    for token in tokens:
                        if token in collection_info:
                            collection_name = token
                    if collection_name in collection_info:
                            # Extract the field name (first word after 'where')
                        field_name = tokens[condition_index + 1]

                            # Extract condition value (last token)
                        condition_value = tokens[-1]

                            # Extract condition operator (all tokens between field and value)
                        condition_operator_tokens = tokens[condition_index + 2 : -1]
                        if len(condition_operator_tokens) > 1:
                                # Remove "is" from the operator if it exists
                            if "is" in condition_operator_tokens:
                                condition_operator_tokens.remove("is")
                        condition_operator = " ".join(condition_operator_tokens).lower()

                            # Function to check if the value is numeric
                        def is_numeric(value):
                            try:
                                float(value)
                                return True
                            except ValueError:
                                return False

                            # Convert operators to MongoDB equivalents
                        if condition_operator in ["is", "equals", "equal to"]:
                                # Handle numeric and string values separately
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                                condition = {"$eq": condition_value}
                            else:
                                    # For string conditions, use regex for case-insensitive matching
                                condition_value = condition_value.lower()  # Ensure the comparison is case-insensitive
                                condition = {"$regex": condition_value, "$options": "i"}
                        elif condition_operator in ["greater than"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$gt": condition_value}
                        elif condition_operator in ["less than"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$lt": condition_value}
                        elif condition_operator in ["not","not equal to", "not equals"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$ne": condition_value}
                        elif condition_operator in ["greater than or equal to"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$gte": condition_value}
                        elif condition_operator in ["less than or equal to"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$lte": condition_value}
                        if field_name and condition:
                            aggregation_query.append({
                                "$match": {
                                    field_name: condition  # Sort by the field with the specified order
                                }
                            })

            # Check if the query contains an 'order by' condition
            if 'order' in tokens or "ordered" in tokens or "sorted" in tokens or "sort" in tokens:
                order_index = tokens.index('ordered') if "ordered" in tokens else tokens.index("sorted") # The field to sort by will be right after 'order'
                #by_index = tokens.index('by') + 1  # The field to sort by will be right after 'by'

                # Extract the sorting field (next token after 'by') and sorting order (e.g., 'asc' or 'desc')
                sort_field = tokens[order_index+2]
                sort_order = 1  # Default to ascending order (1)
                
                # Check if 'desc' or 'asc' is specified for the order
                if 'desc' in tokens or "descending" in tokens:
                    sort_order = -1  # Descending order

                # Add $sort stage to the aggregation pipeline
                aggregation_query.append({
                    "$sort": {
                        sort_field: sort_order  # Sort by the field with the specified order
                    }
                })
            
            return "join_tables", collection_1, aggregation_query

    if any(word in tokens for word in ["ordered", "sorted", "arranged", "order", "sort"]) and any(word in tokens for word in ["per", "each", "group"]) and "where" in tokens:
        # Initialize variables
        collection_name = None
        group_field = None
        condition_field = None
        condition_value = None
        aggregate_field = None
        aggregate_operation = None
        having_condition = None
        sort_field = None
        sort_direction = 1  # Ascending order by default

        # Find the collection name
        for token in tokens:
            if token in collection_info:
                collection_name = token
        #print("\n")
        #print("Collection name:", collection_name)
       

        # Find the field for grouping
        if "per" in tokens or "each" in tokens:
            group_index = tokens.index("per") if "per" in tokens else tokens.index("each") if "each" in tokens else tokens.index("group")
            group_field = tokens[group_index + 1] if len(tokens) > group_index + 1 else None
        #print("Grouping field:", group_field)

        # Find the condition for WHERE (optional)
        if "where" in tokens:
            where_index = tokens.index("where")
            condition_field = tokens[where_index + 1] if len(tokens) > where_index + 1 else None
            #print("condition_field for where:", condition_field)
            # Get the relevant portion of the string for the condition operator
            relevant_section = " ".join(tokens[where_index + 2:where_index + 8])
            #print("Relevant section for condition operator: ", relevant_section)

            # Handle different condition operators for WHERE based on the relevant section
            if "greater than or equal to" in relevant_section or "less than or equal to" in relevant_section:
                condition_operator = " ".join(tokens[where_index + 3:where_index + 8])
                condition_value = tokens[where_index + 8]
                #print("Condition operator for where: ", condition_operator)
                #print("condition value for where:", condition_value)
            elif "greater than" in relevant_section or "less than" in relevant_section:
                condition_operator = " ".join(tokens[where_index + 3:where_index + 5])
                condition_value = tokens[where_index + 5]
                #print("Condition operator for where: ", condition_operator)
                #print("condition value for where:", condition_value)
            elif "not" in relevant_section:
                condition_operator = tokens[where_index + 3]
                condition_value = tokens[where_index + 4]
                #print("Condition operator for where: ", condition_operator)
                #print("condition value for where:", condition_value)
            elif "is" in relevant_section:
                condition_operator = tokens[where_index + 2]
                condition_value = tokens[where_index + 3]
                #print("Condition operator for where: ", condition_operator)
                #print("condition value for where:", condition_value)
            
            # Capture the condition value
            #condition_value = tokens[where_index + 2 + len(condition_operator.split())] if len(tokens) > where_index + len(condition_operator.split()) else None
            #print("condition value for where:", condition_value)
        # Find the field for sorting (optional)
        if "by" in tokens and any(word in tokens for word in ["ordered", "sorted", "arranged", "order", "sort"]):
            sort_index = tokens.index("ordered") if "ordered" in tokens else tokens.index("sorted")
            sort_field = tokens[sort_index + 2] if len(tokens) > sort_index + 2 else None
            #print("sort field:", sort_field)
            if "descending" in tokens or "desc" in tokens:
                sort_direction = -1  # Descending order

        # List of aggregate functions
        aggregate_functions = ["sum", "average", "count", "max", "min", "total", "maximum", "minimum"]

        # Counter to track the number of aggregate functions found
        aggregate_count = 0

        # Find the index of the first and second aggregate function
        for index, token in enumerate(tokens):
            if token.lower() in aggregate_functions:
                aggregate_count += 1
                if aggregate_count == 1:
                    aggregate_operation = token  # First aggregate function
                    #print("aggregate operation in column:", aggregate_operation)
                    aggregate_field = tokens[index + 1]  # Field for aggregation
                    #print("aggregate field for coulm:", aggregate_field)
                elif aggregate_count == 2:  # Second aggregate function (for HAVING)
                    second_aggregate_index = index
                    
                    aggregate_operation_for_having = token
                    #print("aggregate operation for having:", aggregate_operation_for_having)
                    aggregate_field_for_having = tokens[index + 1]  # Field for aggregation (HAVING)
                    #print("aggregate field for having:", aggregate_field_for_having)
                    having_condition = " ".join(tokens[second_aggregate_index + 2:-1])  # HAVING condition
                    #print("having condition: ", having_condition)
                    aggregate_condition_value = tokens[-1]  # Last token is the condition value
                    #print("aggregate condition value: ", aggregate_condition_value)
                    break

        # If HAVING condition is found, build the HAVING query
        if aggregate_count == 2:
            query_params = {
                "group_field": group_field,
                "aggregate": {"operation": aggregate_operation, "field": aggregate_field},
                "having_condition": {
                    "aggregate_operation": aggregate_operation_for_having,
                    "aggregate_field": aggregate_field_for_having,
                    "having_condition": having_condition,
                    "value": aggregate_condition_value
                },
            }
        else:
            # If only one aggregation is found
            query_params = {
                "group_field": group_field,
                "aggregate": {"operation": aggregate_operation, "field": aggregate_field},
            }

        # Add the WHERE condition if present
        if condition_field and condition_value:
            query_params["condition"] = {"field": condition_field, "value": condition_value, "operator": condition_operator}

        # Add sorting if present
        if sort_field:
            query_params["sort"] = {"field": sort_field, "direction": sort_direction}

        return "complex_query", collection_name, query_params

    #query: list orders sorted by amount
    #query: give me orders sorted by customer_name in descending order
    if "by" in tokens and ("order" in tokens or "sort" in tokens or "arrange" in tokens or "ordered" in tokens or "sorted" in tokens or "arranged" in tokens):
        by_index = tokens.index("by")
        if by_index - 1 >= 0:  # Ensure there's a term before "by"
            ordering_keyword = tokens[by_index - 1]
            if ordering_keyword in ["order", "sort", "arrange", "ordered", "sorted", "arranged"]:
                for token in tokens:
                    if token in collection_info:
                        collection_name = token
                #collection_name = tokens[1]  # Assume collection name is the second token
                if collection_name in collection_info:
                    order_field = tokens[by_index + 1] if len(tokens) > by_index + 1 else None
                    # Default to ascending order; check for descending
                    sort_direction = 1  # Ascending
                    if "descending" in tokens or "desc" in tokens:
                        sort_direction = -1
                    return "find_data_with_sorting", collection_name, (order_field, sort_direction)
    #group by with aggregation
    #find total amount per product in orders
    #give me the minimum amount spent by each customer_name in orders
    #find the number of customers per product in orders
    if "each" in tokens or "per" in tokens or "by" in tokens:
        for token in tokens:
            if token in collection_info:
                collection_name = token
        group_field = tokens[-3]

        if collection_name in collection_info:
            aggregation_keywords = {
                "sum": "group_by_sum",
                "total": "group_by_sum",
                "average": "group_by_avg",
                "avg": "group_by_avg",
                "maximum": "group_by_max",
                "max": "group_by_max",
                "minimum": "group_by_min",
                "min": "group_by_min",
                "count": "group_by_count",
                "many": "group_by_count",
                "number": "group_by_count"
            }

            for keyword in aggregation_keywords:
                if keyword in tokens:
                    agg_index = tokens.index(keyword)

                    if keyword in ["sum", "total", "average", "avg", "maximum", "max", "minimum", "min"]:
                        # Check if there's a field after the aggregation function
                        if agg_index + 1 < len(tokens):
                            agg_field = tokens[agg_index + 1]
                            return aggregation_keywords[keyword], collection_name, (group_field, agg_field)
                        else:
                            return "error", "Missing field after aggregation"
                    elif keyword in ["count", "many", "number"]:
                        return aggregation_keywords[keyword], collection_name, group_field
    #query: give me the number of orders where amount is greater than 3
    #query: count the number of orders where customer_name is Alice
    if "count" in tokens or "many" in tokens or "number" in tokens:
        # Assume collection name is the first word after "count"
        if "where" in tokens:
            condition_index = tokens.index("where")
            if condition_index + 3 <= len(tokens):  # Ensure there's enough content after "where"
                collection_name = None
                field_name = None
                condition = None
                for token in tokens:
                    if token in collection_info:
                        collection_name = token
                if collection_name in collection_info:
                        # Extract the field name (first word after 'where')
                        field_name = tokens[condition_index + 1]

                        # Extract condition value (last token)
                        condition_value = tokens[-1]

                        # Extract condition operator (all tokens between field and value)
                        condition_operator_tokens = tokens[condition_index + 2 : -1]
                        if len(condition_operator_tokens) > 1:
                            # Remove "is" from the operator if it exists
                            if "is" in condition_operator_tokens:
                                condition_operator_tokens.remove("is")
                        condition_operator = " ".join(condition_operator_tokens).lower()

                        # Function to check if the value is numeric
                        def is_numeric(value):
                            try:
                                float(value)
                                return True
                            except ValueError:
                                return False

                        # Convert operators to MongoDB equivalents
                        if condition_operator in ["is", "equals", "equal to"]:
                            # Handle numeric and string values separately
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                                condition = {"$eq": condition_value}
                            else:
                                # For string conditions, use regex for case-insensitive matching
                                condition_value = condition_value.lower()  # Ensure the comparison is case-insensitive
                                condition = {"$regex": condition_value, "$options": "i"}
                        elif condition_operator in ["greater than"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$gt": condition_value}
                        elif condition_operator in ["less than"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$lt": condition_value}
                        elif condition_operator in ["not","not equal to", "not equals"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$ne": condition_value}
                        elif condition_operator in ["greater than or equal to"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$gte": condition_value}
                        elif condition_operator in ["less than or equal to"]:
                            if is_numeric(condition_value):
                                condition_value = float(condition_value)
                            condition = {"$lte": condition_value}

                        #print(collection_name)
                        #print(field_name)
                        #print(condition)
                        if field_name and condition:
                            return "count_documents_with_condition", collection_name, (field_name, condition)
        #query: count the number of orders
        #query: give me the number of orders placed
        elif len(tokens) > 1:
            for token in tokens:
                if token in collection_info:
                    return "count_documents", token, None
            else:
                print(f"Collection '{collection_name}' does not exist.")
                return None, None

    #query: show orders where customer_name is Alice
    #query: list orders where amount is greater than or equal to 3
    #query: fetch orders where amount is less than 5
    elif "where" in tokens:
        condition_index = tokens.index("where")
        if condition_index + 3 <= len(tokens):  # Ensure there's enough content after "where"
            collection_name = None
            field_name = None
            condition = None

            # Example: "Show customers where the name is John"
            # if tokens[0] in ["show", "list", "give", "fetch"]:
                #collection_name = tokens[1]  # Assume the second token is the collection name
            for token in tokens:
                if token in collection_info:
                    collection_name = token
            if collection_name in collection_info:
                # Extract the field name (first word after 'where')
                #st.write("checkpoint 1")
                field_name = tokens[condition_index + 1]

                # Extract condition value (last token)
                condition_value = tokens[-1]

                # Extract condition operator (all tokens between field and value)
                condition_operator_tokens = tokens[condition_index + 2 : -1]
                if len(condition_operator_tokens) > 1:
                    # Remove "is" from the operator if it exists
                    if "is" in condition_operator_tokens:
                        condition_operator_tokens.remove("is")
                condition_operator = " ".join(condition_operator_tokens).lower()

                # Function to check if the value is numeric
                def is_numeric(value):
                    try:
                        float(value)
                        return True
                    except ValueError:
                        return False

                # Convert operators to MongoDB equivalents
                if condition_operator in ["is", "equals", "equal to"]:
                    # Handle numeric and string values separately
                    if is_numeric(condition_value):
                        condition_value = float(condition_value)
                        condition = {"$eq": condition_value}
                    else:
                        #st.write("checkpoint 2")
                        # For string conditions, use regex for case-insensitive matching
                        condition_value = condition_value.lower()  # Ensure the comparison is case-insensitive
                        condition = {"$regex": condition_value, "$options": "i"}
                elif condition_operator in ["greater than"]:
                    #st.write("checkpoint 1")
                    if is_numeric(condition_value):
                        condition_value = float(condition_value)
                    condition = {"$gt": condition_value}
                elif condition_operator in ["less than"]:
                    if is_numeric(condition_value):
                        condition_value = float(condition_value)
                    condition = {"$lt": condition_value}
                elif condition_operator in ["not","not equal to", "not equals"]:
                    if is_numeric(condition_value):
                        condition_value = float(condition_value)
                    condition = {"$ne": condition_value}
                elif condition_operator in ["greater than or equal to"]:
                    if is_numeric(condition_value):
                        condition_value = float(condition_value)
                    condition = {"$gte": condition_value}
                elif condition_operator in ["less than or equal to"]:
                    if is_numeric(condition_value):
                        condition_value = float(condition_value)
                    condition = {"$lte": condition_value}

                #print(collection_name)
                #print(field_name)
                #print(condition)
                if field_name and condition:
                    #st.write("checkpoint 3")
                    #st.write(f"{collection_name}")
                    #st.write(f"{condition}{field_name}")
                    return "find_data_with_condition", collection_name, (field_name, condition)
    #query: whats the maximum amount in orders
    if "maximum" in tokens or "max" in tokens:
        agg_index = tokens.index("maximum") if "maximum" in tokens else tokens.index("max")
        #collection_name = tokens[-1]
        for token in tokens:
            if token in collection_info:
                collection_name = token
        if collection_name in collection_info:
            field_name = tokens[agg_index + 1]
            return "find_maximum", collection_name, field_name
    #query: whats the minimum amount in orders
    if "minimum" in tokens or "min" in tokens:
        agg_index = tokens.index("minimum") if "minimum" in tokens else tokens.index("min")
        #collection_name = tokens[-1]
        for token in tokens:
            if token in collection_info:
                collection_name = token
        if collection_name in collection_info:
            field_name = tokens[agg_index + 1]
            return "find_minimum", collection_name, field_name
    #query: whats the average amount in orders
    if "average" in tokens or "avg" in tokens:
        agg_index = tokens.index("average") if "average" in tokens else tokens.index("avg")
        #collection_name = tokens[-1]
        for token in tokens:
            if token in collection_info:
                collection_name = token
        if collection_name in collection_info:
            field_name = tokens[agg_index + 1]
            return "find_average", collection_name, field_name
    #query: whats the total amount in orders
    if "sum" in tokens or "total" in tokens:
        agg_index = tokens.index("sum") if "sum" in tokens else tokens.index("total")
        #collection_name = tokens[-1]
        for token in tokens:
            if token in collection_info:
                collection_name = token

        #st.write(f"Collection name - {collection_name}")
        if collection_name in collection_info:
            field_name = tokens[agg_index + 1]
            return "find_sum", collection_name, field_name

    #show collections
    if any(phrase in tokens for phrase in ["show", "list", "what", "give"]) and "collections" in tokens or ("available" in tokens or "database" in tokens):
        return "show_all_collections", None, None
    #query: show orders
    #query: give me all records from customers
    else:
        collection_name = None
        for token in tokens:
            if token in collection_info:
                collection_name = token
                break
        # st.write(f"Collection Name: {collection_name}")
        if collection_name:
            return "find_all_data_in_collection", collection_name, None



    return None, None, None


def execute_sql_query(cursor, query):
    """Execute the generated query and print the results."""
    try:
        r = []
        st.write(f"\nMySQL query: {query}")
        cursor.execute(query)
        rows = cursor.fetchall()
        #print("Results:")
        for row in rows:
            r.append(row)
        return r
    except Error as e:
        st.error(f"Error executing query: {e}")



def execute_query(database, collection_name, field_name=None, action=None):
    """Execute the generated query."""
    try:
        res = []
        collection = database[collection_name]

        if action == "join_tables":
            pipeline = field_name
            st.code(f"db.{collection_name}.aggregate({pipeline})")
            results = collection.aggregate(pipeline)
        
            #print(f"Data from collection '{collection_name}' with join and sorting:")
            result_list = list(results)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

        if action == "complex_query":
            query_params = field_name
            #print(f"Received query parameters: {query_params}")

            # Initialize the aggregation pipeline
            pipeline = []
            #print("Initialized aggregation pipeline.")

            # Define the aggregation functions map
            aggregation_map = {
                "average": "$avg",
                "maximum": "$max",
                "minimum": "$min",
                "count": "$sum",  # Count is often implemented with $sum for the number of documents
                "sum": "$sum",    # Sum of values
                "standard deviation": "$stdDevPop"  # Example for standard deviation
            }

            # Handle WHERE condition (using $match)
            if query_params.get("condition"):
                condition = query_params["condition"]
                #print(f"Where condition found: {condition}")

                field = condition["field"]
                value = condition["value"]
                operator = condition["operator"]

                # Convert numeric strings to appropriate types
                try:
                    if isinstance(value, str) and value.replace('.', '', 1).isdigit():
                        value = float(value) if '.' in value else int(value)
                except ValueError:
                    pass  # Leave value as-is if conversion fails

                # For string matching, apply case-insensitive regex
                if isinstance(value, str) and operator.lower() == "is":
                    mongo_operator = "$regex"
                    value = f"^{value}$"
                    pipeline.append({
                        "$match": {
                            field: {
                                mongo_operator: value,
                                "$options": "i"  # Case-insensitive flag
                            }
                        }
                    })
                else:
                    operator_mapping = {
                        "greater than or equal to": "$gte",
                        "less than or equal to": "$lte",
                        "greater than": "$gt",
                        "less than": "$lt",
                        "is": "$eq",
                        "not": "$ne",
                        "is greater than or equal to": "$gte",
                        "is less than or equal to": "$lte",
                        "is greater than": "$gt",
                        "is less than": "$lt",
                        "equal": "$eq",
                        "not": "$ne",
                        "is not": "$ne"
                    }
                    mongo_operator = operator_mapping.get(operator.lower(), "$eq")
                    pipeline.append({
                        "$match": {
                            field: {
                                mongo_operator: value
                            }
                        }
                    })

                #print(f"Pipeline after WHERE condition: {pipeline}")

            # Handle GROUP BY (using $group)
            group_stage = {
                "_id": f"${query_params['group_field']}",  # Group by the specified field
                query_params['aggregate']['operation']: {aggregation_map.get(query_params['aggregate']['operation'], "$avg"): f"${query_params['aggregate']['field']}"}  # Use dynamic aggregation
            }
            pipeline.append({"$group": group_stage})
            #print(f"Pipeline after $group: {pipeline}")

            # Handle HAVING condition (if exists)
            if query_params.get("having_condition"):
                having_condition = query_params["having_condition"]
                #print(f"Having condition found: {having_condition}")

                operator_mapping = {
                    "greater than or equal to": "$gte",
                    "less than or equal to": "$lte",
                    "greater than": "$gt",
                    "less than": "$lt",
                    "is": "$eq",
                    "not": "$ne",
                    "is greater than or equal to": "$gte",
                    "is less than or equal to": "$lte",
                    "is greater than": "$gt",
                    "is less than": "$lt",
                    "equal": "$eq",
                    "not": "$ne",
                    "is not": "$ne"
                }
                value = having_condition['value']
                operator = operator_mapping.get(having_condition['having_condition'].lower())
                #print("Operator for HAVING:", operator)

                # Convert numeric strings to appropriate types
                try:
                    if isinstance(value, str) and value.replace('.', '', 1).isdigit():
                        value = float(value) if '.' in value else int(value)
                except ValueError:
                    pass  # Leave value as-is if conversion fails

                pipeline.append({
                    "$match": {
                        query_params['aggregate']['operation']: {
                            operator: value
                        }
                    }
                })
                #print(f"Pipeline after HAVING condition: {pipeline}")

            # Handle SORT BY (using $sort)
            if query_params.get("sort"):
                sort_field = query_params["sort"]["field"]
                sort_direction = query_params["sort"]["direction"]
                #print(f"Sort field: {sort_field}")
                #print(f"Sort direction: {sort_direction}")

                # Sorting by the _id field (which contains customer_name)
                pipeline.append({
                    "$sort": {
                        "_id": sort_direction  # Use '_id' for sorting after the group stage
                    }
                })
                #print(f"Pipeline after SORT: {pipeline}")

            # Print the pipeline as a MongoDB shell command
            #("\nMongoDB Aggregation Pipeline Command:")
            pipeline_string = f"db.{collection.name}.aggregate({pipeline})"
            st.code(pipeline_string)

            #print("Executing aggregation query...")
            # Execute the aggregation query
            result = collection.aggregate(pipeline)
            result_list = list(result)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

        if action == "find_data_with_condition":
            field_name, condition = field_name
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.find({{{field_name}: {condition}}})")
            #print(condition)
            results = collection.find({field_name: condition})
            #print(results)
            #print(f"Data from collection '{collection_name}' with condition '{field_name}: {condition}':")
            result_list = list(results)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []
                    # st.write(f"{res}")
                    # st.session_state["query_results"] = res

        elif action == "find_data_with_sorting":
            field_name, sort_direction = field_name
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.find().sort({{{field_name}: {sort_direction}}})")
            results = collection.find().sort(field_name, sort_direction)
            #print(f"Data from collection '{collection_name}' sorted by '{field_name}' ({'asc' if sort_direction == 1 else 'desc'}):")
            result_list = list(results)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

        elif action == "count_documents":
            # Perform count on the collection
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.countDocuments({{}})")
            count = collection.count_documents({})
            #return f"The total number of documents in '{collection_name}' is: {count}"
            st.session_state["query_results"] = count

        elif action == "count_documents_with_condition":
            field_name, condition = field_name
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.countDocuments({{{field_name}: {condition}}})")
            #print(condition)
            results = collection.count_documents({field_name: condition})
            #return f"The total number of documents in '{collection_name}' with '{field_name}' '{condition}' is: {results}"
            st.session_state["query_results"] = results

        elif action == "find_maximum":
            # Use aggregation to find maximum value
            pipeline = [{"$group": {"_id": None, "max_value": {"$max": f"${field_name}"}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': null, 'max_value': {{'$max': '${field_name}'}}}}}}])")

            result = list(collection.aggregate(pipeline))
            #return f"The maximum value of '{field_name}' in '{collection_name}' is: {result[0]['max_value']}" if result else "No data found."
            if result:
                st.session_state["query_results"] = result[0]['max_value']
            else:
                st.session_state["query_results"] = "No results found."


        elif action == "find_minimum":
            # Use aggregation to find minimum value
            pipeline = [{"$group": {"_id": None, "min_value": {"$min": f"${field_name}"}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': null, 'min_value': {{'$min': '${field_name}'}}}}}}])")

            result = list(collection.aggregate(pipeline))
            #return f"The minimum value of '{field_name}' in '{collection_name}' is: {result[0]['min_value']}" if result else "No data found."
            if result:
                st.session_state["query_results"] = result[0]['min_value']
            else:
                st.session_state["query_results"] = "No results found."

        elif action == "find_average":
            # Use aggregation to find average value
            pipeline = [{"$group": {"_id": None, "avg_value": {"$avg": f"${field_name}"}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': null, 'avg_value': {{'$avg': '${field_name}'}}}}}}])")

            result = list(collection.aggregate(pipeline))
            #return f"The average value of '{field_name}' in '{collection_name}' is: {result[0]['avg_value']}" if result else "No data found."
            if result:
                st.session_state["query_results"] = result[0]['avg_value']
            else:
                st.session_state["query_results"] = "No results found."

        elif action == "find_sum":
            # Use aggregation to calculate sum
            pipeline = [{"$group": {"_id": None, "sum_value": {"$sum": f"${field_name}"}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': null, 'sum_value': {{'$sum': '${field_name}'}}}}}}])")

            result = list(collection.aggregate(pipeline))
            #return f"The sum of '{field_name}' in '{collection_name}' is: {result[0]['sum_value']}" if result else "No data found."
            if result:
                st.session_state["query_results"] = result[0]['sum_value']
            else:
                st.session_state["query_results"] = "No results found."
            
        elif action == "group_by_count":
            # Group by and count occurrences
            group_field = field_name
            pipeline = [{"$group": {"_id": f"${group_field}", "count": {"$sum": 1}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': '${field_name}', 'count': {{'$sum': 1}}}}}}])")

            result = list(collection.aggregate(pipeline))
            #print(f"Grouping by '{field_name}' with counts in '{collection_name}':")
            result_list = list(result)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

        elif action == "group_by_sum":
            # Group by and sum field
            group_field, agg_field = field_name
            pipeline = [{"$group": {"_id": f"${group_field}", "sum_value": {"$sum": f"${agg_field}"}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': '${group_field}', 'sum_value': {{'$sum': '${agg_field}'}}}}}}])")
            result = list(collection.aggregate(pipeline))
            #print(f"Grouping by '{group_field}' and calculating total '{agg_field}' in collection '{collection_name}':")
            #print(f"Group by '{field_name}' with sum in '{collection_name}':")
            result_list = list(result)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

        elif action == "group_by_avg":
            # Group by and calculate average of field
            group_field, agg_field = field_name
            pipeline = [{"$group": {"_id": f"${group_field}", "avg_value": {"$avg": f"${agg_field}"}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': '${group_field}', 'avg_value': {{'$avg': '${agg_field}'}}}}}}])")
            result = list(collection.aggregate(pipeline))
            #print(f"Grouping by '{group_field}' and calculating average '{agg_field}' in collection '{collection_name}':")
            #print(f"Group by '{field_name}' with average in '{collection_name}':")
            result_list = list(result)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

        elif action == "group_by_max":
            # Group by and find max of field
            group_field, agg_field = field_name
            pipeline = [{"$group": {"_id": f"${group_field}", "max_value": {"$max": f"${agg_field}"}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': '${group_field}', 'max_value': {{'$max': '${agg_field}'}}}}}}])")
            result = list(collection.aggregate(pipeline))
            #print(f"Grouping by '{group_field}' and calculating max '{agg_field}' in collection '{collection_name}':")
            #print(f"Group by '{field_name}' with max value in '{collection_name}':")
            result_list = list(result)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

        elif action == "group_by_min":
            # Group by and find min of field
            group_field, agg_field = field_name
            pipeline = [{"$group": {"_id": f"${group_field}", "min_value": {"$min": f"${agg_field}"}}}]
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.aggregate([{{'$group': {{'_id': '${group_field}', 'min_value': {{'$min': '${agg_field}'}}}}}}])")
            result = list(collection.aggregate(pipeline))
            #print(f"Grouping by '{group_field}' and calculating minimum '{agg_field}' in collection '{collection_name}':")
            #print(f"Group by '{field_name}' with min value in '{collection_name}':")
            result_list = list(result)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

        elif action == "find_all_data_in_collection":
            #st.subheader("Query Generated from natural language:")
            st.code(f"db.{collection_name}.find()")
            results = collection.find()
            result_list = list(results)

            # Check if the list has any documents
            if result_list:
                json_results = [json.dumps(doc, default=str) for doc in result_list]
                # for doc in result_list:
                #     st.json(doc)  # Display each document in a readable JSON format
                # Optionally store in session state
                st.session_state["query_results"] = json_results
            else:
                st.write("No documents found.")
                st.session_state["query_results"] = []

            # return list(results)

        else:
            return None

    except Exception as e:
        return f"Error executing query: {e}"
    

def handle_sql_query(user_input, tables, table_column_mapping, cursor):
    """Handle user query after parsing and pattern matching."""
    tokens = nltk.word_tokenize(user_input.lower())
    #st.write(f"Tokens: {tokens}")  # Debugging tokens

    # Extract table, columns, and conditions from the user input
    table_name, columns, conditions = extract_table_and_column_with_condition(
        user_input, tables, table_column_mapping
    )

    # Handle Inner Join: "show me the data table1 and table2 have in common"
    # Handle Inner Join: "give me the data sales and stores have in common"
    if "give" in tokens and "me" in tokens and "10" in tokens and "data" in tokens and "sales" in tokens and "stores" in tokens and "have" in tokens and "in" in tokens and "common" in tokens:
        # Attempting to find common columns
        table1 = 'sales'
        table2 = 'stores'

        if table1 not in tables or table2 not in tables:
            st.error(f"Error: One or both tables '{table1}' and '{table2}' do not exist.")
            return
        
        # Check for common column, defaulting to 'Store'
        query_check_columns1 = f"SHOW COLUMNS FROM {table1}"
        query_check_columns2 = f"SHOW COLUMNS FROM {table2}"
        cursor.execute(query_check_columns1)
        columns1 = {row[0] for row in cursor.fetchall()}
        cursor.execute(query_check_columns2)
        columns2 = {row[0] for row in cursor.fetchall()}

        common_columns = columns1.intersection(columns2)
        if not common_columns:
            st.error(f"Error: No common columns found between tables '{table1}' and '{table2}'. Using 'Store' as the join column.")
            join_column = 'Store'
        else:
            join_column = list(common_columns)[0]  # Take the first common column

        # Limit detection
        limit_value = None
        if "limit" in tokens:
            try:
                limit_index = tokens.index("limit") + 1
                limit_value = int(tokens[limit_index])
            except (ValueError, IndexError):
                st.error("Error: Invalid or missing limit value.")
                return

        # Construct the query
        query = f"""
        SELECT * 
        FROM {table1} 
        INNER JOIN {table2} 
        ON {table1}.{join_column} = {table2}.{join_column}
        """
        if limit_value:
            query += f" LIMIT {limit_value}"

        st.write(f"Executing query: {query}")
        return execute_sql_query(cursor, query)
        

    # Default behavior for unrecognized queries
    # if table_name:
    #     columns = ["*"] if not columns else columns
    #     query = f"SELECT {', '.join(columns)} FROM {table_name}"
    #     if conditions:
    #         where_clauses = " AND ".join(
    #             f"{col} {op} '{val}'" for col, (op, val) in conditions.items()
    #         )
    #         query += f" WHERE {where_clauses}"
    #     return execute_sql_query(cursor, query)

    # Handle "greater than" condition
    if "greater" in tokens and "than" in tokens:
        where_index = tokens.index("where")
        condition_index = where_index + 1
        column = tokens[condition_index]
        value_index = tokens.index("than") + 1
        value = tokens[value_index]
        query = f"SELECT * FROM {table_name} WHERE {column} > {value}"
        return execute_sql_query(cursor, query)
    

    # Handle "less than" condition
    if "less" in tokens and "than" in tokens:
        where_index = tokens.index("where")
        condition_index = where_index + 1
        column = tokens[condition_index]
        value_index = tokens.index("than") + 1
        value = tokens[value_index]
        query = f"SELECT * FROM {table_name} WHERE {column} < {value}"
        return execute_sql_query(cursor, query)

    # Handle "show first N data" for LIMIT
    if "show" in tokens and "first" in tokens and "data" in tokens:
        first_index = tokens.index("first") + 1
        limit_value = tokens[first_index]
        query = f"SELECT * FROM {table_name} LIMIT {limit_value}"
        return execute_sql_query(cursor, query)
        
    # Handle "show last N data" for LIMIT
    if "show" in tokens and "last" in tokens and "data" in tokens:
        last_index = tokens.index("last") + 1
        try:
            limit_value = int(tokens[last_index])  # Extract and validate the number
        except ValueError:
            st.error("Error: Invalid number specified for 'last'.")
            return

        # Validate the table and retrieve columns
        query_check_columns = f"SHOW COLUMNS FROM {table_name}"
        cursor.execute(query_check_columns)
        columns = cursor.fetchall()
        if not columns:
            st.error(f"Error: No valid columns found for table '{table_name}'.")
            return

        # Default to the first column for ordering if no specific order column exists
        ordering_column = columns[0][0]  # Use the first column by default
        query = f"SELECT * FROM {table_name} ORDER BY {ordering_column} DESC LIMIT {limit_value}"
        return execute_sql_query(cursor, query)
        

    # Handle "average" or "avg" for aggregate calculation
    if "average" in tokens or "avg" in tokens:
        st.write("checkpoint 1")
        aggregate_column = tokens[tokens.index("of") + 1]
        query = f"SELECT AVG({aggregate_column}) FROM {table_name}"
        if "group" in tokens and "by" in tokens:
            group_by_index = tokens.index("by")
            group_column = tokens[group_by_index + 1]
            if group_column in table_column_mapping[table_name]:
                query = f"SELECT {group_column}, AVG({aggregate_column}) FROM {table_name} GROUP BY {group_column}"
        if "order" in tokens and "by" in tokens:
            order_index = tokens.index("by")
            order_column = tokens[order_index + 1]
            if order_column in table_column_mapping[table_name]:
                query += f" ORDER BY {order_column}"
        return execute_sql_query(cursor, query)

    # Handle "sum" aggregation
    if "sum" in tokens:
        column_index = tokens.index("sum") + 2
        column_name = tokens[column_index]
        query = f"SELECT SUM({column_name}) FROM {table_name}"
        if "group" in tokens and "by" in tokens:
            group_by_index = tokens.index("by") + 1
            group_by_column = tokens[group_by_index]
            query = f"SELECT {group_by_column}, SUM({column_name}) FROM {table_name} GROUP BY {group_by_column}"
        if "order" in tokens and "by" in tokens:
            query += " ORDER BY SUM({column_name})"
        return execute_sql_query(cursor, query)

    # Default behavior for unrecognized queries
    if table_name:
        columns = ["*"] if not columns else columns
        query = f"SELECT {', '.join(columns)} FROM {table_name}"
        if conditions:
            where_clauses = " AND ".join(
                f"{col} {op} '{val}'" for col, (op, val) in conditions.items()
            )
            query += f" WHERE {where_clauses}"
        return execute_sql_query(cursor, query)
    else:
        st.write("Could not understand the query or the table/column does not exist.")



def handle_query(user_input, collection_info, database):
    """Handle user query after tokenizing and matching patterns."""
    #st.write("checkpoint 5")
    action, collection_name, field_name = match_query_pattern(user_input, collection_info)
    action_list = ["display_column_in_collection", "find_data_with_condition", "find_data_with_sorting", "count_documents_with_condition", "find_maximum", "find_minimum", "find_average", "find_sum", "group_by_count", "group_by_sum", "group_by_avg", "group_by_max", "group_by_min", "complex_query", "join_tables"]
    

    if action == "find_data_with_condition":
        if collection_name in collection_info:
            #st.write("checkpoint 4")
            execute_query(database, collection_name, field_name, action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "find_data_with_sorting":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "count_documents":
        # If the action is to count documents, execute the count query
        if collection_name in collection_info:
            execute_query(database, collection_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "count_documents_with_condition":
        # If the action is to count documents, execute the count query
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "find_maximum":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "find_minimum":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "find_average":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "find_sum":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "group_by_count":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "group_by_sum":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "group_by_avg":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "group_by_max":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "group_by_min":
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action == "show_all_collections":
        #print(f"Mongodb query: db.getCollectionNames()")
        #print("Collections in the database:")
        for collection in collection_info.keys():
            st.write(collection)
    elif action == "find_all_data_in_collection":
        #st.write(f"Collection Name: {collection_name}")
        if collection_name in collection_info:
            execute_query(database, collection_name, action=action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    elif action in action_list:
        if collection_name in collection_info:
            execute_query(database, collection_name, field_name, action)
        else:
            st.error(f"Collection '{collection_name}' does not exist in the database.")
    else:
        st.error(f"Sorry, I couldn't understand your query.")


# Set up the page layout and title
st.set_page_config(page_title="Talk2DB: Talk to your data", layout="centered", page_icon="📊")

# Header
st.markdown(
    """
    <style>
        /* Body Styling */
        body {
            background-color: #f9fafc;
            color: #333;
            font-family: 'Roboto', sans-serif;
        }

        /* Header Styling */
        .main-header {
            background: linear-gradient(90deg, #1f77b4, #2ca02c);
            color: white;
            text-align: center;
            padding: 20px;
            border-radius: 10px;
            font-size: 36px;
            font-weight: bold;
            margin-bottom: 30px;
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
        }

        /* Sidebar Styling */
        [data-testid="stSidebar"] {
            background-color: #1f77b4;
            color: white;
        }
        [data-testid="stSidebar"] .css-1d391kg {
            color: white !important;
        }

        /* Input Containers */
        .input-container {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 20px;
        }

        /* Buttons */
        .stButton>button {
            background-color: #1f77b4 !important;
            color: white !important;
            border: none !important;
            border-radius: 5px !important;
            padding: 10px 20px !important;
            font-size: 14px !important;
        }
        .stButton>button:hover {
            background-color: #2ca02c !important;
        }

        /* Query Results Box */
        .query-results {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            font-family: 'Courier New', Courier, monospace;
        }

        /* Footer */
        .footer {
            text-align: center;
            margin-top: 20px;
            font-size: 14px;
            color: #666;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# Header
st.markdown("<div class='main-header'>Talk2DB: Talk to your data</div>", unsafe_allow_html=True)

# Warning Box
if "show_warning" not in st.session_state:
    st.session_state["show_warning"] = False

if st.session_state["show_warning"]:
    st.warning("⚠️ Your custom warning message goes here.", icon="⚠️")

# Main container
st.sidebar.title("Options")
st.markdown(
    """
    <style>
        /* Sidebar Styling */
        [data-testid="stSidebar"] {
            background-color: #1f77b4;
            color: white;
            padding-top: 10px; /* Reduced padding at the top */
        }
        [data-testid="stSidebar"] .css-1d391kg {
            color: white !important;
        }

        /* Sidebar header or "Options" styling */
        .css-1lcbmhc {
            margin-top: 0 !important; /* Removed any additional margin */
            padding-top: 10px; /* Slight padding for breathing room */
        }

        /* Body Styling */
        body {
            background-color: #f9fafc;
            color: #333;
            font-family: 'Roboto', sans-serif;
        }

        /* Header Styling */
        .main-header {
            background: linear-gradient(90deg, #1f77b4, #2ca02c);
            color: white;
            text-align: center;
            padding: 20px;
            border-radius: 10px;
            font-size: 36px;
            font-weight: bold;
            margin-bottom: 20px;
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
        }

        /* Input Containers */
        .input-container {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 20px;
        }

        /* Buttons */
        .stButton>button {
            background-color: #1f77b4 !important;
            color: white !important;
            border: none !important;
            border-radius: 5px !important;
            padding: 10px 20px !important;
            font-size: 14px !important;
        }
        .stButton>button:hover {
            background-color: #2ca02c !important;
        }

        /* Query Results Box */
        .query-results {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            font-family: 'Courier New', Courier, monospace;
        }

        /* Footer */
        .footer {
            text-align: center;
            margin-top: 20px;
            font-size: 14px;
            color: #666;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

if "datasets" not in st.session_state:
    st.session_state.datasets = ""
if "database" not in st.session_state:
    st.session_state.database = ""
if "uploaded_files" not in st.session_state:
    st.session_state["uploaded_files"] = []
    
# if "construct" not in st.session_state:
#     st.session_state.construct = ""


with st.sidebar:
    # Select Database
    database = st.selectbox("Select Database:", ["", "MySQL", "MongoDB"], key="database")

    # Select Dataset
    if database == "MySQL":
        datasets = st.selectbox("Select Dataset:", ["", "students", "products", "retail"], key="datasets")
    elif database == "MongoDB":
        datasets = st.selectbox("Select Dataset:", ["", "realestate", "coffeesales", "inventorymanagement"], key="datasets")
    else:
        datasets = st.selectbox("Select Dataset:", [""], key="datasets")

    # Upload Dataset
    uploaded_files = st.file_uploader("Upload Your Dataset:", type=["csv", "xlsx", "txt", "json"], accept_multiple_files=True)

    def uploads():
        if uploaded_files:
            st.write("Uploaded Files:")
        for uploaded_file in uploaded_files:
        # Display the file name
            st.write(f"File Name: {uploaded_file.name}")

        # Read the CSV file
            try:
                if uploaded_files is not None:
                # Get the file extension to determine its type
                    file_extension = uploaded_file.name.split('.')[-1].lower()

                if file_extension == "csv":
                    # If the file is a CSV
                    df = pd.read_csv(uploaded_file)
                    st.write("CSV File Loaded Successfully")
                    st.write(df.head())  # Show the first 5 rows of the CSV file

                    conn = mysql.connector.connect(**db_config)
                    cursor = conn.cursor()

                    # Dynamically create a table based on the DataFrame columns
                    #table_name = uploaded_file.name
                    table_name = os.path.splitext(uploaded_file.name)[0]
                    columns = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
                    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
                    cursor.execute(create_table_query)
                    
                    # Insert data into the table
                    for _, row in df.iterrows():
                        row_values = ", ".join([f"'{str(value)}'" for value in row])
                        insert_query = f"INSERT INTO {table_name} VALUES ({row_values});"
                        cursor.execute(insert_query)

                    # Commit the transaction and close the connection
                    conn.commit()
                    cursor.close()
                    conn.close()
                
                elif file_extension == "xlsx":
                    # If the file is an Excel file
                    df = pd.read_excel(uploaded_file)
                    st.write("Excel File Loaded Successfully")
                    st.write(df.head())  # Show the first 5 rows of the Excel file

                    conn = mysql.connector.connect(**db_config)
                    cursor = conn.cursor()

                    # Dynamically create a table based on the DataFrame columns
                    #table_name = uploaded_file.name
                    table_name = os.path.splitext(uploaded_file.name)[0]
                    columns = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
                    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
                    cursor.execute(create_table_query)
                    
                    # Insert data into the table
                    for _, row in df.iterrows():
                        row_values = ", ".join([f"'{str(value)}'" for value in row])
                        insert_query = f"INSERT INTO {table_name} VALUES ({row_values});"
                        cursor.execute(insert_query)

                    # Commit the transaction and close the connection
                    conn.commit()
                    cursor.close()
                    conn.close()
                
                elif file_extension == "json":
                    # If the file is a JSON file
                    df = pd.read_json(uploaded_file)
                    st.write("JSON File Loaded Successfully")
                    st.write(df.head())  # Show the first 5 rows of the JSON file

                    mongo_client = MongoClient("mongodb://localhost:27017/")
                    db = mongo_client["project"]  # Replace with your database name

                    # Assuming uploaded_file and DataFrame (df) are already defined
                    table_name = os.path.splitext(uploaded_file.name)[0]

                    # Convert the DataFrame to a JSON object
                    data_as_json = df.to_dict(orient="records")

                    # Save to a JSON file
                    json_file_path = f"{table_name}.json"
                    with open(json_file_path, "w") as json_file:
                        json.dump(data_as_json, json_file, indent=4)
                    #print(f"Data saved as JSON file: {json_file_path}")

                    # Insert data into a MongoDB collection
                    collection = db[table_name]
                    collection.insert_many(data_as_json)
                    #print(f"Data inserted into MongoDB collection: {table_name}")

                    # MongoDB client cleanup
                    mongo_client.close()
                
                elif file_extension == "txt":
                    # If the file is a TXT file
                    text = uploaded_file.read().decode("utf-8")
                    st.write("Text File Loaded Successfully")
                    st.text(text[:500])  # Display a preview of the dataframe

            except Exception as e:
                return st.error(f"Error reading {uploaded_file.name}: {e}")


    st.write("Preview:")
    r1 = uploads()

    # if uploaded_file is not None:
    #     file_size = len(uploaded_file.getvalue()) / (1024 * 1024)  # Size in MB
    #     st.write(f"Uploaded file size: {file_size:.2f} MB")
    
    #     if uploaded_file.name.endswith(".csv"):
    #         data = pd.read_csv(uploaded_file)
    #         st.write("Data Preview:")
    #         st.dataframe(data.head())
    #     else:
    #         st.write("File uploaded successfully but not processed (unsupported type).")

    # Select Construct
    # construct = st.selectbox(
    #     "Select a Construct:", 
    #     ["", "Group By", "Having", "Order By", "Join", "Where", "Aggregation"], key = "construct"
    # )

    # Enter Natural Language Query
    query = st.text_input("Enter Query in Natural Language:")

counter = 0
# Button group
st.sidebar.markdown("### Actions")
if st.sidebar.button("Generate Sample Query"):
    st.info("Sample query generation is clicked!")
    counter = 1
if st.sidebar.button("Generate from Natural Language"):
    # st.success(f"Generated SQL query based on: {query}")
    counter = 2
if st.sidebar.button("Explore Databases"):
    #st.info("Exploring databases...")
    counter = 3
if st.sidebar.button("Clear Results"):
    st.session_state["query_results"] = ""
    st.info("Results cleared!")
    counter = 4
# if st.sidebar.button("Execute Sample Query"):
#     st.info("Sample query execution is clicked!")
#     counter = 5


# Query Results
st.markdown("## Query Results:")
if "query_results" not in st.session_state:
    st.session_state["query_results"] = ""


num_tables = []
if query:
    tokens = nltk.word_tokenize(query.lower())
    if datasets == "realestate" or datasets == "inventorymanagement" or datasets == "coffeesales":
        num_tables = ["properties","clients","agents","orders","customers","products","warehouse","suppliers","items"]
        for i in num_tables:
            if i in tokens:
                dataset = i
    elif datasets == "demo":
        num_tables = ["students","courses","enrollments"]
        for i in num_tables:
            if i in tokens:
                dataset = i
else:
    if datasets == "realestate":
        num_tables = ["properties","clients","agents"]
        dataset = random.choice(num_tables)
    elif datasets == "inventorymanagement":
        num_tables = ["warehouse","suppliers","items"]
        dataset = random.choice(num_tables)
    elif datasets == "coffeesales":
        num_tables = ["orders","customers","products"]
        dataset = random.choice(num_tables)
    elif datasets == "students":
        num_tables = ["students","courses","enrollments"]
        dataset = random.choice(num_tables)
    elif datasets == "products":
        num_tables = ["orders","customers","products"]
        dataset = random.choice(num_tables)
    elif datasets == "retail":
        num_tables = ["features","sales","stores"]
        dataset = random.choice(num_tables)

if uploaded_files:
    for file in uploaded_files:
        if file not in st.session_state["uploaded_files"]:  # Avoid duplicates
            st.session_state["uploaded_files"].append(file)
            st.success(f"Uploaded: {file.name}") 


results = []
connection = create_connection()
table_info, tables = get_table_names_and_columns(connection)
tables, table_column_mapping, column_data_types = sql_get_table_names_and_columns(connection)
db = create_mongo_connection(host, port, database_name)
collection_info = get_collection_names_and_fields(db)


if counter == 1: #Generate Sample Query

    if not st.session_state.database:
        st.error("Please select a database before proceeding.")
    # elif not st.session_state.dataset:
    #     st.error("Please select existing or upload at least one dataset before proceeding.")
    else:
        #st.success(f"Proceeding with Database: {st.session_state.database}")
        # Add logic for database exploration here
        if st.session_state.database == "MySQL":
            st.write("Exploring MySQL Database...")
            base_queries = {
    "sorted_by_column": [
        "SELECT * FROM {table_name} ORDER BY {column_name} ASC;",
        "SELECT * FROM {table_name} ORDER BY {column_name} DESC;"
    ],
    "aggregates": [
        "SELECT {column_name}, MIN(spent_column) FROM {table_name} GROUP BY {column_name};",
        "SELECT {column_name}, MAX(spent_column) FROM {table_name} GROUP BY {column_name};",
        "SELECT {column_name}, SUM(spent_column) FROM {table_name} GROUP BY {column_name};",
        "SELECT {column_name}, AVG(spent_column) FROM {table_name} GROUP BY {column_name};"
    ],
    "count_where": [
        "SELECT COUNT(*) FROM {table_name} WHERE {column_name} > {numeric_value};",
        "SELECT COUNT(*) FROM {table_name} WHERE {column_name} < {numeric_value};",
        "SELECT COUNT(*) FROM {table_name} WHERE {column_name} >= {numeric_value};",
        "SELECT COUNT(*) FROM {table_name} WHERE {column_name} <= {numeric_value};",
        "SELECT COUNT(*) FROM {table_name} WHERE {column_name} != {numeric_value};"
    ],
    "count_documents": [
        "SELECT COUNT(*) FROM {table_name};"
    ],
    "where": [
        "SELECT * FROM {table_name} WHERE {column_name} >= {numeric_value};",
        "SELECT * FROM {table_name} WHERE {column_name} < {numeric_value};",
        "SELECT * FROM {table_name} WHERE {column_name} = '{value}';",
    ],
    "sample_queries": [
        "SELECT * FROM {table_name};",
        "SELECT {column_name} FROM {table_name};",
        "SELECT * FROM {table_name} WHERE {column_name} = '{value}';",
        "SELECT {column_name}, COUNT(*) FROM {table_name} GROUP BY {column_name};",
        "SELECT * FROM {table_name} ORDER BY {column_name} DESC;"
    ]
}

            # Add MySQL exploration logic
        elif st.session_state.database == "MongoDB":
            st.write("Exploring MongoDB Database...")
            collection_info = sample_get_collection_names_and_fields(db)
            random_queries = generate_sample_query(query, collection_info)
            for query in random_queries:
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown(f"<p style='font-size:25px;'>Natural language query:</p>", unsafe_allow_html=True)
                #st.write(f"Natural language query:")
                st.markdown(f"<p style='font-size:20px;'>{query}</p>", unsafe_allow_html=True)
                results = handle_query(query, collection_info, db)
            

if counter == 2: #Generate from Natural Language

    if not st.session_state.database:
        st.error("Please select a database before proceeding.")
    elif not st.session_state.datasets:
        st.error("Please select existing or upload at least one dataset before proceeding.")
    elif not query:
        st.error("Please enter a natural language query.")
    else:
        if st.session_state.database == "MySQL":
            results = handle_sql_query(query, tables, table_column_mapping, connection.cursor())
            
        elif st.session_state.database == "MongoDB":
            results = handle_query(query, collection_info, db)



if counter == 3: #Explore Databases

    if not st.session_state.database:
        st.error("Please select a database before proceeding.")
    elif not st.session_state.datasets:
        st.error("Please select the existing dataset before proceeding.")
    else:
        st.success(f"Proceeding with Database: {st.session_state.database}")
        # Add logic for database exploration here
        if st.session_state.database == "MySQL":

            st.subheader("Sample Queries for MySQL:")

            # if uploaded_files:
            #     st.session_state.dataset = [file.name for file in uploaded_files]
            #     st.success(f"Uploaded {len(uploaded_files)} file(s): {', '.join(st.session_state.dataset)}")

            # table = st.session_state.dataset if isinstance(st.session_state.dataset, str) else st.session_state.dataset[0]
            table = dataset
            cols = table_info[table]["columns"]
            col = random.choice(cols)

            action = "find_all_data_in_table"

            results = execute_sql_query(connection.cursor(), query)

            
            # Add MySQL exploration logic
        elif st.session_state.database == "MongoDB":
            st.subheader("Sample Queries for MongoDB:")

            collection_name = dataset  # Replace with your actual collection name

            # Choose a random collection and field
            sample_document = db[collection_name].find_one()

            if sample_document:
                # Extract the field names (keys) from the sample document
                field_names = list(sample_document.keys())
    
                # Select a random field from the list of field names
                selected_field = random.choice(field_names)

            
            action = "find_all_data_in_collection"

            results = execute_query(db, collection_name, selected_field, action)



if counter == 4: #Clear Results

   st.session_state["query_results"] = ""

# if counter == 5: #Execute Sample Query

#     sql_query = generate_sample_query(query, collection_info)
#     results = get_data_from_db(sql_query)


if database != "MongoDB":
    if counter != 4 and counter != 1:
        if results:
        #st.write(f"{sql_query}")
            st.session_state["query_results"] = "\n".join([str(row) for row in results])
        else:
            st.session_state["query_results"] = "No results found."

st.text_area("Results", value=st.session_state["query_results"], height=300)

