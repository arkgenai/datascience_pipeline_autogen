import psycopg
import sys
import uuid
import random

# --- Database Connection Configuration ---
# Update these with your actual PostgreSQL credentials.
DB_NAME = "your_db_name"
DB_USER = "your_user"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = 5432

# The name of the graph created in schema.py
GRAPH_NAME = "my_graph"

# The lists of labels from schema.py for reference
VERTEX_LABELS = [
    "Person", "Company", "Product", "City", "Country",
    "Employee", "Customer", "Supplier", "Department", "Project"
]
EDGE_LABELS = [
    "WORKS_AT", "LOCATED_IN", "PURCHASED", "MANUFACTURED_BY", "SUPPLIES"
]

def ingest_triplets():
    """
    Connects to the PostgreSQL database and ingests a list of triplets
    into the Apache AGE graph, including properties for source vertices,
    relationships (edges), and target vertices.
    """
    conn = None
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Load the Apache AGE extension for the current session
        print("Loading AGE extension...")
        cur.execute("LOAD 'age';")
        cur.execute(f"SET search_path TO ag_catalog, '$user', public;")

        # --- Sample Triplet Data ---
        # Each triplet now includes:
        # (source_label, source_properties, edge_label, edge_properties, target_label, target_properties)
        triplets = [
            ("Person", {"id": "500", "name": "Alice"}, "WORKS_AT", {"since": "2020-01-15", "role": "Software Engineer"}, "Company", {"id": "100", "name": "Google"}),
            ("Person", {"id": "1500", "name": "Bob"}, "WORKS_AT", {"since": "2022-03-01", "role": "Data Scientist"}, "Company", {"id": "100", "name": "Google"}),
            ("Person", {"id": "2500", "name": "Charlie"}, "WORKS_AT", {"since": "2019-07-20", "role": "Project Manager"}, "Company", {"id": "101", "name": "Microsoft"}),
            ("Person", {"id": "501", "name": "Alice"}, "PURCHASED", {"date": "2024-06-01", "quantity": 1}, "Product", {"id": "300", "name": "Laptop"}),
            ("Company", {"id": "100", "name": "Google"}, "LOCATED_IN", {"main_office": True}, "City", {"id": "400", "name": "Mountain View"}),
            ("Company", {"id": "100", "name": "Google"}, "SUPPLIES", {"contract_id": "C-9876", "terms": "Net 30"}, "Company", {"id": "102", "name": "Dell"}),
        ]

        print(f"Starting ingestion of {len(triplets)} triplets into graph '{GRAPH_NAME}'...")
        for subject_label, subject_props, predicate_label, edge_props, object_label, object_props in triplets:
            
            # Convert property dictionaries to Cypher-compatible JSON strings
            subject_props_str = ', '.join([f"{k}: '{v}'" for k, v in subject_props.items()])
            object_props_str = ', '.join([f"{k}: '{v}'" for k, v in object_props.items()])
            edge_props_str = ', '.join([f"{k}: '{v}'" for k, v in edge_props.items()])

            # Construct the Cypher query using f-strings for dynamic labels and properties
            # We use MERGE to prevent creating duplicate vertices
            # The edge now includes its properties within the CREATE clause
            cypher_query = f"""
                MERGE (a:{subject_label} {{{subject_props_str}}})
                MERGE (b:{object_label} {{{object_props_str}}})
                CREATE (a)-[r:{predicate_label} {{{edge_props_str}}}]->(b)
            """
            
            # Execute the Cypher query
            cur.execute(f"SELECT * FROM cypher('{GRAPH_NAME}', $$ {cypher_query} $$) AS (result agtype);")
            
            print(f"Ingested: ({subject_label}, id:{subject_props.get('id', 'N/A')})-[:{predicate_label} {edge_props}]->({object_label}, id:{object_props.get('id', 'N/A')})")

        print("\nIngestion complete!")

    except (Exception, psycopg.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    ingest_triplets()
