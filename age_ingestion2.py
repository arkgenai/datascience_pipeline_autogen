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
    into the Apache AGE graph, using the multiple labels from the schema.
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
        # Note: We'll generate a unique ID for each vertex to align with the partitioning scheme.
        triplets = [
            ("Person", {"id": "500", "name": "Alice"}, "WORKS_AT", "Company", {"id": "100", "name": "Google"}),
            ("Person", {"id": "1500", "name": "Bob"}, "WORKS_AT", "Company", {"id": "100", "name": "Google"}),
            ("Person", {"id": "2500", "name": "Charlie"}, "WORKS_AT", "Company", {"id": "101", "name": "Microsoft"}),
            ("Person", {"id": "501", "name": "Alice"}, "PURCHASED", "Product", {"id": "300", "name": "Laptop"}),
            ("Company", {"id": "100", "name": "Google"}, "LOCATED_IN", "City", {"id": "400", "name": "Mountain View"}),
            ("Company", {"id": "100", "name": "Google"}, "SUPPLIES", "Company", {"id": "102", "name": "Dell"}),
        ]

        print(f"Starting ingestion of {len(triplets)} triplets into graph '{GRAPH_NAME}'...")
        for subject_label, subject_props, predicate_label, object_label, object_props in triplets:
            
            # Construct the Cypher query using f-strings for dynamic labels and properties
            # We use MERGE to prevent creating duplicate vertices
            cypher_query = f"""
                MERGE (a:{subject_label} {{id: '{subject_props['id']}', name: '{subject_props['name']}'}})
                MERGE (b:{object_label} {{id: '{object_props['id']}', name: '{object_props['name']}'}})
                CREATE (a)-[:{predicate_label}]->(b)
            """
            
            # Execute the Cypher query
            cur.execute(f"SELECT * FROM cypher('{GRAPH_NAME}', $$ {cypher_query} $$) AS (result agtype);")
            
            print(f"Ingested: ({subject_label}, id:{subject_props['id']})-[:{predicate_label}]->({object_label}, id:{object_props['id']})")

        print("\nIngestion complete!")

    except (Exception, psycopg.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    ingest_triplets()
