import psycopg
import sys
import uuid
import datetime

# --- Database Connection Configuration ---
# Update these with your actual PostgreSQL credentials.
DB_NAME = "your_db_name"
DB_USER = "your_user"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = 5432

# The name of the graph created in schema.py
GRAPH_NAME = "my_graph"

# --- Main Logic ---

def ingest_triplets():
    """
    Connects to the PostgreSQL database and ingests a list of triplets
    (source vertex, edge, target vertex) into the Apache AGE graph.
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

        # Sample triplets to ingest
        triplets = [
            ("Alice", "WORKS_AT", "Google"),
            ("Bob", "WORKS_AT", "Google"),
            ("Charlie", "WORKS_AT", "Microsoft"),
            ("Alice", "FRIENDS_WITH", "Bob"),
            ("Bob", "FRIENDS_WITH", "Charlie"),
        ]

        print(f"Starting ingestion of {len(triplets)} triplets into graph '{GRAPH_NAME}'...")
        for subject, predicate, obj in triplets:
            # Use Cypher's MERGE clause to either find an existing vertex or create a new one.
            # This prevents duplicate vertices if the ingestion script is run multiple times.
            # We add a `created_at` timestamp to a property for range partitioning.
            created_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            cypher_query = f"""
                MERGE (a:Person {{name: '{subject}', created_at: '{created_at}'}})
                MERGE (b:Company {{name: '{obj}'}})
                CREATE (a)-[:{predicate}]->(b)
            """
            
            # Execute the Cypher query
            cur.execute(f"SELECT * FROM cypher('{GRAPH_NAME}', $$ {cypher_query} $$) AS (result agtype);")
            
            print(f"Ingested: ({subject})-[:{predicate}]->({obj})")

        print("\nIngestion complete!")

    except (Exception, psycopg.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    ingest_triplets()
