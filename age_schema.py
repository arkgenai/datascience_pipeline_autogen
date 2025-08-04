import psycopg
import sys

# --- Database Connection Configuration ---
# You will need to replace these with your actual PostgreSQL credentials.
DB_NAME = "your_db_name"
DB_USER = "your_user"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = 5432

# Define your graph name and labels
GRAPH_NAME = "my_graph"
VERTEX_LABEL = "Person"
EDGE_LABEL = "WORKS_AT"

# --- Main Logic ---

def create_schema():
    """
    Connects to the PostgreSQL database and sets up the Apache AGE graph schema.
    This includes creating the graph, defining partitioned tables for vertices and edges,
    and applying both B-Tree and GIN indexes.
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

        # Initialize the graph
        print(f"Initializing graph '{GRAPH_NAME}'...")
        cur.execute(f"SELECT * FROM ag_catalog.create_graph('{GRAPH_NAME}');")

        # --- Partitioning Setup ---
        # The base table for the vertex label will be partitioned by a 'created_at' property.
        # This is a good example of range partitioning.
        
        print(f"Creating a partitioned table for the '{VERTEX_LABEL}' vertex label...")
        cur.execute(f"""
            CREATE TABLE {GRAPH_NAME}."{VERTEX_LABEL}_parent" (
                id AGTYPE,
                properties AGTYPE
            ) PARTITION BY RANGE (agtype_access_operator(VARIADIC ARRAY[properties, '"created_at"'::agtype]));
        """)

        # Define partitions for a few date ranges
        cur.execute(f"""
            CREATE TABLE {GRAPH_NAME}."{VERTEX_LABEL}_2024" PARTITION OF {GRAPH_NAME}."{VERTEX_LABEL}_parent"
            FOR VALUES FROM ('2024-01-01'::agtype) TO ('2025-01-01'::agtype);
        """)
        cur.execute(f"""
            CREATE TABLE {GRAPH_NAME}."{VERTEX_LABEL}_2025" PARTITION OF {GRAPH_NAME}."{VERTEX_LABEL}_parent"
            FOR VALUES FROM ('2025-01-01'::agtype) TO ('2026-01-01'::agtype);
        """)

        # --- Indexing Setup for Vertices ---
        print("Creating indexes on vertex tables...")
        # B-Tree index on the ID column for fast lookups
        cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{VERTEX_LABEL}_parent\" USING BTREE (id);")
        # GIN index on the properties column for general key-value searches
        cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{VERTEX_LABEL}_parent\" USING GIN (properties);")
        # B-Tree index on a specific key ('name') for a frequently queried property
        cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{VERTEX_LABEL}_parent\" USING BTREE (agtype_access_operator(VARIADIC ARRAY[properties, '\"name\"'::agtype]));")

        # --- Indexing Setup for Edges ---
        print("Creating indexes on edge tables...")
        # Create a non-partitioned edge table
        cur.execute(f"""
            CREATE TABLE {GRAPH_NAME}."{EDGE_LABEL}" (
                id AGTYPE,
                start_id AGTYPE,
                end_id AGTYPE,
                properties AGTYPE
            );
        """)
        # B-Tree index on the ID column
        cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{EDGE_LABEL}\" USING BTREE (id);")
        # B-Tree indexes on the start and end IDs for fast traversals
        cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{EDGE_LABEL}\" USING BTREE (start_id);")
        cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{EDGE_LABEL}\" USING BTREE (end_id);")
        # GIN index on the properties column
        cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{EDGE_LABEL}\" USING GIN (properties);")


        print("\nSchema creation complete!")
        print(f"- Graph '{GRAPH_NAME}' created.")
        print(f"- Partitioned table for '{VERTEX_LABEL}' created with partitions for 2024 and 2025.")
        print(f"- Indexes created on vertex and edge tables.")

    except (Exception, psycopg.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    create_schema()
