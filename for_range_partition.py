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

# List of 10 vertex labels
VERTEX_LABELS = [
    "Person", "Company", "Product", "City", "Country",
    "Employee", "Customer", "Supplier", "Department", "Project"
]

# List of 5 edge labels
EDGE_LABELS = [
    "WORKS_AT", "LOCATED_IN", "PURCHASED", "MANUFACTURED_BY", "SUPPLIES"
]

def create_schema():
    """
    Connects to the PostgreSQL database and sets up the Apache AGE graph schema.
    This version creates partitioned tables and indexes for 10 vertex labels
    and 5 edge labels, with vertex partitioning based on a 'created_at' date property.
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

        # --- Partitioning and Indexing for Vertices ---
        print("\nCreating partitioned tables and indexes for all vertex labels...")
        for label in VERTEX_LABELS:
            print(f"- Processing vertex label: '{label}'")
            
            # Create the parent table for partitioning by the 'created_at' property
            # Ensure your ingested vertices have a 'created_at' property in their JSONB 'properties' field.
            cur.execute(f"""
                CREATE TABLE {GRAPH_NAME}."{label}_parent" (
                    id AGTYPE,
                    properties AGTYPE
                ) PARTITION BY RANGE (agtype_access_operator(VARIADIC ARRAY[properties, '"created_at"'::agtype]));
            """)

            # Create partitions for specific date ranges (e.g., by year)
            # Adjust these ranges based on your expected data distribution and query patterns.
            cur.execute(f"""
                CREATE TABLE {GRAPH_NAME}."{label}_2023_data" PARTITION OF {GRAPH_NAME}."{label}_parent"
                FOR VALUES FROM ('2023-01-01'::agtype) TO ('2024-01-01'::agtype);
            """)
            cur.execute(f"""
                CREATE TABLE {GRAPH_NAME}."{label}_2024_data" PARTITION OF {GRAPH_NAME}."{label}_parent"
                FOR VALUES FROM ('2024-01-01'::agtype) TO ('2025-01-01'::agtype);
            """)
            cur.execute(f"""
                CREATE TABLE {GRAPH_NAME}."{label}_2025_data" PARTITION OF {GRAPH_NAME}."{label}_parent"
                FOR VALUES FROM ('2025-01-01'::agtype) TO ('2026-01-01'::agtype);
            """)
            # You might also add a DEFAULT partition for data outside defined ranges
            # CREATE TABLE {GRAPH_NAME}."{label}_default_data" PARTITION OF {GRAPH_NAME}."{label}_parent" DEFAULT;


            # Indexing:
            # Primary key index on the AGTYPE ID column for fast lookups
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING BTREE (id);")
            # Secondary key index on a specific key ('name') for a frequently queried property
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING BTREE (agtype_access_operator(VARIADIC ARRAY[properties, '\"name\"'::agtype]));")
            # GIN index on the properties column for general key-value searches
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING GIN (properties);")


        # --- Indexing for Edges ---
        print("\nCreating tables and indexes for all edge labels...")
        for label in EDGE_LABELS:
            print(f"- Processing edge label: '{label}'")
            
            # Create the edge table, now including a 'properties' column
            cur.execute(f"""
                CREATE TABLE {GRAPH_NAME}."{label}" (
                    id AGTYPE,
                    start_id AGTYPE,
                    end_id AGTYPE,
                    properties AGTYPE
                );
            """)
            
            # Indexing:
            # B-Tree index on the ID column
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (id);")
            # B-Tree indexes on the start and end IDs for fast traversals
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (start_id);")
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (end_id);")
            # GIN index on the properties column for general key-value searches on edge properties
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);")

        print("\nSchema creation complete!")
        print(f"- Graph '{GRAPH_NAME}' created.")
        print(f"- Tables and indexes for {len(VERTEX_LABELS)} vertex labels and {len(EDGE_LABELS)} edge labels created.")
        print("- Vertex tables are now partitioned by 'created_at' date property.")
        print("- Edge tables include a 'properties' column and a GIN index on it.")

    except (Exception, psycopg.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    create_schema()
