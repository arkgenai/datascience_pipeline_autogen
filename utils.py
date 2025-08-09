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
# Get table sizes and row counts
cur.execute(f"""
    SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
        pg_stat_get_tuplestat_n_tup_ins(c.oid) as row_count
    FROM pg_tables pt
    JOIN pg_class c ON c.relname = pt.tablename
    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = pt.schemaname
    WHERE schemaname = '{GRAPH_NAME}'
    ORDER BY tablename
""")
table_stats = cur.fetchall()
print(f"Table statistics for graph '{GRAPH_NAME}':")
for stat in table_stats:
    print(f"  - {stat[1]}: {stat[2]} ({stat[3] or 0} rows)")







def _execute_query(query: str, db_config: Dict[str, Any], graph_name: str) -> Optional[List[Dict[str, Any]]]:
    """Helper function to execute a SQL query and return results."""
    conn = None
    try:
        conn = psycopg.connect(**db_config)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("LOAD 'age';")
        # Set search path to ensure schema and functions are found
        cur.execute(f"SET search_path TO ag_catalog, '{graph_name}', '$user', public;")

        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]
        return results
    except Exception as e:
        print(f"Error executing query: {query}\nError details: {e}")
        return None
    finally:
        if conn:
            conn.close()

def view_non_partitioned_vertex_data(label: str, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Views data from a non-partitioned vertex table.
    e.g., "Person", "Company", "Department"
    """
    print(f"\n--- Viewing data for non-partitioned vertex '{label}' ---")
    table_name = f'"{GRAPH_NAME}"."{label}"'
    query = f"""
        SELECT
            id AS age_internal_id,
            properties AS vertex_properties
        FROM {table_name}
        LIMIT {limit};
    """
    return _execute_query(query, DB_CONFIG, GRAPH_NAME)

def view_partitioned_vertex_parent_data(label: str, start_date: str, end_date: str, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Views data from a partitioned vertex parent table, leveraging partition pruning.
    This works for labels like "Project" (partitioned by 'created_at') or "City" (partitioned by 'id').
    Provide 'start_date' and 'end_date' as 'YYYY-MM-DD' strings for date-partitioned tables.
    For ID-partitioned tables like "City", replace start_date/end_date with appropriate ID strings.
    """
    print(f"\n--- Viewing data for partitioned vertex parent '{label}' (filtered by date) ---")
    table_name = f'"{GRAPH_NAME}"."{label}_parent"'
    
    # This query assumes 'created_at' for date partitioning, adjust if 'id' or other key
    # For City (ID partitioned), you'd pass string IDs like '100' and '500' for start_date/end_date
    # And change agtype_access_operator to '"id"' instead of '"created_at"'
    query = f"""
        SELECT
            id AS age_internal_id,
            properties AS vertex_properties
        FROM {table_name}
        WHERE agtype_access_operator(properties, '"created_at"'::agtype) >= '{start_date}'::agtype
        AND agtype_access_operator(properties, '"created_at"'::agtype) < '{end_date}'::agtype
        LIMIT {limit};
    """
    print(f"Executing query with filter: created_at from {start_date} to {end_date}")
    return _execute_query(query, DB_CONFIG, GRAPH_NAME)

def view_specific_child_partition_data(label: str, partition_suffix: str, limit: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Views data from a specific child partition table.
    e.g., "Project_2024_data" or "City_id_part_1_1000"
    """
    print(f"\n--- Viewing data for specific child partition '{label}_{partition_suffix}' ---")
    table_name = f'"{GRAPH_NAME}"."{label}_{partition_suffix}"'
    query = f"""
        SELECT
            id AS age_internal_id,
            properties AS vertex_properties
        FROM {table_name}
        LIMIT {limit};
    """
    return _execute_query(query, DB_CONFIG, GRAPH_NAME)


if __name__ == "__main__":
    # --- Example Usage ---

    # 1. View data from a Non-Partitioned Vertex Table (e.g., "Person" or "Department")
    person_data = view_non_partitioned_vertex_data("Person", limit=5)
    if person_data:
        for row in person_data:
            print(f"  {row}")

    department_data = view_non_partitioned_vertex_data("Department", limit=3)
    if department_data:
        for row in department_data:
            print(f"  {row}")

    # 2. View data from a Partitioned Vertex Parent Table (e.g., "Project" by date)
    # This will use partition pruning if the date range matches an existing partition
    project_2024_data = view_partitioned_vertex_parent_data("Project", "2024-01-01", "2025-01-01", limit=5)
    if project_2024_data:
        for row in project_2024_data:
            print(f"  {row}")

    # Example for an ID-partitioned table ("City") - adjust function call accordingly
    # You would need a separate function or modify view_partitioned_vertex_parent_data
    # to accept ID ranges instead of date strings for the WHERE clause.
    # For now, illustrating how you'd conceptualize it:
    # city_id_data = view_partitioned_vertex_parent_data("City", "1", "1000", limit=5) # This would require function mod.

    # 3. View data from a Specific Child Partition (e.g., "Project_2024_data")
    specific_project_part_data = view_specific_child_partition_data("Project", "2024_data", limit=5)
    if specific_project_part_data:
        for row in specific_project_part_data:
            print(f"  {row}")

    # Example for a City child partition
    specific_city_part_data = view_specific_child_partition_data("City", "id_part_1_1000", limit=5)
    if specific_city_part_data:
        for row in specific_city_part_data:
            print(f"  {row}")
