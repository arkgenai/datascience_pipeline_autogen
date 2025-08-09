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
    This version applies:
    - Date partitioning to 'Project' and 'City' vertex labels.
    - Specific indexing to 'Department' vertex label.
    - Specific indexing to 'SUPPLIES' edge label.
    - Standard indexing to other labels.
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
        print(f"Initializing graph '{GRAPH_NAME}'...\n")
        cur.execute(f"SELECT * FROM ag_catalog.create_graph('{GRAPH_NAME}');")

        # --- Partitioning and Indexing for Vertices ---
        print("\nCreating tables and indexes for all vertex labels...")
        for label in VERTEX_LABELS:
            print(f"- Processing vertex label: '{label}'")
            
            # Check if this vertex label should be partitioned by date
            if label in ["Project", "City"]:
                print(f"  --> Applying date partitioning for '{label}'")
                # Create the parent table for partitioning by the 'created_at' property
                cur.execute(f"""
                    CREATE TABLE {GRAPH_NAME}."{label}_parent" (
                        id AGTYPE,
                        properties AGTYPE
                    ) PARTITION BY RANGE (agtype_access_operator(VARIADIC ARRAY[properties, '"created_at"'::agtype]));
                """)

                # Create partitions for specific date ranges (e.g., by year)
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
                # cur.execute(f"CREATE TABLE {GRAPH_NAME}.\"{label}_default_data\" PARTITION OF {GRAPH_NAME}.\"{label}_parent\" DEFAULT;")

                # Indexing for partitioned tables (applied to the parent)
                print(f"  --> Applying specific indexing for partitioned '{label}'")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING BTREE (id);")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING BTREE (agtype_access_operator(VARIADIC ARRAY[properties, '\"name\"'::agtype]));")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING GIN (properties);")
            
            else: # For other vertex labels (not partitioned)
                # Create a non-partitioned table for the vertex label
                cur.execute(f"""
                    CREATE TABLE {GRAPH_NAME}."{label}" (
                        id AGTYPE,
                        properties AGTYPE
                    );
                """)
                print(f"  --> Created non-partitioned table for '{label}'")

                # Apply indexing based on specific requests or standard practice
                if label == "Department":
                    print(f"  --> Applying specific indexing for '{label}'")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (id);")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (agtype_access_operator(VARIADIC ARRAY[properties, '\"name\"'::agtype]));")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);")
                else: # Standard indexing for other non-partitioned vertex labels
                    print(f"  --> Applying standard indexing for '{label}'")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (id);")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);")


        # --- Indexing for Edges ---
        print("\nCreating tables and indexes for all edge labels...")
        for label in EDGE_LABELS:
            print(f"- Processing edge label: '{label}'")
            
            # Create the edge table, including the 'properties' column
            cur.execute(f"""
                CREATE TABLE {GRAPH_NAME}."{label}" (
                    id AGTYPE,
                    start_id AGTYPE,
                    end_id AGTYPE,
                    properties AGTYPE
                );
            """)
            print(f"  --> Created table for edge label '{label}'")
            
            # Indexing:
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (id);")
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (start_id);")
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (end_id);")
            
            # Apply GIN index on properties specifically for 'SUPPLIES' or other general cases
            if label == "SUPPLIES":
                print(f"  --> Applying specific GIN indexing for '{label}' properties.")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);")
            else:
                print(f"  --> Applying standard GIN indexing for '{label}' properties.")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);") # Keep GIN for all edges for good practice


        print("\nSchema creation complete!")
        print(f"- Graph '{GRAPH_NAME}' created.")
        print(f"- Tables and indexes for {len(VERTEX_LABELS)} vertex labels and {len(EDGE_LABELS)} edge labels created.")
        print("- Vertex tables 'Project' and 'City' are now partitioned by 'created_at' date property.")
        print("- Vertex table 'Department' has specific indexing (id, name, properties GIN).")
        print("- Edge table 'SUPPLIES' has specific GIN indexing on properties.")

    except (Exception, psycopg.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    create_schema()






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
    This version applies:
    - Date partitioning to 'Project' vertex label (using 'created_at' property).
    - ID partitioning to 'City' vertex label (using 'id' property).
    - Specific indexing to 'Department' vertex label.
    - Specific indexing to 'SUPPLIES' edge label.
    - Standard indexing to other labels.
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
        print(f"Initializing graph '{GRAPH_NAME}'...\n")
        cur.execute(f"SELECT * FROM ag_catalog.create_graph('{GRAPH_NAME}');")

        # --- Partitioning and Indexing for Vertices ---
        print("\nCreating tables and indexes for all vertex labels...")
        for label in VERTEX_LABELS:
            print(f"- Processing vertex label: '{label}'")
            
            if label == "Project":
                print(f"  --> Applying date partitioning for '{label}'")
                # Create the parent table for partitioning by the 'created_at' property
                cur.execute(f"""
                    CREATE TABLE {GRAPH_NAME}."{label}_parent" (
                        id AGTYPE,
                        properties AGTYPE
                    ) PARTITION BY RANGE (agtype_access_operator(VARIADIC ARRAY[properties, '"created_at"'::agtype]));
                """)

                # Create partitions for specific date ranges (e.g., by year)
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
                # Optional: Add a DEFAULT partition for data outside defined ranges
                # cur.execute(f"CREATE TABLE {GRAPH_NAME}.\"{label}_default_data\" PARTITION OF {GRAPH_NAME}.\"{label}_parent\" DEFAULT;")

                # Indexing for partitioned tables (applied to the parent)
                print(f"  --> Applying specific indexing for partitioned '{label}'")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING BTREE (id);")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING BTREE (agtype_access_operator(VARIADIC ARRAY[properties, '\"name\"'::agtype]));")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING GIN (properties);")
            
            elif label == "City": # Assuming "Located_at" was a typo and meant "City" vertex label
                print(f"  --> Applying ID partitioning for '{label}'")
                # Create the parent table for partitioning by the 'id' property
                cur.execute(f"""
                    CREATE TABLE {GRAPH_NAME}."{label}_parent" (
                        id AGTYPE,
                        properties AGTYPE
                    ) PARTITION BY RANGE (agtype_access_operator(VARIADIC ARRAY[properties, '"id"'::agtype]));
                """)

                # Create partitions for specific ID ranges
                cur.execute(f"""
                    CREATE TABLE {GRAPH_NAME}."{label}_id_part_1_1000" PARTITION OF {GRAPH_NAME}."{label}_parent"
                    FOR VALUES FROM ('1'::agtype) TO ('1000'::agtype);
                """)
                cur.execute(f"""
                    CREATE TABLE {GRAPH_NAME}."{label}_id_part_1001_2000" PARTITION OF {GRAPH_NAME}."{label}_parent"
                    FOR VALUES FROM ('1001'::agtype) TO ('2000'::agtype);
                """)
                cur.execute(f"""
                    CREATE TABLE {GRAPH_NAME}."{label}_id_part_2001_MAX" PARTITION OF {GRAPH_NAME}."{label}_parent"
                    FOR VALUES FROM ('2001'::agtype) TO (MAXVALUE);
                """)

                # Indexing for partitioned tables (applied to the parent)
                print(f"  --> Applying specific indexing for partitioned '{label}'")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING BTREE (id);")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING BTREE (agtype_access_operator(VARIADIC ARRAY[properties, '\"name\"'::agtype]));")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}_parent\" USING GIN (properties);")

            else: # For other vertex labels (not partitioned)
                # Create a non-partitioned table for the vertex label
                cur.execute(f"""
                    CREATE TABLE {GRAPH_NAME}."{label}" (
                        id AGTYPE,
                        properties AGTYPE
                    );
                """)
                print(f"  --> Created non-partitioned table for '{label}'")

                # Apply indexing based on specific requests or standard practice
                if label == "Department":
                    print(f"  --> Applying specific indexing for '{label}'")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (id);")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (agtype_access_operator(VARIADIC ARRAY[properties, '\"name\"'::agtype]));")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);")
                else: # Standard indexing for other non-partitioned vertex labels
                    print(f"  --> Applying standard indexing for '{label}'")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (id);")
                    cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);")


        # --- Indexing for Edges ---
        print("\nCreating tables and indexes for all edge labels...")
        for label in EDGE_LABELS:
            print(f"- Processing edge label: '{label}'")
            
            # Create the edge table, including the 'properties' column
            cur.execute(f"""
                CREATE TABLE {GRAPH_NAME}."{label}" (
                    id AGTYPE,
                    start_id AGTYPE,
                    end_id AGTYPE,
                    properties AGTYPE
                );
            """)
            print(f"  --> Created table for edge label '{label}'")
            
            # Indexing:
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (id);")
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (start_id);")
            cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING BTREE (end_id);")
            
            # Apply GIN index on properties specifically for 'SUPPLIES' or other general cases
            if label == "SUPPLIES":
                print(f"  --> Applying specific GIN indexing for '{label}' properties.")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);")
            else:
                print(f"  --> Applying standard GIN indexing for '{label}' properties.")
                cur.execute(f"CREATE INDEX ON {GRAPH_NAME}.\"{label}\" USING GIN (properties);")


        print("\nSchema creation complete!")
        print(f"- Graph '{GRAPH_NAME}' created.")
        print(f"- Tables and indexes for {len(VERTEX_LABELS)} vertex labels and {len(EDGE_LABELS)} edge labels created.")
        print("- Vertex table 'Project' is partitioned by 'created_at' date property.")
        print("- Vertex table 'City' is partitioned by 'id' property.")
        print("- Vertex table 'Department' has specific indexing (id, name, properties GIN).")
        print("- Edge table 'SUPPLIES' has specific GIN indexing on properties.")

    except (Exception, psycopg.DatabaseError) as error:
        print(f"Error: {error}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    create_schema()





import psycopg
import json
import datetime
import random
from typing import Dict, List, Any

# --- Database Connection Configuration ---
# Update these with your actual PostgreSQL credentials.
DB_NAME = "your_db_name"
DB_USER = "your_user"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = 5432

# The name of the graph created in schema.py
GRAPH_NAME = "my_graph"

# --- Helper Function for Property Conversion ---
def _convert_properties_to_agtype(properties: Dict[str, Any]) -> str:
    """Convert Python dict to AGE AGTYPE format for Cypher."""
    # Ensure properties that are partitioning keys are handled correctly as strings
    # For dates, format as 'YYYY-MM-DD' string for ::agtype casting in SQL
    converted = {}
    for key, value in properties.items():
        if isinstance(value, datetime.date):
            converted[key] = value.isoformat() # Convert date object to 'YYYY-MM-DD' string
        elif isinstance(value, datetime.datetime):
            converted[key] = value.isoformat() # Convert datetime object to string
        elif isinstance(value, (int, float, bool)):
            converted[key] = value # Keep numbers and booleans as is
        else:
            converted[key] = str(value) # Ensure IDs are strings as expected by agtype_access_operator with ::agtype
    return json.dumps(converted)


# --- Sample Data Generation ---
def create_sample_data() -> List[tuple]:
    """
    Generates sample triplet data (source, relationship, target)
    including properties for vertices and edges, aligning with schema partitions.
    """
    sample_data = []

    # Vertex IDs for City for ID-based partitioning
    city_ids = [50, 1500, 2500, 3500] # Examples hitting different partitions, or MAXVALUE

    # Project creation dates for Date-based partitioning
    project_dates = [
        datetime.date(2023, 11, 15), # Falls into 2023 partition
        datetime.date(2024, 6, 20),  # Falls into 2024 partition
        datetime.date(2025, 1, 10),  # Falls into 2025 partition
    ]

    # Sample Vertices
    # Person (non-partitioned, standard indexing)
    sample_data.append(("Person", {"id": str(uuid.uuid4()), "name": "Alice Smith", "age": 30}, None, None, None, None))
    sample_data.append(("Person", {"id": str(uuid.uuid4()), "name": "Bob Johnson", "age": 45}, None, None, None, None))

    # Company (non-partitioned, standard indexing)
    sample_data.append(("Company", {"id": str(uuid.uuid4()), "name": "InnovateX Corp", "industry": "Tech"}, None, None, None, None))
    sample_data.append(("Company", {"id": str(uuid.uuid4()), "name": "Global Solutions Ltd", "industry": "Consulting"}, None, None, None, None))

    # Product (non-partitioned, standard indexing)
    sample_data.append(("Product", {"id": str(uuid.uuid4()), "name": "Quantum Processor", "version": "1.0"}, None, None, None, None))

    # City (ID-partitioned)
    sample_data.append(("City", {"id": str(city_ids[0]), "name": "New York", "population": 8000000}, None, None, None, None))
    sample_data.append(("City", {"id": str(city_ids[1]), "name": "Los Angeles", "population": 4000000}, None, None, None, None))
    sample_data.append(("City", {"id": str(city_ids[2]), "name": "Chicago", "population": 2700000}, None, None, None, None))

    # Country (non-partitioned, standard indexing)
    sample_data.append(("Country", {"id": str(uuid.uuid4()), "name": "USA", "continent": "North America"}, None, None, None, None))

    # Employee (non-partitioned, standard indexing)
    sample_data.append(("Employee", {"id": str(uuid.uuid4()), "employee_id": "E1001", "name": "David Lee"}, None, None, None, None))

    # Customer (non-partitioned, standard indexing)
    sample_data.append(("Customer", {"id": str(uuid.uuid4()), "customer_id": "C2001", "name": "Emily White"}, None, None, None, None))

    # Supplier (non-partitioned, standard indexing)
    sample_data.append(("Supplier", {"id": str(uuid.uuid4()), "supplier_id": "S3001", "name": "Raw Materials Inc."}, None, None, None, None))

    # Department (non-partitioned, specific indexing)
    sample_data.append(("Department", {"id": str(uuid.uuid4()), "name": "Engineering", "budget": 1000000}, None, None, None, None))
    sample_data.append(("Department", {"id": str(uuid.uuid4()), "name": "Marketing", "budget": 500000}, None, None, None, None))

    # Project (Date-partitioned)
    sample_data.append(("Project", {"id": str(uuid.uuid4()), "name": "Project Alpha", "status": "Active", "created_at": project_dates[0]}, None, None, None, None))
    sample_data.append(("Project", {"id": str(uuid.uuid4()), "name": "Project Beta", "status": "Completed", "created_at": project_dates[1]}, None, None, None, None))
    sample_data.append(("Project", {"id": str(uuid.uuid4()), "name": "Project Gamma", "status": "Planning", "created_at": project_dates[2]}, None, None, None, None))


    # Sample Edges (Relationships)
    # Triplet format: (source_label, source_props, edge_label, edge_props, target_label, target_props)
    
    # WORKS_AT edge (standard indexing)
    sample_data.append(
        ("Person", {"id": "Alice Smith"}, "WORKS_AT", {"since": "2020-01-15", "role": "Software Engineer"},
         "Company", {"name": "InnovateX Corp"})
    )
    sample_data.append(
        ("Employee", {"employee_id": "E1001"}, "WORKS_AT", {"start_date": "2023-01-01", "department": "Engineering"},
         "Company", {"name": "InnovateX Corp"})
    )

    # LOCATED_IN edge (standard indexing)
    sample_data.append(
        ("Company", {"name": "InnovateX Corp"}, "LOCATED_IN", {"main_office": True},
         "City", {"id": str(city_ids[0])}) # Link to New York City
    )
    sample_data.append(
        ("Person", {"name": "Bob Johnson"}, "LOCATED_IN", {"current": True},
         "City", {"id": str(city_ids[1])}) # Link to Los Angeles
    )

    # PURCHASED edge (standard indexing)
    sample_data.append(
        ("Customer", {"customer_id": "C2001"}, "PURCHASED", {"order_date": "2024-05-10", "quantity": 1},
         "Product", {"name": "Quantum Processor"})
    )

    # MANUFACTURED_BY edge (standard indexing)
    sample_data.append(
        ("Product", {"name": "Quantum Processor"}, "MANUFACTURED_BY", {"production_line": "A", "batch_no": "XYZ789"},
         "Company", {"name": "InnovateX Corp"})
    )

    # SUPPLIES edge (specific indexing)
    sample_data.append(
        ("Supplier", {"supplier_id": "S3001"}, "SUPPLIES", {"contract_duration_months": 24, "material_type": "Semiconductors"},
         "Company", {"name": "InnovateX Corp"})
    )
    sample_data.append(
        ("Company", {"name": "InnovateX Corp"}, "SUPPLIES", {"contract_id": "INV-DELL-001"},
         "Company", {"name": "Global Solutions Ltd"})
    )

    # Link a Department to a Project
    sample_data.append(
        ("Department", {"name": "Engineering"}, "WORKS_ON", {"allocation_pct": 75},
         "Project", {"name": "Project Alpha"})
    )

    return sample_data


# --- Graph Ingestion Class ---
class AGEGraphIngestion:
    def __init__(self, db_config: Dict[str, str], graph_name: str):
        self.db_config = db_config
        self.graph_name = graph_name
        self.conn = None
        self.cur = None
        # This mapping will store AGE internal IDs for easier edge creation
        self.vertex_internal_id_mapping = {}

    def connect(self):
        try:
            self.conn = psycopg.connect(**self.db_config)
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
            self.cur.execute("LOAD 'age';")
            self.cur.execute("SET search_path TO ag_catalog, '$user', public;")
            print("Connected to database and loaded AGE extension.")
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            raise

    def disconnect(self(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def ingest_triplet(self,
                        source_label: str, source_props: Dict[str, Any],
                        edge_label: str, edge_props: Dict[str, Any],
                        target_label: str, target_props: Dict[str, Any]):
        try:
            # Convert properties to AGE AGTYPE format strings
            source_props_str = _convert_properties_to_agtype(source_props)
            target_props_str = _convert_properties_to_agtype(target_props)
            edge_props_str = _convert_properties_to_agtype(edge_props)

            # Use MERGE for vertices to prevent duplicates if running multiple times
            # Use CREATE for edges to allow multiple edges between same vertices
            cypher_query = f"""
                MERGE (a:{source_label} {source_props_str})
                MERGE (b:{target_label} {target_props_str})
                CREATE (a)-[r:{edge_label} {edge_props_str}]->(b)
                RETURN a, b, r
            """
            
            self.cur.execute(f"SELECT * FROM cypher('{self.graph_name}', $$ {cypher_query} $$) AS (s agtype, t agtype, r agtype);")
            
            # Optionally fetch results if you want to see the AGE IDs created
            # result = self.cur.fetchone()
            # print(f"Ingested: {result}")
            print(f"Ingested: ({source_label} {source_props.get('name', '') or source_props.get('id', '')})-[{edge_label} {edge_props.get('role', '') or edge_props.get('contract_id', '')}]->({target_label} {target_props.get('name', '') or target_props.get('id', '')})")

        except Exception as e:
            print(f"Error ingesting triplet: {e}")
            raise # Re-raise to see full traceback


# --- Main execution example ---
if __name__ == "__main__":
    # Database configuration (ensure this matches your PostgreSQL setup)
    DB_CONFIG = {
        "dbname": "your_db_name",
        "user": "your_user",
        "password": "your_password",
        "host": "localhost",
        "port": "5432"
    }
    
    # Graph name (ensure this matches the GRAPH_NAME in your schema.py)
    GRAPH_NAME = "my_graph" 
    
    with AGEGraphIngestion(DB_CONFIG, GRAPH_NAME) as ingestion:
        sample_data = create_sample_data()
        print(f"\n--- Starting ingestion of {len(sample_data)} graph elements ---")
        
        # Ingest vertices first, then edges (to ensure vertices exist)
        # Separate vertices and edges from the sample data
        vertices_to_ingest = []
        edges_to_ingest = []

        for item in sample_data:
            # Check if it's a vertex (no edge_label) or an edge (has edge_label)
            if item[2] is None: # Format: (label, props, None, None, None, None) for vertices
                vertices_to_ingest.append(item)
            else: # Format: (source_label, source_props, edge_label, edge_props, target_label, target_props) for edges
                edges_to_ingest.append(item)

        print("\nIngesting Vertices:")
        # Ingest vertices first to ensure they exist for edges
        for item in vertices_to_ingest:
            label, props, _, _, _, _ = item
            try:
                # Use a MERGE query to create vertex if it doesn't exist
                props_str = _convert_properties_to_agtype(props)
                cypher_query = f"MERGE (v:{label} {props_str}) RETURN id(v)"
                ingestion.cur.execute(f"SELECT * FROM cypher('{ingestion.graph_name}', $$ {cypher_query} $$) AS (id agtype);")
                ingestion.cur.fetchone() # Fetch to clear result
                print(f"  ✓ Created/Merged Vertex: {label} with props {props}")
                # Store a mapping for simpler edge creation later if needed
                # For this script, we're relying on MERGE by properties for edge linking
            except Exception as e:
                print(f"  ✗ Failed to create/merge vertex {label} with props {props}: {e}")

        print("\nIngesting Edges:")
        # Now ingest edges
        for item in edges_to_ingest:
            source_label, source_props, edge_label, edge_props, target_label, target_props = item
            ingestion.ingest_triplet(source_label, source_props, edge_label, edge_props, target_label, target_props)

        print("\n--- Ingestion completed! ---")


# --- Helper Function for Property Conversion ---
def _format_props_for_cypher_map(properties: Dict[str, Any]) -> str:
    """
    Converts a Python dictionary of properties into a string
    suitable for direct embedding into a Cypher property map.
    Example output: 'key1: "value1", key2: 123, date_key: "YYYY-MM-DD"'
    String values are single-quoted, numbers/booleans are unquoted.
    """
    parts = []
    for key, value in properties.items():
        if isinstance(value, str):
            # Escape any single quotes within the string itself for Cypher literal
            escaped_value = value.replace("'", "\\'")
            parts.append(f"{key}: '{escaped_value}'")
        elif isinstance(value, (int, float, bool)):
            parts.append(f"{key}: {value}") # Numbers and booleans do not need quotes in Cypher
        elif isinstance(value, datetime.date):
            parts.append(f"{key}: '{value.isoformat()}'") # Dates as quoted ISO strings
        elif isinstance(value, datetime.datetime):
            parts.append(f"{key}: '{value.isoformat()}'") # Datetimes as quoted ISO strings
        else:
            # Fallback for other types like UUID objects, convert to string and quote
            escaped_value = str(value).replace("'", "\\'")
            parts.append(f"{key}: '{escaped_value}'")
    return ", ".join(parts)
