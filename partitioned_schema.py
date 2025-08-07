def create_truly_partitioned_schema():
    """
    Actually implements partitioning for Apache AGE tables.
    WARNING: This is complex and may break AGE's assumptions.
    Use only if you understand the implications.
    """
    conn = None
    try:
        conn = psycopg.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Step 1: Load AGE and create basic graph structure
        print("Setting up AGE...")
        cur.execute("LOAD 'age';")
        cur.execute(f"SET search_path TO ag_catalog, '$user', public;")
        
        # Create the graph
        print(f"Creating graph '{GRAPH_NAME}'...")
        cur.execute(f"SELECT * FROM ag_catalog.create_graph('{GRAPH_NAME}');")
        
        # Create labels first (this creates the basic tables)
        print("Creating basic AGE labels...")
        for label in VERTEX_LABELS:
            cur.execute(f"SELECT * FROM ag_catalog.create_vlabel('{GRAPH_NAME}', '{label}');")
        
        for label in EDGE_LABELS:
            cur.execute(f"SELECT * FROM ag_catalog.create_elabel('{GRAPH_NAME}', '{label}');")

        # Step 2: Now implement partitioning on selected large tables
        print("\nImplementing partitioning on large vertex tables...")
        
        # Choose which tables to partition (typically the largest ones)
        LARGE_VERTEX_LABELS = ["Person", "Product"]  # Adjust based on your data size expectations
        
        for label in LARGE_VERTEX_LABELS:
            table_name = f"{GRAPH_NAME}.{label.lower()}"
            temp_table = f"{GRAPH_NAME}.{label.lower()}_temp"
            
            try:
                print(f"- Partitioning table: {table_name}")
                
                # Step 2a: Create a temporary backup of existing data
                cur.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {table_name};")
                
                # Step 2b: Drop the original table
                cur.execute(f"DROP TABLE {table_name};")
                
                # Step 2c: Create partitioned version
                # Partition by ID range (adjust ranges based on your data)
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id BIGINT,
                        start_id BIGINT,
                        end_id BIGINT,
                        properties JSONB
                    ) PARTITION BY RANGE (id);
                """)
                
                # Create partitions
                cur.execute(f"""
                    CREATE TABLE {table_name}_p1 PARTITION OF {table_name}
                    FOR VALUES FROM (0) TO (1000000);
                """)
                cur.execute(f"""
                    CREATE TABLE {table_name}_p2 PARTITION OF {table_name}
                    FOR VALUES FROM (1000000) TO (2000000);
                """)
                cur.execute(f"""
                    CREATE TABLE {table_name}_p3 PARTITION OF {table_name}
                    FOR VALUES FROM (2000000) TO (MAXVALUE);
                """)
                
                # Step 2d: Restore data
                cur.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table};")
                
                # Step 2e: Clean up
                cur.execute(f"DROP TABLE {temp_table};")
                
                print(f"  ✓ Successfully partitioned {table_name}")
                
            except Exception as e:
                print(f"  ✗ Error partitioning {table_name}: {e}")
                # Try to restore from backup
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
                    cur.execute(f"ALTER TABLE {temp_table} RENAME TO {label.lower()};")
                except:
                    pass

        # Step 3: Add indexes to all tables (partitioned and non-partitioned)
        print("\nAdding indexes...")
        
        for label in VERTEX_LABELS:
            table_name = f"{GRAPH_NAME}.{label.lower()}"
            try:
                # Primary index on ID
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_id ON {table_name} (id);")
                
                # GIN index on properties
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_properties_gin ON {table_name} USING GIN (properties);")
                
                print(f"  ✓ Indexed {table_name}")
            except Exception as e:
                print(f"  ✗ Error indexing {table_name}: {e}")

        for label in EDGE_LABELS:
            table_name = f"{GRAPH_NAME}.{label.lower()}"
            try:
                # Indexes for graph traversals
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_start ON {table_name} (start_id);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_end ON {table_name} (end_id);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_properties_gin ON {table_name} USING GIN (properties);")
                
                print(f"  ✓ Indexed {table_name}")
            except Exception as e:
                print(f"  ✗ Error indexing {table_name}: {e}")

        print("\nPartitioned schema creation complete!")

    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn:
            conn.close()


def create_simple_indexed_schema():
    """
    Simplified version that focuses on proper indexing without partitioning.
    This is recommended for most use cases.
    """
    conn = None
    try:
        conn = psycopg.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Setup AGE
        print("Loading AGE extension...")
        cur.execute("LOAD 'age';")
        cur.execute(f"SET search_path TO ag_catalog, '$user', public;")

        # Create graph
        print(f"Creating graph '{GRAPH_NAME}'...")
        cur.execute(f"SELECT * FROM ag_catalog.create_graph('{GRAPH_NAME}');")

        # Create labels
        print("Creating vertex labels...")
        for label in VERTEX_LABELS:
            cur.execute(f"SELECT * FROM ag_catalog.create_vlabel('{GRAPH_NAME}', '{label}');")
            print(f"  ✓ Created vertex label: {label}")

        print("Creating edge labels...")
        for label in EDGE_LABELS:
            cur.execute(f"SELECT * FROM ag_catalog.create_elabel('{GRAPH_NAME}', '{label}');")
            print(f"  ✓ Created edge label: {label}")

        # Add proper indexes with correct case handling
        print("Adding optimized indexes...")
        
        for label in VERTEX_LABELS:
            table_name = f"{GRAPH_NAME}.{label.lower()}"  # Fixed case issue
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_properties_gin ON {table_name} USING GIN (properties);")
                print(f"  ✓ Added GIN index to {label}")
            except Exception as e:
                print(f"  ✗ Failed to index {label}: {e}")

        for label in EDGE_LABELS:
            table_name = f"{GRAPH_NAME}.{label.lower()}"  # Fixed case issue
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_start ON {table_name} (start_id);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_end ON {table_name} (end_id);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{label.lower()}_properties_gin ON {table_name} USING GIN (properties);")
                print(f"  ✓ Added traversal indexes to {label}")
            except Exception as e:
                print(f"  ✗ Failed to index {label}: {e}")

        # Proper verification with correct column name
        print("\nVerifying creation...")
        cur.execute("SELECT name FROM ag_catalog.ag_graph;")
        graphs = [row[0] for row in cur.fetchall()]
        print(f"Available graphs: {graphs}")
        
        cur.execute(f"SELECT name, kind FROM ag_catalog.ag_label WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = '{GRAPH_NAME}');")
        labels = cur.fetchall()
        print(f"Created {len(labels)} labels: {labels}")

        print(f"\n✓ Schema creation complete! Graph '{GRAPH_NAME}' ready with proper indexing.")

    except Exception as error:
        print(f"Error: {error}")
    finally:
        if conn:
            conn.close()
