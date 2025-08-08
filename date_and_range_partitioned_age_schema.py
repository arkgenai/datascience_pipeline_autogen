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
        print("\nImplementing partitioning on large vertex and edge tables...")
        
        # Define partitioning strategies
        PARTITION_CONFIG = {
            # Vertex tables with date-based partitioning
            "date_partitioned_vertices": {
                "labels": ["Person", "Employee", "Customer"],  # Tables with date fields
                "date_column": "created_at",  # Property name in JSONB
                "partition_type": "date"
            },
            # Vertex tables with ID-based partitioning
            "id_partitioned_vertices": {
                "labels": ["Product", "Company"],
                "partition_type": "id"
            },
            # Edge tables with date-based partitioning
            "date_partitioned_edges": {
                "labels": ["WORKS_AT", "PURCHASED"],  # Relationship tables with temporal data
                "date_column": "start_date",
                "partition_type": "date"
            },
            # Edge tables with ID-based partitioning
            "id_partitioned_edges": {
                "labels": ["LOCATED_IN", "MANUFACTURED_BY"],
                "partition_type": "id"
            }
        }
        
        # Process date-partitioned vertex tables
        for label in PARTITION_CONFIG["date_partitioned_vertices"]["labels"]:
            if label in VERTEX_LABELS:
                table_name = f"{GRAPH_NAME}.{label.lower()}"
                temp_table = f"{GRAPH_NAME}.{label.lower()}_temp"
                date_column = PARTITION_CONFIG["date_partitioned_vertices"]["date_column"]
                
                try:
                    print(f"- Creating date-partitioned vertex table: {table_name}")
                    
                    # Backup existing data
                    cur.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {table_name};")
                    cur.execute(f"DROP TABLE {table_name};")
                    
                    # Create date-partitioned table
                    cur.execute(f"""
                        CREATE TABLE {table_name} (
                            id BIGINT,
                            start_id BIGINT,
                            end_id BIGINT,
                            properties JSONB
                        ) PARTITION BY RANGE ((properties->>'{date_column}')::DATE);
                    """)
                    
                    # Create date-based partitions
                    from datetime import datetime, timedelta
                    
                    # Current year partitions
                    current_year = datetime.now().year
                    for year in range(current_year - 2, current_year + 2):  # 4 years of partitions
                        cur.execute(f"""
                            CREATE TABLE {table_name}_y{year} PARTITION OF {table_name}
                            FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01');
                        """)
                    
                    # Default partition for dates outside range
                    cur.execute(f"""
                        CREATE TABLE {table_name}_default PARTITION OF {table_name}
                        DEFAULT;
                    """)
                    
                    # Restore data (only if properties contain the date column)
                    cur.execute(f"""
                        INSERT INTO {table_name} 
                        SELECT * FROM {temp_table}
                        WHERE properties ? '{date_column}';
                    """)
                    
                    # Handle records without date in default partition by updating properties
                    cur.execute(f"""
                        INSERT INTO {table_name} 
                        SELECT 
                            id, start_id, end_id,
                            properties || jsonb_build_object('{date_column}', CURRENT_DATE)
                        FROM {temp_table}
                        WHERE NOT (properties ? '{date_column}');
                    """)
                    
                    cur.execute(f"DROP TABLE {temp_table};")
                    print(f"  ✓ Successfully created date-partitioned vertex table: {table_name}")
                    
                except Exception as e:
                    print(f"  ✗ Error creating date-partitioned vertex table {table_name}: {e}")
                    self._restore_from_backup(cur, table_name, temp_table, label)
        
        # Process ID-partitioned vertex tables
        for label in PARTITION_CONFIG["id_partitioned_vertices"]["labels"]:
            if label in VERTEX_LABELS:
                table_name = f"{GRAPH_NAME}.{label.lower()}"
                temp_table = f"{GRAPH_NAME}.{label.lower()}_temp"
                
                try:
                    print(f"- Creating ID-partitioned vertex table: {table_name}")
                    
                    cur.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {table_name};")
                    cur.execute(f"DROP TABLE {table_name};")
                    
                    # Create ID-partitioned table
                    cur.execute(f"""
                        CREATE TABLE {table_name} (
                            id BIGINT,
                            start_id BIGINT,
                            end_id BIGINT,
                            properties JSONB
                        ) PARTITION BY RANGE (id);
                    """)
                    
                    # Create ID-based partitions
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
                    
                    cur.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table};")
                    cur.execute(f"DROP TABLE {temp_table};")
                    print(f"  ✓ Successfully created ID-partitioned vertex table: {table_name}")
                    
                except Exception as e:
                    print(f"  ✗ Error creating ID-partitioned vertex table {table_name}: {e}")
                    self._restore_from_backup(cur, table_name, temp_table, label)
        
        # Process date-partitioned edge tables
        for label in PARTITION_CONFIG["date_partitioned_edges"]["labels"]:
            if label in EDGE_LABELS:
                table_name = f"{GRAPH_NAME}.{label.lower()}"
                temp_table = f"{GRAPH_NAME}.{label.lower()}_temp"
                date_column = PARTITION_CONFIG["date_partitioned_edges"]["date_column"]
                
                try:
                    print(f"- Creating date-partitioned edge table: {table_name}")
                    
                    cur.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {table_name};")
                    cur.execute(f"DROP TABLE {table_name};")
                    
                    # Create date-partitioned edge table
                    cur.execute(f"""
                        CREATE TABLE {table_name} (
                            id BIGINT,
                            start_id BIGINT,
                            end_id BIGINT,
                            properties JSONB
                        ) PARTITION BY RANGE ((properties->>'{date_column}')::DATE);
                    """)
                    
                    # Create date-based partitions for edges
                    current_year = datetime.now().year
                    for year in range(current_year - 5, current_year + 2):  # More historical data for relationships
                        cur.execute(f"""
                            CREATE TABLE {table_name}_y{year} PARTITION OF {table_name}
                            FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01');
                        """)
                    
                    # Default partition
                    cur.execute(f"""
                        CREATE TABLE {table_name}_default PARTITION OF {table_name}
                        DEFAULT;
                    """)
                    
                    # Restore data with date validation
                    cur.execute(f"""
                        INSERT INTO {table_name} 
                        SELECT * FROM {temp_table}
                        WHERE properties ? '{date_column}';
                    """)
                    
                    # Handle records without date
                    cur.execute(f"""
                        INSERT INTO {table_name} 
                        SELECT 
                            id, start_id, end_id,
                            properties || jsonb_build_object('{date_column}', CURRENT_DATE)
                        FROM {temp_table}
                        WHERE NOT (properties ? '{date_column}');
                    """)
                    
                    cur.execute(f"DROP TABLE {temp_table};")
                    print(f"  ✓ Successfully created date-partitioned edge table: {table_name}")
                    
                except Exception as e:
                    print(f"  ✗ Error creating date-partitioned edge table {table_name}: {e}")
                    self._restore_from_backup(cur, table_name, temp_table, label)
        
        # Process ID-partitioned edge tables
        for label in PARTITION_CONFIG["id_partitioned_edges"]["labels"]:
            if label in EDGE_LABELS:
                table_name = f"{GRAPH_NAME}.{label.lower()}"
                temp_table = f"{GRAPH_NAME}.{label.lower()}_temp"
                
                try:
                    print(f"- Creating ID-partitioned edge table: {table_name}")
                    
                    cur.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {table_name};")
                    cur.execute(f"DROP TABLE {table_name};")
                    
                    # Create ID-partitioned edge table
                    cur.execute(f"""
                        CREATE TABLE {table_name} (
                            id BIGINT,
                            start_id BIGINT,
                            end_id BIGINT,
                            properties JSONB
                        ) PARTITION BY RANGE (id);
                    """)
                    
                    # Create ID-based partitions
                    cur.execute(f"""
                        CREATE TABLE {table_name}_p1 PARTITION OF {table_name}
                        FOR VALUES FROM (0) TO (500000);
                    """)
                    cur.execute(f"""
                        CREATE TABLE {table_name}_p2 PARTITION OF {table_name}
                        FOR VALUES FROM (500000) TO (1000000);
                    """)
                    cur.execute(f"""
                        CREATE TABLE {table_name}_p3 PARTITION OF {table_name}
                        FOR VALUES FROM (1000000) TO (MAXVALUE);
                    """)
                    
                    cur.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table};")
                    cur.execute(f"DROP TABLE {temp_table};")
                    print(f"  ✓ Successfully created ID-partitioned edge table: {table_name}")
                    
                except Exception as e:
                    print(f"  ✗ Error creating ID-partitioned edge table {table_name}: {e}")
                    self._restore_from_backup(cur, table_name, temp_table, label)
    
    def _restore_from_backup(self, cur, table_name, temp_table, label):
        """Helper method to restore table from backup on partition failure"""
        try:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            cur.execute(f"ALTER TABLE {temp_table} RENAME TO {label.lower()};")
            print(f"  ↻ Restored original table: {table_name}")
        except Exception as restore_error:
            print(f"  ✗ Failed to restore table {table_name}: {restore_error}")

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


def add_future_date_partitions(graph_name: str, table_name: str, years_ahead: int = 2):
    """
    Utility function to add future date partitions to existing partitioned tables.
    This should be run periodically (e.g., annually) to maintain partitions.
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

        from datetime import datetime
        current_year = datetime.now().year
        
        for year in range(current_year + 1, current_year + years_ahead + 1):
            try:
                partition_name = f"{table_name}_y{year}"
                cur.execute(f"""
                    CREATE TABLE {partition_name} PARTITION OF {graph_name}.{table_name}
                    FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01');
                """)
                print(f"✓ Created future partition: {partition_name}")
            except Exception as e:
                print(f"✗ Failed to create partition for {year}: {e}")
                
    except Exception as error:
        print(f"Error adding future partitions: {error}")
    finally:
        if conn:
            conn.close()


def cleanup_old_date_partitions(graph_name: str, table_name: str, keep_years: int = 3):
    """
    Utility function to remove old date partitions and archive data.
    WARNING: This will permanently delete data!
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

        from datetime import datetime
        current_year = datetime.now().year
        cutoff_year = current_year - keep_years
        
        # List existing partitions
        cur.execute(f"""
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname = '{graph_name}' 
            AND tablename LIKE '{table_name}_y%'
            AND tablename NOT LIKE '%_default'
            ORDER BY tablename;
        """)
        
        partitions = cur.fetchall()
        
        for schema, partition_name in partitions:
            # Extract year from partition name
            try:
                year_str = partition_name.split('_y')[-1]
                year = int(year_str)
                
                if year < cutoff_year:
                    # Optional: Export data before deletion
                    print(f"⚠ Warning: About to drop old partition {partition_name} (year {year})")
                    
                    # Uncomment these lines to actually perform cleanup:
                    # cur.execute(f"DROP TABLE {schema}.{partition_name};")
                    # print(f"✓ Dropped old partition: {partition_name}")
                    
            except ValueError:
                print(f"✗ Could not parse year from partition name: {partition_name}")
                
    except Exception as error:
        print(f"Error cleaning up old partitions: {error}")
    finally:
        if conn:
            conn.close()


def get_partition_info(graph_name: str) -> Dict[str, List[str]]:
    """
    Get information about all partitions in the graph
    """
    conn = None
    partition_info = {}
    
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

        # Get all tables and their partitions
        cur.execute(f"""
            SELECT 
                pt.schemaname,
                pt.tablename as parent_table,
                c.relname as partition_name,
                pg_get_expr(c.relpartbound, c.oid) as partition_bounds
            FROM pg_partitioned_table p
            JOIN pg_class pc ON p.partrelid = pc.oid
            JOIN pg_namespace pn ON pc.relnamespace = pn.oid
            JOIN pg_inherits i ON i.inhparent = pc.oid
            JOIN pg_class c ON i.inhrelid = c.oid
            JOIN pg_tables pt ON pt.tablename = pc.relname AND pt.schemaname = pn.nspname
            WHERE pn.nspname = '{graph_name}'
            ORDER BY pt.tablename, c.relname;
        """)
        
        results = cur.fetchall()
        
        for schema, parent_table, partition_name, bounds in results:
            if parent_table not in partition_info:
                partition_info[parent_table] = []
            partition_info[parent_table].append({
                'partition_name': partition_name,
                'bounds': bounds
            })
                
    except Exception as error:
        print(f"Error getting partition info: {error}")
    finally:
        if conn:
            conn.close()
    
    return partition_info
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
