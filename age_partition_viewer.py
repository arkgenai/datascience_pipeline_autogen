import psycopg
from typing import Dict, Any, List, Optional
import json
from datetime import datetime

class AGEPartitionViewer:
    """
    Comprehensive viewer for Apache AGE partitioned tables.
    Shows data across all partitions with detailed analysis.
    """
    
    def __init__(self, db_config: Dict[str, Any], graph_name: str = 'partitioned_graph'):
        self.db_config = db_config
        self.graph_name = graph_name
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg.connect(**self.db_config)
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            
            # Set up AGE environment
            self.cursor.execute("LOAD 'age';")
            self.cursor.execute(f"SET search_path TO ag_catalog, '{self.graph_name}', '$user', public;")
            
            print(f"✅ Connected to graph '{self.graph_name}'")
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Clean up database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("🔌 Connection closed")
    
    def view_all_partitions_info(self):
        """
        Show comprehensive information about all partitions in the graph.
        """
        print(f"\n🔍 PARTITION ANALYSIS for graph '{self.graph_name}'")
        print("="*80)
        
        try:
            # Get all partitioned tables in the graph schema
            self.cursor.execute("""
                SELECT 
                    pt.schemaname,
                    pt.tablename,
                    pt.partdef as partition_definition,
                    c.reltuples::bigint as estimated_rows
                FROM pg_partitioned_table ppt
                JOIN pg_class pc ON ppt.partrelid = pc.oid
                JOIN pg_namespace pn ON pc.relnamespace = pn.oid
                JOIN pg_tables pt ON pt.tablename = pc.relname AND pt.schemaname = pn.nspname
                LEFT JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = pn.oid
                WHERE pn.nspname = %s
                ORDER BY pt.tablename;
            """, (self.graph_name,))
            
            partitioned_tables = self.cursor.fetchall()
            
            if not partitioned_tables:
                print("⚠️  No partitioned tables found in this graph")
                return
            
            print(f"📊 Found {len(partitioned_tables)} partitioned tables:")
            print()
            
            for schema, table, partition_def, est_rows in partitioned_tables:
                print(f"🏗️  Table: {schema}.{table}")
                print(f"   📋 Partition Strategy: {partition_def}")
                print(f"   📊 Estimated Rows: {est_rows:,}" if est_rows else "   📊 Estimated Rows: Unknown")
                
                # Get all partitions for this table
                self.cursor.execute("""
                    SELECT 
                        c.relname as partition_name,
                        pg_get_expr(c.relpartbound, c.oid) as partition_bound,
                        c.reltuples::bigint as estimated_rows
                    FROM pg_inherits i
                    JOIN pg_class c ON i.inhrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE i.inhparent = (
                        SELECT oid FROM pg_class 
                        WHERE relname = %s AND relnamespace = (
                            SELECT oid FROM pg_namespace WHERE nspname = %s
                        )
                    )
                    ORDER BY c.relname;
                """, (table, schema))
                
                partitions = self.cursor.fetchall()
                
                if partitions:
                    print(f"   🔧 Partitions ({len(partitions)}):")
                    total_rows = 0
                    for part_name, part_bound, part_rows in partitions:
                        print(f"      • {part_name}: {part_bound} ({part_rows:,} rows)" if part_rows else f"      • {part_name}: {part_bound} (unknown rows)")
                        if part_rows:
                            total_rows += part_rows
                    print(f"   📈 Total Actual Rows: {total_rows:,}")
                else:
                    print(f"   ⚠️  No partitions found")
                
                print()
            
        except Exception as e:
            print(f"❌ Error getting partition info: {e}")
    
    def view_table_data(self, table_name: str, limit: int = 10, partition_name: Optional[str] = None):
        """
        View data from a specific table or partition.
        
        Args:
            table_name: Name of the table (e.g., 'person', 'works_on')
            limit: Maximum number of rows to display
            partition_name: Specific partition name, or None for all partitions
        """
        try:
            full_table_name = f"{self.graph_name}.{table_name}"
            
            if partition_name:
                query_table = f"{self.graph_name}.{partition_name}"
                print(f"\n📋 Data from partition '{partition_name}' (limit {limit}):")
            else:
                query_table = full_table_name
                print(f"\n📋 Data from table '{table_name}' (all partitions, limit {limit}):")
            
            print("-" * 80)
            
            # Query the data
            self.cursor.execute(f"""
                SELECT 
                    id,
                    start_id,
                    end_id,
                    properties
                FROM {query_table}
                ORDER BY id
                LIMIT %s;
            """, (limit,))
            
            rows = self.cursor.fetchall()
            
            if not rows:
                print("   📭 No data found")
                return
            
            print(f"   📊 Found {len(rows)} rows:")
            print()
            
            for i, (node_id, start_id, end_id, properties) in enumerate(rows, 1):
                print(f"   🔸 Row {i}:")
                print(f"      ID: {node_id}")
                if start_id is not None:
                    print(f"      Start ID: {start_id}")
                if end_id is not None:
                    print(f"      End ID: {end_id}")
                
                # Pretty print properties
                if properties:
                    try:
                        props_dict = json.loads(str(properties)) if isinstance(properties, str) else properties
                        print(f"      Properties:")
                        for key, value in props_dict.items():
                            print(f"        • {key}: {value}")
                    except:
                        print(f"      Properties: {properties}")
                else:
                    print(f"      Properties: (empty)")
                print()
            
            # Show total count
            self.cursor.execute(f"SELECT COUNT(*) FROM {query_table};")
            total_count = self.cursor.fetchone()[0]
            print(f"   📊 Total rows in {'partition' if partition_name else 'table'}: {total_count:,}")
            
        except Exception as e:
            print(f"❌ Error viewing table data: {e}")
    
    def view_cypher_data(self, cypher_query: str, limit: int = 10):
        """
        View data using Cypher queries (AGE native approach).
        """
        try:
            print(f"\n🔍 Cypher Query Results (limit {limit}):")
            print("-" * 60)
            print(f"Query: {cypher_query}")
            print("-" * 60)
            
            # Execute Cypher query through AGE
            age_query = f"""
            SELECT * FROM cypher('{self.graph_name}', $$
                {cypher_query}
                LIMIT {limit}
            $$) as (result agtype);
            """
            
            self.cursor.execute(age_query)
            results = self.cursor.fetchall()
            
            if not results:
                print("   📭 No results found")
                return
            
            print(f"   📊 Found {len(results)} results:")
            print()
            
            for i, (result,) in enumerate(results, 1):
                print(f"   🔸 Result {i}: {result}")
            
        except Exception as e:
            print(f"❌ Error executing Cypher query: {e}")
    
    def view_partition_distribution(self, table_name: str):
        """
        Show how data is distributed across partitions for a specific table.
        """
        try:
            print(f"\n📊 PARTITION DISTRIBUTION for table '{table_name}'")
            print("="*60)
            
            # Get all partitions for this table
            self.cursor.execute("""
                SELECT 
                    c.relname as partition_name,
                    pg_get_expr(c.relpartbound, c.oid) as partition_bound
                FROM pg_inherits i
                JOIN pg_class c ON i.inhrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE i.inhparent = (
                    SELECT oid FROM pg_class 
                    WHERE relname = %s AND relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = %s
                    )
                )
                ORDER BY c.relname;
            """, (table_name, self.graph_name))
            
            partitions = self.cursor.fetchall()
            
            if not partitions:
                print(f"   ⚠️  Table '{table_name}' is not partitioned or doesn't exist")
                return
            
            total_rows = 0
            partition_stats = []
            
            for partition_name, partition_bound in partitions:
                # Get row count for each partition
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {self.graph_name}.{partition_name};")
                    row_count = self.cursor.fetchone()[0]
                    total_rows += row_count
                    
                    partition_stats.append({
                        'name': partition_name,
                        'bound': partition_bound,
                        'rows': row_count
                    })
                except Exception as e:
                    print(f"   ⚠️  Error getting count for {partition_name}: {e}")
                    partition_stats.append({
                        'name': partition_name,
                        'bound': partition_bound,
                        'rows': 'Error'
                    })
            
            # Display results
            for stats in partition_stats:
                if isinstance(stats['rows'], int):
                    percentage = (stats['rows'] / total_rows * 100) if total_rows > 0 else 0
                    print(f"   🔸 {stats['name']}")
                    print(f"      Range: {stats['bound']}")
                    print(f"      Rows: {stats['rows']:,} ({percentage:.1f}%)")
                else:
                    print(f"   🔸 {stats['name']}")
                    print(f"      Range: {stats['bound']}")
                    print(f"      Rows: {stats['rows']}")
                print()
            
            print(f"   📊 Total rows across all partitions: {total_rows:,}")
            
        except Exception as e:
            print(f"❌ Error analyzing partition distribution: {e}")
    
    def search_data(self, table_name: str, property_name: str, property_value: Any, limit: int = 10):
        """
        Search for specific data across all partitions.
        """
        try:
            print(f"\n🔍 SEARCH RESULTS in table '{table_name}'")
            print(f"Looking for: {property_name} = {property_value}")
            print("-" * 60)
            
            # Use JSONB query to search properties
            self.cursor.execute(f"""
                SELECT 
                    id,
                    start_id,
                    end_id,
                    properties
                FROM {self.graph_name}.{table_name}
                WHERE properties->>%s = %s
                ORDER BY id
                LIMIT %s;
            """, (property_name, str(property_value), limit))
            
            results = self.cursor.fetchall()
            
            if not results:
                print("   📭 No matching records found")
                return
            
            print(f"   📊 Found {len(results)} matching records:")
            print()
            
            for i, (node_id, start_id, end_id, properties) in enumerate(results, 1):
                print(f"   🔸 Match {i}:")
                print(f"      ID: {node_id}")
                if start_id is not None:
                    print(f"      Start ID: {start_id}")
                if end_id is not None:
                    print(f"      End ID: {end_id}")
                
                if properties:
                    try:
                        props_dict = json.loads(str(properties)) if isinstance(properties, str) else properties
                        print(f"      Properties:")
                        for key, value in props_dict.items():
                            marker = "🎯" if key == property_name else "•"
                            print(f"        {marker} {key}: {value}")
                    except:
                        print(f"      Properties: {properties}")
                print()
                
        except Exception as e:
            print(f"❌ Error searching data: {e}")


def demonstrate_partition_viewing():
    """
    Comprehensive demonstration of viewing partitioned AGE data.
    """
    # Database configuration
    DB_CONFIG = {
        'dbname': 'your_database',
        'user': 'your_user',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432
    }
    
    viewer = AGEPartitionViewer(DB_CONFIG, 'partitioned_graph')
    
    try:
        if not viewer.connect():
            return False
        
        print("🚀 COMPREHENSIVE PARTITION VIEWING DEMO")
        print("="*80)
        
        # 1. Show all partition information
        viewer.view_all_partitions_info()
        
        # 2. View data from specific tables
        print("\n" + "="*80)
        print("📋 TABLE DATA SAMPLES")
        print("="*80)
        
        # Common table names (adjust based on your labels)
        table_names = ['person', 'project', 'department', 'works_on', 'leads']
        
        for table in table_names:
            try:
                viewer.view_table_data(table, limit=5)
            except Exception as e:
                print(f"⚠️  Could not view table '{table}': {e}")
        
        # 3. Show partition distribution
        print("\n" + "="*80)
        print("📊 PARTITION DISTRIBUTION ANALYSIS")
        print("="*80)
        
        for table in table_names:
            try:
                viewer.view_partition_distribution(table)
            except Exception as e:
                print(f"⚠️  Could not analyze distribution for '{table}': {e}")
        
        # 4. Cypher queries
        print("\n" + "="*80)
        print("🔍 CYPHER QUERY SAMPLES")
        print("="*80)
        
        # Sample Cypher queries
        cypher_queries = [
            "MATCH (n) RETURN labels(n), count(*)",
            "MATCH (p:Project) RETURN p.name, p.status",
            "MATCH (n)-[r]->(m) RETURN type(r), count(*)",
            "MATCH (p:Person) RETURN p.name, p.email LIMIT 5"
        ]
        
        for query in cypher_queries:
            try:
                viewer.view_cypher_data(query, limit=10)
            except Exception as e:
                print(f"⚠️  Query failed: {e}")
        
        # 5. Search examples
        print("\n" + "="*80)
        print("🎯 SEARCH EXAMPLES")
        print("="*80)
        
        # Example searches (adjust based on your data)
        search_examples = [
            ('project', 'status', 'In Progress'),
            ('person', 'name', 'Jane Doe'),
            ('department', 'name', 'Research & Development')
        ]
        
        for table, prop, value in search_examples:
            try:
                viewer.search_data(table, prop, value, limit=5)
            except Exception as e:
                print(f"⚠️  Search failed for {table}.{prop}={value}: {e}")
        
        return True
        
    except Exception as e:
        print(f"💥 Demo failed: {e}")
        return False
        
    finally:
        viewer.disconnect()


# Quick commands for common operations
def quick_view_commands():
    """
    Show quick SQL commands for viewing partitioned data.
    """
    
    print("\n🚀 QUICK VIEW COMMANDS")
    print("="*50)
    print("Copy and paste these into your PostgreSQL client:")
    print()
    
    commands = [
        ("View all partitioned tables", """
SELECT 
    schemaname, tablename, 
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'partitioned_graph'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
        """),
        
        ("View partition information", """
SELECT 
    t.schemaname,
    t.tablename,
    p.relname as partition_name,
    pg_get_expr(p.relpartbound, p.oid) as partition_bound,
    pg_size_pretty(pg_relation_size(p.oid)) as partition_size
FROM pg_tables t
JOIN pg_inherits i ON i.inhparent = (t.schemaname||'.'||t.tablename)::regclass
JOIN pg_class p ON i.inhrelid = p.oid
WHERE t.schemaname = 'partitioned_graph'
ORDER BY t.tablename, p.relname;
        """),
        
        ("View data from all partitions", """
-- Replace 'person' with your table name
SELECT id, properties FROM partitioned_graph.person LIMIT 10;
        """),
        
        ("View data from specific partition", """
-- Replace 'person_y2024' with your partition name
SELECT id, properties FROM partitioned_graph.person_y2024 LIMIT 10;
        """),
        
        ("Search by property", """
-- Search for specific property values
SELECT id, properties 
FROM partitioned_graph.person 
WHERE properties->>'name' = 'Jane Doe';
        """),
        
        ("Count rows per partition", """
SELECT 
    schemaname, tablename,
    (SELECT count(*) FROM pg_class WHERE relname = tablename) as row_count
FROM pg_tables 
WHERE schemaname = 'partitioned_graph'
ORDER BY tablename;
        """)
    ]
    
    for title, command in commands:
        print(f"📋 {title}:")
        print(f"```sql{command}```")
        print()


if __name__ == "__main__":
    print("🔍 Apache AGE Partition Viewer")
    print("Choose an option:")
    print("1. Full demonstration")
    print("2. Quick SQL commands")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        success = demonstrate_partition_viewing()
        if success:
            print("\n✅ Partition viewing demo completed!")
        else:
            print("\n❌ Demo failed - check your database configuration")
    elif choice == "2":
        quick_view_commands()
    else:
        print("Invalid choice")
    
    print("\n📖 Remember to update DB_CONFIG with your actual database credentials!")
