import psycopg
import json
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class VertexData:
    """Data structure for vertex information"""
    label: str
    properties: Dict[str, Any]
    external_id: Optional[str] = None  # For tracking original IDs

@dataclass
class EdgeData:
    """Data structure for edge information"""
    label: str
    start_vertex_id: Union[int, str]  # Can be AGE ID or external ID
    end_vertex_id: Union[int, str]    # Can be AGE ID or external ID
    properties: Dict[str, Any]
    start_vertex_label: Optional[str] = None  # For ID resolution
    end_vertex_label: Optional[str] = None    # For ID resolution

class AGEGraphIngestion:
    """
    Apache AGE Graph Ingestion Module
    Handles bulk loading of vertices and edges with properties
    """
    
    def __init__(self, db_config: Dict[str, str], graph_name: str):
        self.db_config = db_config
        self.graph_name = graph_name
        self.conn = None
        self.cur = None
        self.vertex_id_mapping = {}  # Maps external IDs to AGE IDs
        
    def connect(self):
        """Establish database connection and setup AGE"""
        try:
            self.conn = psycopg.connect(**self.db_config)
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
            
            # Load AGE extension and set search path
            self.cur.execute("LOAD 'age';")
            self.cur.execute("SET search_path TO ag_catalog, '$user', public;")
            logger.info("Connected to database and loaded AGE extension")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def _convert_properties_to_agtype(self, properties: Dict[str, Any]) -> str:
        """Convert Python dict to AGE AGTYPE format"""
        # Convert Python types to AGE-compatible JSON
        converted = {}
        for key, value in properties.items():
            if isinstance(value, datetime):
                converted[key] = value.isoformat()
            elif isinstance(value, (list, tuple)):
                converted[key] = list(value)
            elif isinstance(value, dict):
                converted[key] = value
            else:
                converted[key] = value
        
        return json.dumps(converted)

    def ingest_vertex(self, vertex_data: VertexData) -> int:
        """
        Ingest a single vertex into the graph
        Returns the AGE vertex ID
        """
        try:
            properties_json = self._convert_properties_to_agtype(vertex_data.properties)
            
            # Use AGE's Cypher query to create vertex
            cypher_query = f"""
            SELECT * FROM cypher('{self.graph_name}', $$
                CREATE (v:{vertex_data.label} {properties_json})
                RETURN id(v)
            $$) AS (vertex_id agtype);
            """
            
            self.cur.execute(cypher_query)
            result = self.cur.fetchone()
            vertex_id = int(result[0])
            
            # Store mapping if external ID provided
            if vertex_data.external_id:
                self.vertex_id_mapping[vertex_data.external_id] = vertex_id
            
            logger.debug(f"Created vertex {vertex_data.label} with ID: {vertex_id}")
            return vertex_id
            
        except Exception as e:
            logger.error(f"Failed to ingest vertex {vertex_data.label}: {e}")
            raise

    def ingest_vertices_batch(self, vertices: List[VertexData], batch_size: int = 1000) -> List[int]:
        """
        Ingest vertices in batches for better performance
        Returns list of AGE vertex IDs
        """
        vertex_ids = []
        
        for i in range(0, len(vertices), batch_size):
            batch = vertices[i:i + batch_size]
            batch_ids = []
            
            logger.info(f"Processing vertex batch {i//batch_size + 1}/{(len(vertices) + batch_size - 1)//batch_size}")
            
            for vertex_data in batch:
                try:
                    vertex_id = self.ingest_vertex(vertex_data)
                    batch_ids.append(vertex_id)
                except Exception as e:
                    logger.warning(f"Skipped vertex due to error: {e}")
                    batch_ids.append(None)
            
            vertex_ids.extend(batch_ids)
        
        logger.info(f"Completed vertex ingestion: {len([v for v in vertex_ids if v])} successful, {len([v for v in vertex_ids if not v])} failed")
        return vertex_ids

    def ingest_edge(self, edge_data: EdgeData) -> int:
        """
        Ingest a single edge into the graph
        Returns the AGE edge ID
        """
        try:
            # Resolve vertex IDs if they're external IDs
            start_id = self._resolve_vertex_id(edge_data.start_vertex_id, edge_data.start_vertex_label)
            end_id = self._resolve_vertex_id(edge_data.end_vertex_id, edge_data.end_vertex_label)
            
            properties_json = self._convert_properties_to_agtype(edge_data.properties)
            
            # Use AGE's Cypher query to create edge
            cypher_query = f"""
            SELECT * FROM cypher('{self.graph_name}', $$
                MATCH (a), (b)
                WHERE id(a) = {start_id} AND id(b) = {end_id}
                CREATE (a)-[r:{edge_data.label} {properties_json}]->(b)
                RETURN id(r)
            $$) AS (edge_id agtype);
            """
            
            self.cur.execute(cypher_query)
            result = self.cur.fetchone()
            
            if result:
                edge_id = int(result[0])
                logger.debug(f"Created edge {edge_data.label} with ID: {edge_id}")
                return edge_id
            else:
                raise Exception("Edge creation returned no result")
                
        except Exception as e:
            logger.error(f"Failed to ingest edge {edge_data.label}: {e}")
            raise

    def ingest_edges_batch(self, edges: List[EdgeData], batch_size: int = 1000) -> List[int]:
        """
        Ingest edges in batches for better performance
        Returns list of AGE edge IDs
        """
        edge_ids = []
        
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]
            batch_ids = []
            
            logger.info(f"Processing edge batch {i//batch_size + 1}/{(len(edges) + batch_size - 1)//batch_size}")
            
            for edge_data in batch:
                try:
                    edge_id = self.ingest_edge(edge_data)
                    batch_ids.append(edge_id)
                except Exception as e:
                    logger.warning(f"Skipped edge due to error: {e}")
                    batch_ids.append(None)
            
            edge_ids.extend(batch_ids)
        
        logger.info(f"Completed edge ingestion: {len([e for e in edge_ids if e])} successful, {len([e for e in edge_ids if not e])} failed")
        return edge_ids

    def _resolve_vertex_id(self, vertex_id: Union[int, str], vertex_label: Optional[str] = None) -> int:
        """Resolve external vertex ID to AGE internal ID"""
        if isinstance(vertex_id, int):
            return vertex_id
        
        # Check mapping first
        if vertex_id in self.vertex_id_mapping:
            return self.vertex_id_mapping[vertex_id]
        
        # Try to find by external ID property
        if vertex_label:
            try:
                cypher_query = f"""
                SELECT * FROM cypher('{self.graph_name}', $$
                    MATCH (v:{vertex_label})
                    WHERE v.external_id = '{vertex_id}'
                    RETURN id(v)
                $$) AS (vertex_id agtype);
                """
                self.cur.execute(cypher_query)
                result = self.cur.fetchone()
                if result:
                    age_id = int(result[0])
                    self.vertex_id_mapping[vertex_id] = age_id
                    return age_id
            except Exception as e:
                logger.warning(f"Failed to resolve vertex ID {vertex_id}: {e}")
        
        raise ValueError(f"Cannot resolve vertex ID: {vertex_id}")

    def bulk_ingest_from_dict(self, data: Dict[str, Any]):
        """
        Bulk ingest from structured dictionary
        Expected format:
        {
            "vertices": [
                {
                    "label": "Person",
                    "properties": {"name": "John", "age": 30},
                    "external_id": "person_1"
                }
            ],
            "edges": [
                {
                    "label": "WORKS_AT",
                    "start_vertex_id": "person_1",
                    "end_vertex_id": "company_1",
                    "start_vertex_label": "Person",
                    "end_vertex_label": "Company",
                    "properties": {"since": "2020-01-01"}
                }
            ]
        }
        """
        logger.info("Starting bulk ingestion from dictionary")
        
        # Ingest vertices first
        if "vertices" in data:
            vertices = [
                VertexData(
                    label=v["label"],
                    properties=v["properties"],
                    external_id=v.get("external_id")
                ) for v in data["vertices"]
            ]
            self.ingest_vertices_batch(vertices)
        
        # Then ingest edges
        if "edges" in data:
            edges = [
                EdgeData(
                    label=e["label"],
                    start_vertex_id=e["start_vertex_id"],
                    end_vertex_id=e["end_vertex_id"],
                    properties=e["properties"],
                    start_vertex_label=e.get("start_vertex_label"),
                    end_vertex_label=e.get("end_vertex_label")
                ) for e in data["edges"]
            ]
            self.ingest_edges_batch(edges)
        
        logger.info("Bulk ingestion completed")

    def ingest_from_csv_files(self, vertex_files: Dict[str, str], edge_files: Dict[str, str]):
        """
        Ingest from CSV files
        vertex_files: {label: filepath}
        edge_files: {label: filepath}
        """
        import pandas as pd
        
        logger.info("Starting CSV ingestion")
        
        # Process vertex files
        for label, filepath in vertex_files.items():
            logger.info(f"Processing vertex file for {label}: {filepath}")
            df = pd.read_csv(filepath)
            
            vertices = []
            for _, row in df.iterrows():
                # Assume 'id' column exists for external ID mapping
                external_id = str(row.get('id', None))
                properties = row.drop('id').to_dict() if 'id' in row else row.to_dict()
                
                vertices.append(VertexData(
                    label=label,
                    properties=properties,
                    external_id=external_id
                ))
            
            self.ingest_vertices_batch(vertices)
        
        # Process edge files
        for label, filepath in edge_files.items():
            logger.info(f"Processing edge file for {label}: {filepath}")
            df = pd.read_csv(filepath)
            
            edges = []
            for _, row in df.iterrows():
                # Assume columns: start_id, end_id, start_label, end_label, other properties
                edge_props = row.drop(['start_id', 'end_id', 'start_label', 'end_label'], errors='ignore').to_dict()
                
                edges.append(EdgeData(
                    label=label,
                    start_vertex_id=str(row['start_id']),
                    end_vertex_id=str(row['end_id']),
                    start_vertex_label=row.get('start_label'),
                    end_vertex_label=row.get('end_label'),
                    properties=edge_props
                ))
            
            self.ingest_edges_batch(edges)
        
        logger.info("CSV ingestion completed")

    def get_ingestion_stats(self) -> Dict[str, int]:
        """Get statistics about the ingested data"""
        stats = {}
        
        try:
            # Count vertices by label
            cypher_query = f"""
            SELECT * FROM cypher('{self.graph_name}', $$
                MATCH (v)
                RETURN labels(v)[0] as label, count(v) as count
                ORDER BY label
            $$) AS (label agtype, count agtype);
            """
            self.cur.execute(cypher_query)
            results = self.cur.fetchall()
            
            for label, count in results:
                stats[f"vertices_{label}"] = int(count)
            
            # Count edges by label
            cypher_query = f"""
            SELECT * FROM cypher('{self.graph_name}', $$
                MATCH ()-[r]->()
                RETURN type(r) as label, count(r) as count
                ORDER BY label
            $$) AS (label agtype, count agtype);
            """
            self.cur.execute(cypher_query)
            results = self.cur.fetchall()
            
            for label, count in results:
                stats[f"edges_{label}"] = int(count)
                
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
        
        return stats


# Example usage and helper functions
def create_sample_data() -> Dict[str, Any]:
    """Generate sample data for testing"""
    return {
        "vertices": [
            {
                "label": "Person",
                "external_id": "person_1",
                "properties": {
                    "name": "Alice Johnson",
                    "age": 30,
                    "email": "alice@example.com",
                    "created_at": datetime.now().isoformat()
                }
            },
            {
                "label": "Person", 
                "external_id": "person_2",
                "properties": {
                    "name": "Bob Smith",
                    "age": 25,
                    "email": "bob@example.com"
                }
            },
            {
                "label": "Company",
                "external_id": "company_1", 
                "properties": {
                    "name": "Tech Corp",
                    "industry": "Technology",
                    "founded": 2010
                }
            },
            {
                "label": "Product",
                "external_id": "product_1",
                "properties": {
                    "name": "Super Widget",
                    "price": 99.99,
                    "category": "Electronics"
                }
            }
        ],
        "edges": [
            {
                "label": "WORKS_AT",
                "start_vertex_id": "person_1",
                "end_vertex_id": "company_1",
                "start_vertex_label": "Person",
                "end_vertex_label": "Company",
                "properties": {
                    "position": "Software Engineer",
                    "start_date": "2020-01-15",
                    "salary": 75000
                }
            },
            {
                "label": "WORKS_AT",
                "start_vertex_id": "person_2", 
                "end_vertex_id": "company_1",
                "start_vertex_label": "Person",
                "end_vertex_label": "Company",
                "properties": {
                    "position": "Data Scientist",
                    "start_date": "2021-03-01",
                    "salary": 80000
                }
            },
            {
                "label": "MANUFACTURED_BY",
                "start_vertex_id": "product_1",
                "end_vertex_id": "company_1", 
                "start_vertex_label": "Product",
                "end_vertex_label": "Company",
                "properties": {
                    "launch_date": "2022-05-01",
                    "version": "1.0"
                }
            }
        ]
    }


# Main execution example
if __name__ == "__main__":
    # Database configuration
    DB_CONFIG = {
        "dbname": "your_db_name",
        "user": "your_username", 
        "password": "your_password",
        "host": "localhost",
        "port": "5432"
    }
    
    GRAPH_NAME = "kg_graph"  # Match your graph name
    
    # Example usage
    with AGEGraphIngestion(DB_CONFIG, GRAPH_NAME) as ingestion:
        # Generate and ingest sample data
        sample_data = create_sample_data()
        ingestion.bulk_ingest_from_dict(sample_data)
        
        # Get statistics
        stats = ingestion.get_ingestion_stats()
        print("Ingestion Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print("Ingestion completed successfully!")
