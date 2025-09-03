from boto3 import Session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from ..config import Settings
from ..utils import get_logger

class NeptuneService:
    """Neptune Graph Database service"""
    
    def __init__(self, endpoint=None):
        self.logger = get_logger(__name__)
        self.endpoint = endpoint or Settings.NEPTUNE_ENDPOINT
        self.conn_string = f'wss://{self.endpoint}:8182/gremlin'
        self.service = 'neptune-db'
        self._connection = None
        self._g = None
        
    def _get_authenticated_connection(self):
        """Get authenticated connection to Neptune"""
        if not self.endpoint:
            raise ValueError("Neptune endpoint not configured")
            
        credentials = Session().get_credentials()
        if not credentials:
            raise ValueError("AWS credentials not found")
            
        creds = credentials.get_frozen_credentials()
        region = Session().region_name or Settings.AWS_REGION
        
        request = AWSRequest(method='GET', url=self.conn_string, data=None)
        SigV4Auth(creds, self.service, region).add_auth(request)
        
        return DriverRemoteConnection(
            self.conn_string, 
            'g', 
            headers=request.headers.items()
        )
    
    def connect(self):
        """Establish connection to Neptune"""
        try:
            self._connection = self._get_authenticated_connection()
            self._g = traversal().with_remote(self._connection)
            self.logger.info("Connected to Neptune")
            return self._g
        except Exception as e:
            self.logger.error(f"Failed to connect to Neptune: {e}")
            raise
    
    def disconnect(self):
        """Close Neptune connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
            self._g = None
            self.logger.info("Disconnected from Neptune")
    
    def get_traversal(self):
        """Get graph traversal object"""
        if not self._g:
            self.connect()
        return self._g
    
    def vertex_count(self):
        """Get total vertex count"""
        g = self.get_traversal()
        try:
            count = g.V().count().next()
            self.logger.info(f"Vertex count: {count}")
            return count
        except Exception as e:
            self.logger.error(f"Failed to get vertex count: {e}")
            raise
    
    def edge_count(self):
        """Get total edge count"""
        g = self.get_traversal()
        try:
            count = g.E().count().next()
            self.logger.info(f"Edge count: {count}")
            return count
        except Exception as e:
            self.logger.error(f"Failed to get edge count: {e}")
            raise
    
    def add_vertex(self, label, properties=None):
        """Add vertex with label and properties"""
        g = self.get_traversal()
        try:
            vertex_traversal = g.addV(label)
            
            if properties:
                for key, value in properties.items():
                    vertex_traversal = vertex_traversal.property(key, value)
            
            vertex = vertex_traversal.next()
            self.logger.info(f"Added vertex: {label}")
            return vertex
        except Exception as e:
            self.logger.error(f"Failed to add vertex: {e}")
            raise
    
    def add_edge(self, from_vertex_id, to_vertex_id, label, properties=None):
        """Add edge between vertices"""
        g = self.get_traversal()
        try:
            edge_traversal = g.V(from_vertex_id).addE(label).to(g.V(to_vertex_id))
            
            if properties:
                for key, value in properties.items():
                    edge_traversal = edge_traversal.property(key, value)
            
            edge = edge_traversal.next()
            self.logger.info(f"Added edge: {label}")
            return edge
        except Exception as e:
            self.logger.error(f"Failed to add edge: {e}")
            raise
    
    def find_vertices_by_label(self, label, limit=10):
        """Find vertices by label"""
        g = self.get_traversal()
        try:
            vertices = g.V().hasLabel(label).limit(limit).toList()
            self.logger.info(f"Found {len(vertices)} vertices with label: {label}")
            return vertices
        except Exception as e:
            self.logger.error(f"Failed to find vertices: {e}")
            raise
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()