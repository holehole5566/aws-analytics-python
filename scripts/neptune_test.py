from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __
from dotenv import load_dotenv
import os
import sys

load_dotenv()

# AWS Neptune endpoint from .env
NEPTUNE_ENDPOINT = os.getenv('NEPTUNE_ENDPOINT')
NEPTUNE_PORT = int(os.getenv('NEPTUNE_PORT', '8182'))

if not NEPTUNE_ENDPOINT:
    print("Error: NEPTUNE_ENDPOINT not found in .env file")
    sys.exit(1)

# Create connection
remote_conn = DriverRemoteConnection(
    f'wss://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/gremlin',
    'g'
)
g = traversal().withRemote(remote_conn)

try:
    # 1. Add vertices (寫入資料)
    print("Adding vertices...")
    
    # Add person vertices
    alice_id = g.addV('person').property('name', 'Alice').property('age', 30).id_().next()
    print(f"Added Alice with ID: {alice_id}")
    
    bob_id = g.addV('person').property('name', 'Bob').property('age', 25).id_().next()
    print(f"Added Bob with ID: {bob_id}")
    
    # Add company vertex
    company_id = g.addV('company').property('name', 'TechCorp').id_().next()
    print(f"Added TechCorp with ID: {company_id}")
    
    # 2. Add edges (建立關係)
    print("\nAdding edges...")
    
    # Alice works at TechCorp
    g.V(alice_id).addE('works_at').to(__.V(company_id)).property('since', 2020).iterate()
    print("Added edge: Alice -> works_at -> TechCorp")
    
    # Bob works at TechCorp
    g.V(bob_id).addE('works_at').to(__.V(company_id)).property('since', 2021).iterate()
    print("Added edge: Bob -> works_at -> TechCorp")
    
    # Alice knows Bob
    g.V(alice_id).addE('knows').to(__.V(bob_id)).iterate()
    print("Added edge: Alice -> knows -> Bob")
    
    print("\nEdges added successfully")
    
    # 3. Query data (查詢資料)
    print("\n=== Query Results ===")
    
    # Count vertices
    vertex_count = g.V().count().next()
    print(f"Total vertices: {vertex_count}")
    
    # Count edges
    edge_count = g.E().count().next()
    print(f"Total edges: {edge_count}")
    
    # Find all persons
    persons = g.V().hasLabel('person').valueMap(True).toList()
    print(f"\nAll persons:")
    for p in persons:
        print(f"  {p}")
    
    # Find who works at TechCorp
    employees = g.V().has('company', 'name', 'TechCorp').in_('works_at').values('name').toList()
    print(f"\nEmployees at TechCorp: {employees}")
    
    # Find Alice's connections
    alice_knows = g.V().has('person', 'name', 'Alice').out('knows').values('name').toList()
    print(f"\nAlice knows: {alice_knows}")
    
    # Show all edges (簡化版)
    print(f"\nAll edges:")
    all_edges = g.E().toList()
    for edge in all_edges:
        edge_label = g.E(edge.id).label().next()
        from_name = g.E(edge.id).outV().values('name').next()
        to_name = g.E(edge.id).inV().values('name').next()
        print(f"  {from_name} --[{edge_label}]--> {to_name}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    remote_conn.close()
