from Constant import *
from InitMySQL import *
from watchPerformance import *
from Draw import *
import pyorient
from arango import ArangoClient

def initOrientDB():
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect("root", "welcome")
    client.db_open("graph", "admin", "admin")
    # client.db_create("graph", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
    # client.command("create class persons extends V")

def deleteOrientDB():
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect("root", "welcome")
    client.db_open("graph", "admin", "admin")
    client.command("DELETE VERTEX persons where true")
    client.command("DELETE EDGE  where true")

def initAnangoDB():
    # Initialize the client for ArangoDB
    client = ArangoClient(
        protocol='http',
        host='localhost',
        port=8529,
        username='root',
        password='',
        enable_logging=True
    )
    graph = client.db('graph').create_graph('graphPersons')
    persons = graph.create_vertex_collection('persons')
    know = graph.create_edge_definition(
        name='know',
        from_collections=['persons'],
        to_collections=['persons']
    )

def deleteArangoDB():
    client = ArangoClient(
        protocol='http',
        host='localhost',
        port=8529,
        username='root',
        password='',
        enable_logging=True
    )
    client.db('graph').delete_collection("persons")
    client.db('graph').delete_collection("know")
    client.db('graph').delete_graph('graphPersons')

def deleteNeo():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "welcome"))
    session = driver.session()
    session.run("match (n:person) detach delete n")
    session.close()

def delete_all():
    deleteNeo()
    deleteOrientDB()
    deleteArangoDB()

def init_all():
    initAnangoDB()
    initOrientDB()

def main():
    delete_all()
    init_all()

if __name__ == '__main__':
    main()