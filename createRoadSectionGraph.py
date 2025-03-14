import osmnx as ox
import argparse
from neo4j import GraphDatabase
import os


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def creation_graph(self):
        # creation of the dual graph
        with self.driver.session() as session:
            result = session.write_transaction(self._creation_graph)
            return result

    @staticmethod
    def _creation_graph(tx):
        # Creation of nodes, one for each street
        result = tx.run("""
            MATCH (m:RoadJunction)-[r:ROUTE {status: 'active'}]->(n:RoadJunction) 
            WITH DISTINCT r.osmid AS street_names
            UNWIND street_names AS street_name
            CREATE (road:RoadOsm {osmid: street_name})
            WITH street_name
            MATCH (m:RoadJunction)-[r1:ROUTE {osmid: street_name, status: 'active'}]->(n:RoadJunction)
            WITH AVG(r1.AADT) AS AADT, SUM(r1.distance) AS dist, street_name, r1.name AS road_name
            MATCH (d:RoadOsm {osmid: street_name}) 
            SET d.traffic = AADT / dist, 
                d.status = 'active',
                d.AADT = AADT,
                d.distance = dist,
                d.name = road_name
            RETURN d
        """)

        values = result.values()
        if values:
            print(values)

        # Creation of relationships between road sections
        result = tx.run("""
            MATCH (m:RoadJunction)-[r:ROUTE]->(n:RoadJunction) 
            WITH DISTINCT r.osmid AS street_names
            UNWIND street_names AS street_name
            MATCH (m:RoadJunction)-[r1:ROUTE {osmid: street_name}]->(n:RoadJunction)
            WITH m, street_name
            MATCH (x:RoadJunction)-[r2:ROUTE]->(m:RoadJunction)
            WHERE r2.osmid <> street_name
            WITH r2.osmid AS source, street_name, m
            MATCH (r1:RoadOsm {osmid: source}), (r2:RoadOsm {osmid: street_name})
            CREATE (r1)-[r:CONNECTED {junction: m.id, location: m.location}]->(r2)
            RETURN r
        """)

        values = result.values()
        if values:
            print(values)

        return values

    def set_index(self):
        """create index on nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_index)
            return result

    @staticmethod
    def _set_index(tx):
        result = tx.run("""
                           create index on :RoadOsm(osmid)
                       """)
        return result.values()


def add_options():
    parser = argparse.ArgumentParser(description='Creation of routing graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    return parser


def main(args=None):
    argParser = add_options()
    # retrieve arguments
    options = argParser.parse_args(args=args)
    # connecting to the neo4j instance
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    # creation of the dual graph
    greeter.creation_graph()
    # set index on road nodes
    greeter.set_index()
    greeter.close()
    return 0


main()
