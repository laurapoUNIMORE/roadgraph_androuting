from neo4j import GraphDatabase
import overpy
import json
import argparse
import os
import time

"""In this file we are going to show how to connect CrossNode and Footway nodes"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def connect_footways_to_crossing_nodes(self,file):
        """Generate relationships between CrossNode and Footway nodes"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_footways_to_crossing_nodes,file)
            return result


    @staticmethod
    def _connect_footways_to_crossing_nodes(tx,file):
        tx.run("""
            call apoc.load.json($file) yield value as value with value.data as data unwind data as record
            match(cr:CrossNode {osm_id: record.id}) where NOT isEmpty (record.closest_footways) 
            UNWIND record.closest_footways as foot with cr, foot match (f:Footway) where f.osm_id = foot
            merge (f)-[r:CROSS_THE_ROAD]->(cr)
            merge (cr)-[r2:CROSS_THE_ROAD]->(f);
        """, file = file)
        result = tx.run("""
            match(f:Footway)-[r:CONTINUE_ON_FOOTWAY_BY_CROSSING_ROAD]-(f1:Footway) with f, f1, r
            match(f)-[:CROSS_THE_ROAD]->(cr:Crossing)<-[:CROSS_THE_ROAD]-(f1) 
            delete r; 

        """)
        return result




def add_options():
    """Parameters needed to run the script"""
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .json file of crossnodes.""",
                        required=True)
    return parser



def main(args=None):
    """Parsing parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Generate relationships between CrossNode and Footway nodes"""
    start_time = time.time()
    greeter.connect_footways_to_crossing_nodes(options.file_name)
    print("Connect elements close to the crossing nodes: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    

    return 0


if __name__ == "__main__":
    main()