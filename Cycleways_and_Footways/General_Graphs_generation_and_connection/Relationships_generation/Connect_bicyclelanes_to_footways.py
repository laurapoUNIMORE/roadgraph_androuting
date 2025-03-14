from neo4j import GraphDatabase
import json
import argparse
import os
import time

"""In this file we are going to show how to generate relationships between BicycleLane and Footway nodes"""

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def connect_footways_to_touched_bicycle_lanes(self, file):
        """Generate relationships between BicycleLane and Footway nodes representing cycling and foot paths that
           touch or intersect
        """
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_footways_to_touched_bicycle_lanes, file)
            return result

    @staticmethod
    def _connect_footways_to_touched_bicycle_lanes(tx, file):
        result = tx.run("""
            call apoc.load.json($file) yield value as value with value.data as data 
            UNWIND data as record match(f:Footway) 
            where NOT isEmpty(record.touched_lanes) and f.osm_id = record.id  
            UNWIND record.touched_lanes as lane
            match(b:BicycleLane) 
            where b.osm_id = lane and f.osm_id <> b.osm_id 
            merge (f)-[r:CONTINUE_ON_LANE]->(b) merge (b)-[r1:CONTINUE_ON_FOOTWAY]->(f);
        """, file = file)
        return result

    def connect_footways_to_close_lanes(self, file):
        """Generate relationships between BicycleLane and Footway nodes representing cycling and foot paths that
           are reachable by crossing the road where the crossing is not signaled"""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_footways_to_close_lanes, file)
            return result

    @staticmethod
    def _connect_footways_to_close_lanes(tx, file):
        result = tx.run("""
                call apoc.load.json($file) yield value as value with value.data as data 
                UNWIND data as record match (f:Footway) where f.osm_id = record.id and NOT isEmpty(record.closest_lanes)
                UNWIND record.closest_lanes as lane with f, lane 
                match (b:BicycleLane) 
                where b.osm_id = lane[0] and b.osm_id <> f.osm_id
                and not exists((b)-[:CONTINUE_ON_FOOTWAY]->(f))
                merge (b)-[r:CONTINUE_ON_CLOSE_FOOTWAY_BY_CROSSING_ROAD]->(f) on create set r.length = lane[1]
                merge(f)-[r1:CONTINUE_ON_CLOSE_LANE_BY_CROSSING_ROAD]->(b) ON CREATE SET r1.length = r.length; 
        """, file = file)
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
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing footways.""",
                        required=True)
    return parser

def main(args=None):
    """Parsing parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)

    """Generate relationships between BicycleLane and Footway nodes representing cycling and foot paths that
       touch or intersect
    """
    start_time = time.time()
    greeter.connect_footways_to_touched_bicycle_lanes(options.file_name_footways)
    print("Connect footways to cycleways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))

    """Generate relationships between BicycleLane and Footway nodes representing cycling and foot paths that are
       reachable by crossing the road where the crossing is not signaled
    """
    start_time = time.time()
    greeter.connect_footways_to_close_lanes(options.file_name_footways)
    print("Connect footways to close cycleways: done")
    print("Execution time : %s seconds" % (time.time() - start_time))
    

    

    return 0


if __name__ == "__main__":
    main()