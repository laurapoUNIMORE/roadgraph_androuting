import overpy
import json
from neo4j import GraphDatabase
import argparse
import os
import time


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_path)
            return result

    @staticmethod
    def _get_path(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.neo4j_home' return value;
                    """)
        return result.values()

    def get_import_folder_name(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()

    def import_node(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._import_node)
            return result

    @staticmethod
    def _import_node(tx):
        result = tx.run("""
            CALL apoc.load.json("nodefile.json") YIELD value AS value 
            WITH value.elements AS elements
            UNWIND elements AS nodo
            MERGE (wn:OSMNode {osm_id: nodo.id})
              ON CREATE SET wn.lat=tofloat(nodo.lat), 
                            wn.lon=tofloat(nodo.lon), 
                            wn.geometry='POINT(' + nodo.lat + ' ' + nodo.lon +')'
            MERGE (wn)-[:PART_OF]->(n:PointOfInterest {osm_id: nodo.id})
              ON CREATE SET n.name=nodo.tags.name
            MERGE (n)-[:TAGS]->(t:Tag)
              ON CREATE SET t += nodo.tags
            MERGE (wn)-[:TAGS]->(t)
        """)
        return result.values()

    def import_node_way(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._import_node_way)
            return result

    @staticmethod
    def _import_node_way(tx):
        result = tx.run("""
            CALL apoc.load.json("nodeway.json") YIELD value AS value 
            WITH value.elements AS elements
            UNWIND elements AS nodo
            MERGE (wn:OSMNode {osm_id: nodo.id})
              ON CREATE SET wn.lat=tofloat(nodo.lat), 
                            wn.lon=tofloat(nodo.lon), 
                            wn.geometry='POINT(' + nodo.lat + ' ' + nodo.lon +')'
        """)
        return result.values()

    def import_way(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._import_way)
            return result

    @staticmethod
    def _import_way(tx):
        result = tx.run("""
            CALL apoc.load.json("wayfile.json") YIELD value 
            WITH value.elements AS elements
            UNWIND elements AS way
            MERGE (w:OSMWay:PointOfInterest {osm_id: way.id}) 
              ON CREATE SET w.name = way.tags.name
            MERGE (w)-[:TAGS]->(t:Tag) 
              ON CREATE SET t += way.tags
            WITH w, way.nodes AS nodes
            UNWIND nodes AS node
            MATCH (wn:OSMNode {osm_id: node})
            MERGE (wn)-[:PART_OF]->(w)
            MERGE (wn)-[:TAGS]->(t)
        """)
        return result.values()

    def import_nodes_into_spatial_layer(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._import_nodes_into_spatial_layer)
            return result

    @staticmethod
    def _import_nodes_into_spatial_layer(tx):
        tx.run("""
            MATCH (n:OSMNode)
            CALL spatial.addNode('spatial', n) yield node return node;
        """)
        return result.values()

    def set_location(self):
        """Insert the location in the POI, OSMNode, and RoadJunction nodes."""
        with self.driver.session() as session:
            result = session.write_transaction(self._set_location)
            return result

    @staticmethod
    def _set_location(tx):
        result = tx.run("""
                MATCH (n:OSMNode) 
                SET n.location = point({latitude: tofloat(n.lat), longitude: tofloat(n.lon)})
            """)
        return result.values()

    def set_index(self):
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self._set_index)
                return result
            except Exception as e:
                print(f"Index creation skipped or failed: {e}")

    @staticmethod
    def _set_index(tx):
        try:
            tx.run("CREATE INDEX IF NOT EXISTS FOR (n:OSMNode) ON (n.osm_id)")
            tx.run("CREATE INDEX IF NOT EXISTS FOR (n:PointOfInterest) ON (n.osm_id)")
        except Exception as e:
            print(f"Failed to create index: {e}")
        return []

    def mark_driveable_roadjunctions(self):
        with self.driver.session() as session:
            session.write_transaction(self._mark_driveable_roadjunctions)

    @staticmethod
    def _mark_driveable_roadjunctions(tx):
        # Step 1: Ensure 'driveable' property is stored as boolean
        tx.run("""
               MATCH ()-[r:ROUTE]-()
               WHERE r.driveable = 'True' OR r.driveable = 'true'
               SET r.driveable = 'True'
           """).consume()

        # Step 2: Default all RoadJunctions to false (using string 'False' for consistency)
        tx.run("""
               MATCH (n:RoadJunction)
               SET n.driveable = 'False'
           """).consume()

        # Step 3: Mark RoadJunctions connected to a driveable route as 'True'
        tx.run("""
               MATCH (n:RoadJunction)-[r:ROUTE]-()
               WHERE r.driveable = 'True'
               SET n.driveable = 'True'
           """).consume()

    def connect_amenity(self):
        """Connect the POI and OSMNode to the nearest RoadJunction."""
        with self.driver.session() as session:
            result = session.write_transaction(self._connect_amenity)

    @staticmethod
    def _connect_amenity(tx):

        # Step 1:Find and create relationships between POIs/OSMNode to nearest RoadJunctions within 120m."""
        result = tx.run("""
                MATCH (poi:PointOfInterest)<-[:PART_OF]-(osmn:OSMNode)
                WHERE exists(osmn.lat) AND exists(osmn.lon)
                WITH poi, osmn, point({latitude: toFloat(osmn.lat), longitude: toFloat(osmn.lon)}) AS poiLoc
                MATCH (rj:RoadJunction)
                WHERE rj.driveable = 'True' AND exists(rj.location) AND distance(rj.location, poiLoc) < 120
                WITH poi, osmn, rj, distance(rj.location, poiLoc) AS dist
                ORDER BY dist
                WITH poi, osmn, collect({rj: rj, dist: dist})[0] AS nearest
                WITH poi, osmn, nearest.rj AS nearestRJ, nearest.dist AS nearestDist
                MERGE (poi)-[r1:NEAR]->(nearestRJ)
                  ON CREATE SET r1.distance = nearestDist, r1.status = 'driveable_nearest'
                MERGE (osmn)-[r2:NEAR]->(nearestRJ)
                  ON CREATE SET r2.distance = nearestDist, r2.status = 'driveable_nearest'

        """)

        # Step 2: Connect POIs/OSMNode to nearest driveable RoadJunctions to nearest non-driveable RoadJunction
        result = tx.run("""
                MATCH (poi:PointOfInterest)<-[:PART_OF]-(osmn:OSMNode)
                WHERE NOT (osmn)-[:NEAR]->(:RoadJunction)
                 AND exists(osmn.lat) AND exists(osmn.lon)
                WITH poi, osmn, point({latitude: toFloat(osmn.lat), longitude: toFloat(osmn.lon)}) AS poiLoc
                MATCH (rj:RoadJunction)
                WHERE rj.driveable = 'False' AND exists(rj.location) AND distance(rj.location, poiLoc) < 120
                WITH poi, osmn, rj, distance(rj.location, poiLoc) AS dist
                ORDER BY dist
                WITH poi, osmn, collect({rj: rj, dist: dist})[0] AS nearest
                WITH poi, osmn, nearest.rj AS nearestRJ, nearest.dist AS nearestDist
                MERGE (poi)-[r1:NEAR]->(nearestRJ)
                  ON CREATE SET r1.distance = nearestDist, r1.status = 'non_driveable_nearest'
                MERGE (osmn)-[r2:NEAR]->(nearestRJ)
                  ON CREATE SET r2.distance = nearestDist, r2.status = 'non_driveable_nearest'

        """)

        # Step 3: Ensure at least one OSMNode connected to a non-driveable RoadJunction is also connected to a driveable RoadJunction
        result = tx.run("""
                MATCH (osmn:OSMNode)
                WHERE NOT (osmn)-[:NEAR]->(:RoadJunction)
                  AND exists(osmn.lat) AND exists(osmn.lon)
                WITH osmn, point({latitude: toFloat(osmn.lat), longitude: toFloat(osmn.lon)}) AS osmLoc
                CALL {
                  WITH osmn, osmLoc
                  MATCH (rj:RoadJunction)
                  WHERE rj.driveable = 'True' AND exists(rj.location)
                  RETURN rj, distance(osmLoc, rj.location) AS dist
                  ORDER BY dist ASC
                  LIMIT 1
                }
                MERGE (osmn)-[r:NEAR]->(rj)
                SET r.distance = dist, r.status = 'nearest_driveable'
        """)
        # Step 4: Remove the relationship between OSMWay and RoadJunction if exists
        result = tx.run("""
                MATCH (p:OSMWay)-[r:NEAR]->(n:RoadJunction)
                DELETE r
            """)
        return result.values()


def add_options():
    parser = argparse.ArgumentParser(description='Insertion of POI in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str, required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str, required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str, required=True)
    parser.add_argument('--latitude', '-x', dest='lat', type=float, required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float, required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float, required=True)
    parser.add_argument('--spatial', '-s', dest='spatial', type=str, required=False, default='False')
    return parser

def main(args=None):
    start_time = time.time()
    argParser = add_options()
    options = argParser.parse_args(args=args)
    api = overpy.Overpass()
    dist = options.dist
    lon = options.lon
    lat = options.lat
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + "\\"
    result = api.query(f"""(   
                           way(around:{dist},{lat},{lon})["amenity"];
                       );(._;>;);
                       out body;
                """)
    # generate a json file with the retrieved information about the nodes that compose each way
    list_node_way = []
    for w in result.ways:
        print(w)
        for n in w.get_nodes(resolve_missing=False):
            d = {'type': 'node', 'id': n.id,
                 'id_way': w.id,
                 'lat': str(n.lat),
                 'lon': str(n.lon),
                 'geometry': 'POINT(' + str(n.lat) + ' ' + str(n.lon) + ')',
                 'tags': n.tags}
            print(d)
            list_node_way.append(d)
    res = {"elements": list_node_way}
    print("nodes to import:")
    print(res)
    print("-----------------------------------------------------------------------")
    with open(path + 'nodeway.json', "w") as f:
        json.dump(res, f)
        print("file generated in import directory")
    # import the nodes in the graph as OSMNodes
    greeter.import_node_way()
    # generatio of the way file in the import directory
    list_way = []
    for way in result.ways:
        d = {'type': 'way', 'id': way.id, 'tags': way.tags}
        l_node = []
        for node in way.nodes:
            l_node.append(node.id)
        d['nodes'] = l_node
        list_way.append(d)
    res = {"elements": list_way}
    print("ways to import:")
    print(res)
    print("-----------------------------------------------------------------------")
    with open(path + "wayfile.json", "w") as f:
        json.dump(res, f)
        print("file generated in import directory")
    # import the ways in the graph as POI nodes
    greeter.import_way()
    print("import wayfile.json: done")
    # query overpass API for POI represented as nodes
    result = api.query(f"""(   
                                   node(around:{dist},{lat},{lon})["amenity"];
                               );
                               out body;
                               """)
    # generation of the node file in the import directory
    list_node = []
    for node in result.nodes:
        d = {'type': 'node', 'id': node.id,
             'lat': str(node.lat),
             'lon': str(node.lon),
             'geometry': 'POINT(' + str(node.lat) + ' ' + str(node.lon) + ')',
             'tags': node.tags}
        list_node.append(d)
    res = {"elements": list_node}
    print("nodes to import:")
    print(res)
    print("-----------------------------------------------------------------------")
    with open(path + 'nodefile.json', "w") as f:
        json.dump(res, f)
    print("file generated in import directory")
    greeter.import_way()
    greeter.import_node()
    if (options.spatial == 'True'):
        greeter.import_nodes_into_spatial_layer()
    greeter.set_location()
    greeter.mark_driveable_roadjunctions()
    greeter.connect_amenity()
    greeter.close()
    print(f"Total execution time: {time.time() - start_time:.2f} seconds")
    return 0

main()
