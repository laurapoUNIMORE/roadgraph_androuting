from neo4j import GraphDatabase
import json
import argparse
import os
import geopandas as gpd
import pandas as pd
import requests
from Tools import *


class App:
    """In this file we are going to extract from OSM crossings mapped as ways"""

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        """gets the path of the neo4j instance"""

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
        """gets the path of the import folder of the neo4j instance"""

        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()

def elem_to_feature(elem, geomType):
    """Convert the element in a json format"""

    if geomType == "LineString":
        prop = {}
        for key in elem['tags'].keys():
            prop[key]=elem['tags'][key]
        prop['nodes']=elem['nodes']
        return {
            "geometry": {
                    "type": geomType,
                    "coordinates": [[d["lon"], d["lat"]] for d in elem["geometry"]]
            },
            "properties": prop
        }

    return {
        "geometry": {
            "type": geomType,
            "coordinates": [elem["lon"], elem["lat"]]
        },
        "properties": prop
    } 

def add_options():
    """parameters to be used in order to run the script"""

    parser = argparse.ArgumentParser(description='Insertion of CROSSING NODES in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be covered""",
                        required=True)
    return parser



def createQueryCrossingWays(dist, lat, lon):
    """Create the query to fetch the data of interest"""

    query = f"""[out:json];
                                (
                                way(around:{dist},{lat},{lon})["crossing"]->.all;
                                way(around:{dist},{lat},{lon})[highway="crossing"]->.all;
                                way(around:{dist},{lat},{lon})[footway="crossing"]->.all;
                                way(around:{dist},{lat},{lon})[cycleway="crossing"]->.all;
                                way(around:{dist},{lat},{lon})[crossing="traffic_signals"]->.all;
                                way(around:{dist},{lat},{lon})[crossing="uncontrolled"]->.all;
                                way(around:{dist},{lat},{lon})[crossing="marked"]->.all;
                                way(around:{dist},{lat},{lon})[crossing="unmarked"]->.all;
                                way(around:{dist},{lat},{lon})[crossing="zebra"]->.all;                     
                            );
                            out geom;
                           """
    return query

    

def main(args=None):

    """Parsing of input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    dist = options.dist
    lon = options.lon
    lat = options.lat
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    url = 'http://overpass-api.de/api/interpreter'

    """overpass query to get crossings mapped as nodes fro OSM"""
    query = createQueryCrossingWays(dist, lat, lon)

    """Crossing ways extraction and generation of the GeoDataframe"""
    result = requests.get(url, params={'data': query})
    data = result.json()['elements']
    features = [elem_to_feature(elem, "LineString") for elem in data]
    gdf = gpd.GeoDataFrame.from_features(features, crs=4326)
    list_ids = ["way/"+str(elem["id"]) for elem in data]
    gdf.insert(0, 'id', list_ids)
    

    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'


    """Save the GeoDataframe in a json file"""
    save_gdf(gdf, path, "crossing_ways.json")
    print("Storing crossing ways: done")

if __name__ == "__main__":
    main()
