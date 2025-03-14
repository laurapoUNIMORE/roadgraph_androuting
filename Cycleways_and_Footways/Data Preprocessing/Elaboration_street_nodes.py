from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt
import osmnx as ox


"""In this file we are going to make some preprocessing on street nodes in order to find 
relationships between them and cycleways, footways and crossings 
"""

class App:
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

def add_options():
    """parameters to be used in order to run the script"""

    parser = argparse.ArgumentParser(description='Creation of routing graph.')
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be cover""",
                        required=True)
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFilecycleways', '-fc', dest='file_name_cycleways', type=str,
                        help="""Insert the name of the .json file containing cycleways.""",
                        required=True)
    parser.add_argument('--nameFileCrossingWays', '-fcw', dest='file_name_crossing_ways', type=str,
                        help="""Insert the name of the .json file containing crossing ways.""",
                        required=True)
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing footways.""",
                        required=True)
    return parser


def read_file(path):
    """read the file specified by the path"""

    f = open(path)
    fjson = json.load(f)
    df = pd.DataFrame(fjson['data'])
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs='epsg:3035')
    gdf.drop('index', axis=1, inplace=True)
    return gdf


def bike_cross_cycleways(gdf_cycleways, nodes):
    """Find the street nodes within cycling paths"""

    list_bike_cross = []

    for i in range(gdf_cycleways.shape[0]):
        list_bike_cross.append([])

    gdf_cycleways['bike_cross'] = list_bike_cross


    nodes.to_crs(epsg=3035, inplace=True)
    gdf_cycleways.to_crs(epsg=3035, inplace=True)

    s = nodes['geometry'].buffer(2)

    for index, r in gdf_cycleways.iterrows():    
        polygon = r['geometry']
    
        l = list(s.sindex.query(polygon, predicate="intersects"))
        for i in l:
            gdf_cycleways[gdf_cycleways['id_num'] == r.id_num]['bike_cross'].iloc[0].append(nodes.iloc[i].osmid)



def foot_cross(gdf_footways, nodes):
    """Find street nodes within footways"""
    nodes.to_crs(epsg=3035, inplace=True)
    gdf_footways.to_crs(epsg=3035, inplace=True)

    list_foot_cross = []

    for i in range(gdf_footways.shape[0]):
        list_foot_cross.append([])

    gdf_footways['foot_cross'] = list_foot_cross


    s = nodes['geometry'].buffer(2)

    for index, r in gdf_footways.iterrows():    
        polygon = r['geometry']
    
        l = list(s.sindex.query(polygon, predicate="intersects"))
        for i in l:
            gdf_footways[gdf_footways['id_num'] == r.id_num]['foot_cross'].iloc[0].append(nodes.iloc[i].osmid)


def junction_cross_crossing_ways(gdf_crossing_ways, nodes):
    """Find street nodes within crossing mapped as ways"""

    list_junction_cross = []

    for i in range(gdf_crossing_ways.shape[0]):
        list_junction_cross.append([])

    gdf_crossing_ways['junction_crosses'] = list_junction_cross

    s = nodes['geometry'].buffer(2)

    for index, r in gdf_crossing_ways.iterrows():
        polygon = r['geometry']

        l = list(s.sindex.query(polygon, predicate="intersects"))
        for i in l:
            gdf_crossing_ways[gdf_crossing_ways['id_num'] == r.id_num]['junction_crosses'].iloc[0].append(
                nodes.iloc[i].osmid)


def save_gdf(gdf, path):
    """
    save the geopandas DataFrame in a json file
    """

    gdf.to_crs(epsg=4326, inplace=True)
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].astype(str)
    df.to_json(path, orient='table')


def preprocessing(gdf_cycleways, gdf_footways, gdf_crossing_ways, options):
    G_total = ox.graph_from_point((options.lat, options.lon),
                                  dist=int(options.dist),
                                  dist_type='bbox',
                                  simplify=False,
                                  network_type='all_private'
                                  )

    nodes, edges = ox.graph_to_gdfs(G_total)
    nodes.reset_index(inplace=True)
    nodes.to_crs(epsg=3035, inplace=True)

    bike_cross_cycleways(gdf_cycleways, nodes)
    print("Creation of column bike_cross in gdf_cycleways GeoDataFrame : done")
    #print(gdf_cycleways['bike_cross'])

    foot_cross(gdf_footways, nodes)
    print("Creation of column foot_cross in gdf_footways GeoDataFrame : done")
    #print(gdf_footways['foot_cross'])

    junction_cross_crossing_ways(gdf_crossing_ways, nodes)
    print("Creation of column bike_cross in gdf_crossing_ways GeoDataFrame : done")


def main(args=None):

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'

    """Read the content of the json file, store it in a geodataframe and apply the preprocessing"""
    gdf_cycleways = read_file(path + options.file_name_cycleways)
    gdf_crossing_ways = read_file(path + options.file_name_crossing_ways)
    gdf_footways = read_file(path + options.file_name_footways)

    gdf_cycleways.to_crs(epsg=3035, inplace=True)
    gdf_footways.to_crs(epsg=3035, inplace=True)
    gdf_crossing_ways.to_crs(epsg=3035, inplace=True)

    preprocessing(gdf_cycleways, gdf_footways, gdf_crossing_ways, options)

    """Store the results in json files"""
    save_gdf(gdf_cycleways, path + options.file_name_cycleways)
    save_gdf(gdf_crossing_ways, path + options.file_name_crossing_ways)
    save_gdf(gdf_footways, path + options.file_name_footways)



if __name__ == "__main__":
    main()