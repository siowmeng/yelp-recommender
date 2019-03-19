import os
import json
from rating_data_utils import *
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

dataDir = '/media/lowsiowmeng/DATA/GitRepo/yelper_recommendation_system/data/yelp_dataset/'

appName = "Yelper Recommendation System"
conf = SparkConf().setAppName(appName).setMaster("local")
sc = SparkContext(conf=conf)
# sc.setLogLevel("WARN") # config log level to make console less verbose
sqlContext = SQLContext(sc)

# Map userid and biz id to fixed list of integers
# NEW - no longer mapping user ID and biz ID to integer
#userid_to_index_map = load_userid_to_index_map(dataDir)
#bizid_to_index_map = load_bizid_to_index_map(dataDir)

targetCities = [('NV', 'Las Vegas'), 
                ('AZ', 'Phoenix'), 
                ('NC', 'Charlotte'), 
                ('PA', 'Pittsburgh'), 
                ('WI', 'Madison')]

parsed_ratingdata_all_cities = build_rating_df_for_cities(sc, sqlContext, dataDir, targetCities)

base_dir = dataDir + "processed_data/"

# process all major cities
#us_cities = [
#                ("NC", "us_charlotte"), 
#                ("NV", "us_lasvegas"), 
#                ("WI", "us_madison"),
#                ("AZ", "us_phoenix"), 
#                ("PA", "us_pittsburgh"), 
#                ("IL", "us_urbana_champaign")
#            ]
#canada_cities = [("QC", "canada_montreal")]
#germany_cities = [("BW", "germany_karlsruhe")]
#uk_cities = [("EDH", "uk_edinburgh")]

#cities = us_cities + canada_cities + germany_cities + uk_cities
#city_names = [p[1] for p in cities]
#
for state_name, city_name in [('NV', 'Las Vegas')]:
    builder = RatingDataBuilderForCity(state_name, city_name, base_dir, sc)
    builder.process_one_city(parsed_ratingdata_all_cities)
#
#print ("The rating data of all {} cities were successfully processed!".format(len(city_names)))
#
#
