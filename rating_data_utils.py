import os
import json
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# ---------------------------------------------------------------------------
class RatingDataBuilderForCity:

    def __init__(self, state_name, city_name, base_dir, spark_context):
        self.state_name = state_name
        self.city_name = city_name
        self.base_dir = base_dir
        self.spark_context = spark_context

    def __get_and_write_userid_businessid_star_tuple_in_city(self, starsDF):
        
        file_name = self.state_name + '-' + self.city_name + "-reviewStars.csv"
        csv_full_path = os.path.join(self.base_dir, file_name)
        
        selectHeader = ['user_id', 'business_id', 'stars']
        starsDF = starsDF.select(selectHeader)
        starsDF = starsDF.coalesce(1)
        starsDF.write.csv(csv_full_path, header = True)
        print ("Done! tuple written for city {}!".format(self.city_name))
        
    def process_one_city(self, reviewDF):
        
        print ("Processing {}".format(self.state_name) + ", {}".format(self.city_name))
        starsDF = reviewDF.filter((reviewDF.state == self.state_name) & (reviewDF.city == self.city_name))
        print (starsDF.count())
        self.__get_and_write_userid_businessid_star_tuple_in_city(starsDF)


# ---------------------------------------------------------------------------
# NEW - no longer mapping user ID and biz ID to integer
#def load_userid_to_index_map(dataDir):
#    with open(dataDir + 'userid_to_index_map.json', 'r') as fp:
#        userid_to_index_map = json.load(fp)
#        print ("userid_to_index_map json file loaded!")
#    return userid_to_index_map
#
#def load_bizid_to_index_map(dataDir):
#    with open(dataDir + 'bizid_to_index_map.json', 'r') as fp:
#        bizid_to_index_map = json.load(fp)
#        print ("json file loaded!")
#    return bizid_to_index_map

# ---------------------------------------------------------------------------
def build_rating_df_for_cities(spark_context, sql_context,
                               dataDir, listCities):
    
    # load the data files
    bizFile = 'business.json'    
    biz_raw_data = spark_context.textFile(os.path.join(dataDir, bizFile))
    biz_json_rdd = biz_raw_data.map(lambda line: json.loads(line))
    biz_json_rdd = biz_json_rdd.map(lambda jsonDoc: (jsonDoc['business_id'], jsonDoc))
    #print(biz_json_rdd.take(2))
    
    reviewFile = 'review.json'
    review_raw_data = spark_context.textFile(os.path.join(dataDir, reviewFile))
    review_json_rdd = review_raw_data.map(lambda line: json.loads(line))
    review_json_rdd = review_json_rdd.map(lambda jsonDoc: (jsonDoc['business_id'], jsonDoc))
    #print(json.dumps(review_json_rdd.take(2), indent = 4, sort_keys = True))
    
    review_json_rdd = review_json_rdd \
        .leftOuterJoin(biz_json_rdd) \
        .map(lambda kv: ((kv[1][1]['state'].strip(), kv[1][1]['city'].strip()), kv[1][0]))
    
    #review_json_rdd.filter(lambda kv: kv[0] in listCities)
    #print(review_json_rdd.take(2))
    
    converted_ratings_data = review_json_rdd \
        .map(lambda kv: (kv[0][0], kv[0][1], kv[1]['user_id'], kv[1]['business_id'], kv[1]['stars'])) \
        .cache()
    
    parsed_ratingdata_for_cities = sql_context.createDataFrame(converted_ratings_data, ['state', 'city', 'user_id', 'business_id', 'stars'])
    parsed_ratingdata_for_cities.printSchema()

    return parsed_ratingdata_for_cities
    
#    rating_raw_data = spark_context.textFile(os.path.join(review_data_dir, csv_file_name))
#
#    header = rating_raw_data.take(1)[0]
#
#    ratings_data = rating_raw_data \
#        .filter(lambda line: line != header) \
#        .map(lambda line: line.split(",")) \
#        .map(lambda tokens: (tokens[1],tokens[2],tokens[3])) \
#        .toDF(['user_id', 'business_id', 'stars'])
#
#    ratings_data.show()
#
#    assert ratings_data.select('user_id').count() == 2685066
#    assert ratings_data.select('user_id').distinct().count() == 686556
#
#    def convert_row(row):
#        new_user_id = userid_to_index_map[row.user_id]
#        new_business_id = bizid_to_index_map[row.business_id]
#        stars = int(row.stars)
#        #return Row(user_id=new_user_id, business_id=new_business_id, stars=stars) # no need to return Row
#        return (new_user_id, new_business_id, stars)
#
#    converted_ratings_data = ratings_data.rdd.map(convert_row).cache()
#    print (converted_ratings_data.count())
#    print ("converted_ratings_data done")
#
#    print ("prepare converted_ratings_data dafaframe ...")
#    parsed_ratingdata_for_all_cities = sql_context.createDataFrame(converted_ratings_data, ['user_id', "business_id", "star"])
#    parsed_ratingdata_for_all_cities.printSchema()
#
#    return parsed_ratingdata_for_all_cities

# ---------------------------------------------------------------------------
def load_and_parse_ratingdata_for_city(city_name, base_dir, spark_session):
        # load data for one city
        file_name = "userid_businessid_star_tuple.csv"
        csv_full_path = os.path.join(base_dir, city_name, file_name)

        ratingSchema = StructType([StructField("user_id", IntegerType(), True),
                                   StructField("business_id", IntegerType(), True),
                                   StructField("star", IntegerType(), True)])

        df = spark_session.read.csv(path=csv_full_path, sep=u",", schema=ratingSchema)

        df_rdd = df.rdd
        print (df_rdd.take(3))

        def convert_row(row):
            user_id = int(row.user_id)
            business_id = int(row.business_id)
            star = int(row.star)
            return (user_id, business_id, star)

        converted_df_rdd = df_rdd.map(convert_row)
        print (converted_df_rdd.take(3))
        return converted_df_rdd
