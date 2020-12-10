import datetime
from pyspark.sql import Window as window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# load mongo data
input_uri = "mongodb://34.80.90.86/db.twitter_search"
output_uri = "mongodb://34.80.90.86/db.twitter_search"

# create spark session
my_spark = SparkSession\
    .builder\
    .appName("SpaceBar")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()

# read data from mongodb
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df2 = df.select('timestamp_ms', 'source')

#convert timestamp
df2 = df2.withColumn("time", F.to_utc_timestamp(F.from_unixtime(F.col("timestamp_ms")/1000,'yyyy-MM-dd HH:mm:ss'),'UTC'))

#select tool from source
df2 = df2.withColumn("tool", when(col("source").contains("tweetdeck"),"Tweetdeck")
                                 .when(col("source").contains("mobile"),"mobile")
				 .when(col("source").contains("android"),"android")
				 .when(col("source").contains("iPhone"),"iPhone")
				 .when(col("source").contains("Android"),"android")
				 .when(col("source").contains("iPad"),"iPad")
                                 .otherwise("Unknown"))
df3 = df2.select('time', 'timestamp_ms',  'tool')
df3.show()

#export to csv
df3.toPandas().to_csv('data.csv')




