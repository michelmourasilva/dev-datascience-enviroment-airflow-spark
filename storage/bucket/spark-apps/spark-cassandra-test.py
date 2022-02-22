import sys, csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import datediff, col, abs
from pyspark import SparkConf

# change ip for spark cluster
# maybe is necessary install or download jar connector for cassandra
# ex:
# ./bin/pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions
# https://spark-packages.org/package/datastax/spark-cassandra-connector
spark = SparkSession.builder \
.appName("extract_and_load") \
.config("spark.cassandra.connection.host", "172.20.0.11") \
.config("spark.cassandra.connection.port", "9042") \
.config("spark.cassandra.auth.username", "cassandra") \
.config("spark.cassandra.auth.password", "cassandra") \
.getOrCreate()

def load_and_get_table_df(keys_space_name, table_name):
    table_df = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df

load_and_get_table_df("test", "previous_employees_by_job_title").show()


csv_df = spark.read.format("csv").option("header", "true").load("/usr/local/spark/data/previous_employees_by_job_title.txt")

write_df = csv_df.select("job_title", "employee_id", "employee_name", "first_day", "last_day")

write_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="previous_employees_by_job_title", keyspace='test')\
    .save()

# configure database catalog
spark.conf.set(f"spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog")
spark.sql("use cassandra.test")


calcDF = spark.sql("select job_title, employee_id, employee_name, abs(datediff(last_day, first_day)) as days_worked from previous_employees_by_job_title")

calcDF.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="days_worked_by_previous_employees_by_job_title", keyspace='test')\
    .save()


spark.stop()
