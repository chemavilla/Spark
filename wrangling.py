from pyspark.sql import SparkSession 
from car_schema import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

base_path= 'car_tables/'
dfp=spark.read.csv(base_path+'Price_table.csv',header=False, schema=schema_price)
dfs=spark.read.csv(base_path+'Sales_table.csv',header=False, schema=schema_sales)

#remove outliers from price table, 3 * stddev
stddevp  = dfp.agg({'Price': 'stddev'}).collect()[0][0]
meanp    = dfp.agg({'Price': 'mean'}).collect()[0][0]
out_highp= meanp + (3*stddevp)
out_lowp = meanp - (3*stddevp)

dfp=dfp.where(f"Price > {out_lowp} AND Price < {out_highp}")
dfp.show()