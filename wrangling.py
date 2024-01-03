from pyspark.sql import SparkSession 
from car_schema import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

base_path= 'car_tables/'
df_price=spark.read.csv(base_path+'Price_table.csv',header=True, schema=schema_price)
df_sales=spark.read.csv(base_path+'Sales_table.csv',header=True, schema=schema_sales)
df_trim =spark.read.csv(base_path+'Trim_table.csv' ,header=True, schema=schema_trim)
df_ads  =spark.read.csv(base_path+'Ad_table.csv'   ,header=True, schema=schema_ads)


#remove outliers from price table, 3 * stddev
stddevp  = df_price.agg({'Entry_Price': 'stddev'}).collect()[0][0]
meanp    = df_price.agg({'Entry_Price': 'mean'}).collect()[0][0]
out_highp= meanp + (3*stddevp)
out_lowp = meanp - (3*stddevp)

df=df_price.where(f"Entry_Price > {out_lowp} AND Entry_Price < {out_highp}")

#comprobar duplicados y valores null

#quitar columnas


#Adding entry price from new to Ads Table for reference
join_cond_ad=(df_price.Maker       == df_ads.Adv_Maker      ) & \
             (df_price.Genmodel    == df_ads.Adv_Genmodel   ) & \
             (df_price.Genmodel_ID == df_ads.Adv_Genmodel_ID) & \
             (df_price.Year        == df_ads.Adv_year       ) 

df_ads = df_ads.join(df_price, on=join_cond_ad, how='left')

dupli_cols=['Maker', 'Genmodel', 'Genmodel_ID']
df_ads = df_ads.drop(*dupli_cols)