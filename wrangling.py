from pyspark.sql import SparkSession
from pyspark.sql.functions import substring 
from car_schema import *

# Connect with local spark 
sprk_session = SparkSession.builder.master("local[1]") \
                        .appName('SparkByExamples.com') \
                        .getOrCreate()

# Load csv files from de dataset
base_path = 'car_tables/'
df_price = sprk_session.read.csv(base_path + 'Price_table.csv', header=True, schema=schema_price)
df_sales = sprk_session.read.csv(base_path + 'Sales_table.csv', header=True, schema=schema_sales)
df_trim = sprk_session.read.csv(base_path + 'Trim_table.csv', header=True, schema=schema_trim)
df_ads = sprk_session.read.csv(base_path + 'Ad_table.csv', header=True, schema=schema_ads)

# Remove outliers from price table, 3 * standard deviation
stddevp = df_price.agg({'Entry_Price': 'stddev'}).collect()[0][0]
meanp = df_price.agg({'Entry_Price': 'mean'}).collect()[0][0]
out_highp = meanp + (3 * stddevp)
out_lowp = meanp - (3 * stddevp)

df_price = df_price.where(f"Entry_Price > {out_lowp} AND Entry_Price < {out_highp}")

# Adding entry price from new to Ads Table for reference
join_cond_ad = (df_price.Maker       == df_ads.Adv_Maker      ) & \
               (df_price.Genmodel    == df_ads.Adv_Genmodel   ) & \
               (df_price.Genmodel_ID == df_ads.Adv_Genmodel_ID) 

df_ads = df_ads.join(df_price, on = join_cond_ad, how = 'left')

# Cols duplicates with data duplication, remove null values from price
dupli_cols = ['Maker', 'Genmodel', 'Genmodel_ID']
df_ads = df_ads.drop(*dupli_cols).dropna(subset=['Price'])

# Engin_size is a string, we need to convert to double
df_ads = df_ads.withColumn('Engin_sizeD', substring(df_ads.Engin_size, 1, 3)).drop('Engin_size')
df_ads = df_ads.withColumn('Engin_size', df_ads.Engin_sizeD.cast(DoubleType())).drop('Engin_sizeD')

# New feature devaluation per year, remove null values
df_ads_dev = df_ads.withColumn('Deval_year',
                                (df_ads.Entry_Price - df_ads.Price) \
                                    / (df_ads.Reg_year - df_ads.Adv_year)
                              ).dropna(subset=['Deval_year'])


# We save the features engineer for later for an ML model
df_ads_dev.write.parquet(base_path + 'Ads_table_features.parquet', mode='overwrite')

sprk_session.stop()
