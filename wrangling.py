from pyspark.sql import SparkSession
from pyspark.sql.functions import substring 
from car_schema import *

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

base_path = 'car_tables/'
df_price = spark.read.csv(base_path + 'Price_table.csv', header=True, schema=schema_price)
df_sales = spark.read.csv(base_path + 'Sales_table.csv', header=True, schema=schema_sales)
df_trim = spark.read.csv(base_path + 'Trim_table.csv', header=True, schema=schema_trim)
df_ads = spark.read.csv(base_path + 'Ad_table.csv', header=True, schema=schema_ads)

# Adding entry price from new to Ads Table for reference
join_cond_ad = (
    (df_price.Maker == df_ads.Adv_Maker) &
    (df_price.Genmodel == df_ads.Adv_Genmodel) &
    (df_price.Genmodel_ID == df_ads.Adv_Genmodel_ID)
)

df_ads = df_ads.join(df_price, on=join_cond_ad, how='left')

# Cols duplicates and register with null value in Price
dupli_cols = ['Maker', 'Genmodel', 'Genmodel_ID']
df_ads = df_ads.drop(*dupli_cols).dropna(subset=['Price'])

# Remove outliers from price table, 3 * stddev
stddevp = df_price.agg({'Entry_Price': 'stddev'}).collect()[0][0]
meanp = df_price.agg({'Entry_Price': 'mean'}).collect()[0][0]
out_highp = meanp + (3 * stddevp)
out_lowp = meanp - (3 * stddevp)

df = df_price.where(f"Entry_Price > {out_lowp} AND Entry_Price < {out_highp}")

# Advertisement cars with devaluation per year
df_ads_dev = df_ads.withColumn(
    'Deval_year',
    (df_ads.Entry_Price - df_ads.Price) / (df_ads.Reg_year - df_ads.Adv_year)
).dropna(subset=['Deval_year'])

# Categorical variables to numeric to calculate the correlation with Devaluation

# Engin_size String -> Double
df_ads = df_ads.withColumn('Engin_sizeD', substring(df_ads.Engin_size, 1, 3)).drop('Engin_size')
base_path = 'car_tables/'
df_price = spark.read.csv(base_path + 'Price_table.csv',header=True, schema=schema_price)
df_sales = spark.read.csv(base_path + 'Sales_table.csv',header=True, schema=schema_sales)
df_trim  = spark.read.csv(base_path + 'Trim_table.csv' ,header=True, schema=schema_trim)
df_ads   = spark.read.csv(base_path + 'Ad_table.csv'   ,header=True, schema=schema_ads)

# Adding entry price from new to Ads Table for reference
join_cond_ad = (df_price.Maker       == df_ads.Adv_Maker      ) & \
               (df_price.Genmodel    == df_ads.Adv_Genmodel   ) & \
               (df_price.Genmodel_ID == df_ads.Adv_Genmodel_ID) 

df_ads = df_ads.join(df_price, on = join_cond_ad, how = 'left')

# Cols duplicates and register with null value in Price
dupli_cols = ['Maker', 'Genmodel', 'Genmodel_ID']
df_ads = df_ads.drop(*dupli_cols).dropna(subset=['Price'])

# Remove outliers from price table, 3 * stddev
stddevp   = df_price.agg({'Entry_Price': 'stddev'}).collect()[0][0]
meanp     = df_price.agg({'Entry_Price': 'mean'}).collect()[0][0]
out_highp = meanp + (3*stddevp)
out_lowp  = meanp - (3*stddevp)

df=df_price.where(f"Entry_Price > {out_lowp} AND Entry_Price < {out_highp}")


# Advertisement cars with devaluation per year
df_ads_dev = df_ads.withColumn('Deval_year',
                               (df_ads.Entry_Price - df_ads.Price)/
                               (df_ads.Reg_year - df_ads.Adv_year))\
                   .dropna(subset=['Deval_year'])

# Categorical variables to numeric to calculate the correlation with Devaluation

# Engin_size String -> Double
df_ads = df_ads.withColumn('Engin_sizeD', substring(df_ads.Engin_size, 1, 3)).drop('Engin_size')
df_ads = df_ads.withColumn('Engin_size', df_ads.Engin_sizeD.cast(DoubleType())).drop('Engin_sizeD')

# We save the features engineer for later doing a easy ML model
# df_ads_dev.write.parquet('car_tables/Ads_table_features.parquet')

# spark.stop()
