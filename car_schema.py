from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#CSV import from spark gets numeric as string
schema_price=StructType([
    StructField("Maker"      , StringType() , False),
    StructField("Genmodel"   , StringType() , False),
    StructField("Genmodel_ID", StringType() , False),
    StructField("Year"       , IntegerType(), False),
    StructField("Price"      , IntegerType(), False)])
 
schema_sales=StructType([
    StructField("Maker"      , StringType() , False),
    StructField("Genmodel"   , StringType() , False),
    StructField("Genmodel_ID", StringType() , False)])

#Sales table has 20 integer fields [2000...2020]
for i in range(0,20):
    schema_sales.add(StructField(f"20{i:02d}", IntegerType(), False))
