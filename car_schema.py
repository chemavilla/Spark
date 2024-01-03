from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#CSV import from spark gets numeric as string
schema_price=StructType([
    StructField("Maker"      , StringType() , False),
    StructField("Genmodel"   , StringType() , False),
    StructField("Genmodel_ID", StringType() , False),
    StructField("Year"       , IntegerType(), False),
    StructField("Entry_Price", IntegerType(), False)])
 
schema_sales=StructType([
    StructField("Maker"      , StringType() , False),
    StructField("Genmodel"   , StringType() , False),
    StructField("Genmodel_ID", StringType() , False)])

#Sales table has 20 integer fields [2000...2020]
for i in range(0,20):
    schema_sales.add(StructField(f"20{i:02d}", IntegerType(), False))

schema_trim=StructType([
    StructField("Genmodel_ID", StringType() , False),
    StructField("Maker"      , StringType() , False),
    StructField("Genmodel"   , StringType() , False),
    StructField("Trim"       , StringType() , False),
    StructField("Year"       , IntegerType(), False),
    StructField("Price"      , IntegerType(), False),
    StructField("Emission"   , IntegerType(), False),
    StructField("Fuel"       , StringType() , False),
    StructField("Engine"     , IntegerType(), False)])

schema_ads=StructType([
    StructField("Adv_Maker"       , StringType() , False),
    StructField("Adv_Genmodel"    , StringType() , False),
    StructField("Adv_Genmodel_ID" , StringType() , False),
    StructField("Adv_ID"      , StringType() , False),
    StructField("Adv_year"    , StringType() , False),
    StructField("Adv_month"   , IntegerType(), False),
    StructField("Color"       , StringType() , False),
    StructField("Reg_year"    , StringType() , False),
    StructField("Bodytype"    , StringType() , False),
    StructField("Runned_Miles", IntegerType(), False),
    StructField("Engin_size"  , StringType() , False),
    StructField("Gearbox"     , StringType() , False),
    StructField("Fuel_type"   , StringType() , False),
    StructField("Price"       , IntegerType(), False),
    StructField("Seat_num"    , IntegerType(), False),
    StructField("Door_num"    , IntegerType(), False)])