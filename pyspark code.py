# import pyspark liberary & sparkSession &sql function

import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.functions import col


#--Tells wget to output the downloaded file directly to standard output (the terminal or the next command in the pipeline), instead of saving it to a file.
# | (Pipe)
#-Sends the output of the wget command (the file content) as input to the next command (aws s3 cp).
#This avoids storing the file locally and directly streams the data to the next step.
# aws s3 cp - s3://store-raw-data-yml/city_market_tracker.tsv000.gz
#Command: aws s3 cp
#AWS CLI command to copy files to or from an S3 bucket.

%%bash
wget -O- https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz | aws s3 cp - s3://store-row-data-emr/city_market_tracker.tsv000.gz

#Create object form spark Session
spark = SparkSession.builder.appName('redfineDataAnalysis').getOrCreate()

#assign the data into a variable 
redfin_data = spark.read.csv("data.csv", header=True, inferSchema=True)

#show only the first 3 row of the row data
redfin_data.show(3)

#Check the schema
redfin_data.printSchema()

#print column names
redfin_data.columns

# use Select function to select some of the columns
df_redfin = redfin_data.select(['period_end','period_duration', 'city', 'state', 'property_type',
    'median_sale_price', 'median_ppsf', 'homes_sold', 'inventory', 'months_of_supply', 'median_dom', 'sold_above_list', 'last_updated'])
df_redfin.show(3)

#check total number of rows
print(f"Total number of rows: {df_redfin.count()}")

# Count null values in each column
from pyspark.sql.functions import isnull
null_counts = [df_redfin.where(isnull(col_name)).count() for col_name in df_redfin.columns]
null_counts

# Display the count of null values in each columns

for i, col_name in enumerate(df_redfin.columns):
    print(f"{col_name}: {null_counts[i]} null values")

# Check for missing values in the entire DataFrame
remaining_count = df_redfin.na.drop().count()

print(f"Number of missing rows: {df_redfin.count() - remaining_count}")

print(f"Total number of remaining rows: {remaining_count}")


#remove na and count total number of remaining rows
df_redfin = df_redfin.na.drop()
print(f"Total number of rows: {df_redfin.count()}")

# Count null values in each column to confirm if we have removed all na
null_counts = [df_redfin.where(isnull(col_name)).count() for col_name in df_redfin.columns]
null_counts

from pyspark.sql.functions import year, month
#Extract year from period_end and save in a new column "period_end_yr"
df_redfin = df_redfin.withColumn("period_end_yr", year(col("period_end")))

#Extract month from period_end and save in a new column "period_end_month"
df_redfin = df_redfin.withColumn("period_end_month", month(col("period_end")))

# Drop period_end and last_updated columns
df_redfin = df_redfin.drop("period_end", "last_updated")

df_redfin.show(3)


from pyspark.sql.functions import when

#let's map the month number to their respective month name.

df_redfin = df_redfin.withColumn("period_end_month", 
                   when(col("period_end_month") == 1, "January")
                   .when(col("period_end_month") == 2, "February")
                   .when(col("period_end_month") == 3, "March")
                   .when(col("period_end_month") == 4, "April")
                   .when(col("period_end_month") == 5, "May")
                   .when(col("period_end_month") == 6, "June")
                   .when(col("period_end_month") == 7, "July")
                   .when(col("period_end_month") == 8, "August")
                   .when(col("period_end_month") == 9, "September")
                   .when(col("period_end_month") == 10, "October")
                   .when(col("period_end_month") == 11, "November")
                   .when(col("period_end_month") == 12, "December")
                   .otherwise("Unknown")
                 )

df_redfin.show(3)

#let us write the final dataframe into our s3 bucket as a parquet file.
s3_bucket = "s3://redfin-transform-zone-yml/redfin_data.parquet"
df_redfin.write.mode("overwrite").parquet(s3_bucket)