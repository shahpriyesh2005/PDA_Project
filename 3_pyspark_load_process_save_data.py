#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType

# Create a spark session to connect MongoDB using Spark-Mongo Connector
spark_session = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/") \
    .getOrCreate()

# Load data from MongoDB collections to data frames
hepatitis_df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://localhost:27017/pdaproject.hepatitis").load()
measles_df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/pdaproject.measles").load()
mumps_df = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/pdaproject.mumps").load()

# Set alias of each data frame
hep = hepatitis_df.alias('hep')
mea = measles_df.alias('mea')
mum = mumps_df.alias('mum')

# Drop the _id column set by default in MongoDB from the data frames as they are not required
hep = hep.drop('_id')
mea = mea.drop('_id')
mum = mum.drop('_id')

##################################################

# Create a new column year which is taken as a substring of the week. E.g.: Week 198001 means year 1980 and week 01. So extract the year from the value of week column
# Filter the data to get records of year greater than or equal to 1980
# Aggregate the weekly values for each year into total cases which is the sum of all weekly cases of that year
hep_cases_df = hep.withColumn("year", hep.week.substr(1,4)).filter(func.col("year")>=1980).groupBy("year").agg(func.sum("cases").alias("hep_total_cases")).orderBy("year")

mea_cases_df = mea.withColumn("year", mea.week.substr(1,4)).filter(func.col("year")>=1980).groupBy("year").agg(func.sum("cases").alias("mea_total_cases")).orderBy("year")

mum_cases_df = mum.withColumn("year", mum.week.substr(1,4)).filter(func.col("year")>=1980).groupBy("year").agg(func.sum("cases").alias("mum_total_cases")).orderBy("year")

# Join the 3 data frames based on year
cases_inner_join = hep_cases_df.join(mea_cases_df, on=["year"], how="inner").join(mum_cases_df, on=["year"], how="inner").orderBy("year", ascending=True)

# Save data in MySQL table year_disease_cases having columns: year, hep_total_cases, mea_total_cases, and mum_total_cases
cases_inner_join.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='year_disease_cases', user='hduser', password='Hadoop@123').mode('overwrite').save()

# Save data in MySQL table year_total_cases having columns: year and total_cases
cases_inner_join.withColumn("total_cases", col("hep_total_cases") + col("mea_total_cases") + col("mum_total_cases")).select("year", "total_cases").write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='year_total_cases', user='hduser', password='Hadoop@123').mode('overwrite').save()

##################################################

# Create a new column year which is taken as a substring of the week. E.g.: Week 198001 means year 1980 and week 01. So extract the year from the value of week column
# Filter the data to get records of year greater than or equal to 1980
# Aggregate the weekly values for each year into average incidence per capita which is the average of all weekly incidences per capita of that year
hep_incidences_df = hep.withColumn("year", hep.week.substr(1,4)).filter(func.col("year")>=1980).groupBy("year").agg(func.avg("incidence_per_capita").alias("hep_avg_incidence_per_capita")).orderBy("year")

mea_incidences_df = mea.withColumn("year", mea.week.substr(1,4)).filter(func.col("year")>=1980).groupBy("year").agg(func.avg("incidence_per_capita").alias("mea_avg_incidence_per_capita")).orderBy("year")

mum_incidences_df = mum.withColumn("year", mum.week.substr(1,4)).filter(func.col("year")>=1980).groupBy("year").agg(func.avg("incidence_per_capita").alias("mum_avg_incidence_per_capita")).orderBy("year")

# Join the 3 data frames based on year
incidences_inner_join = hep_incidences_df.join(mea_incidences_df, on=["year"], how="inner").join(mum_incidences_df, on=["year"], how="inner").orderBy("year", ascending=True)

# Save data in MySQL table year_disease_incidences having columns: year, hep_avg_incidence_per_capita, mea_avg_incidence_per_capita, and mum_avg_incidence_per_capita
incidences_inner_join.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='year_disease_incidences', user='hduser', password='Hadoop@123').mode('overwrite').save()

# Save data in MySQL table year_avg_incidences having columns: year and avg_incidences
incidences_inner_join.withColumn("avg_incidences", (col("hep_avg_incidence_per_capita") + col("mea_avg_incidence_per_capita") + col("mum_avg_incidence_per_capita")) / 3).select("year", "avg_incidences").write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='year_avg_incidences', user='hduser', password='Hadoop@123').mode('overwrite').save()

##################################################

# Filter the data to get records of year greater than or equal to 1980 and less than or equal to 2002
# Aggregate the weekly values for each state into total cases which is the sum of all weekly cases of that state
hep_states_cases_df = hep.filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("state_name").agg(func.sum("cases").alias("hep_total_cases")).orderBy("state_name")

mea_states_cases_df = mea.filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("state_name").agg(func.sum("cases").alias("mea_total_cases")).orderBy("state_name")

mum_states_cases_df = mum.filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("state_name").agg(func.sum("cases").alias("mum_total_cases")).orderBy("state_name")

# Join the 3 data frames based on state
state_cases_inner_join = hep_states_cases_df.join(mea_states_cases_df, on=["state_name"], how="inner").join(mum_states_cases_df, on=["state_name"], how="inner").orderBy("state_name", ascending=True)

# Calculate the total cases for each state which is a sum of the total cases of all 3 diseases
state_total_cases_df = state_cases_inner_join.withColumn("total_cases", col("hep_total_cases") + col("mea_total_cases") + col("mum_total_cases")).select("state_name", "total_cases")

# Find the top 10 states having the highest number of total cases
cases_top10_df = state_total_cases_df.orderBy("total_cases", ascending=False).select("state_name").limit(10)

state_disease_cases_top10_df = state_cases_inner_join.join(cases_top10_df, on=["state_name"], how="inner").orderBy("state_name", ascending=True)

state_total_cases_top10_df = state_total_cases_df.join(cases_top10_df, on=["state_name"], how="inner").orderBy("total_cases", ascending=False)

# Save data in MySQL table state_disease_cases having columns: state_name, hep_total_cases, mea_total_cases, and mum_total_cases
state_disease_cases_top10_df.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='state_disease_cases', user='hduser', password='Hadoop@123').mode('overwrite').save()

# Save data in MySQL table state_total_cases having columns: state_name and total_cases
state_total_cases_top10_df.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='state_total_cases', user='hduser', password='Hadoop@123').mode('overwrite').save()

##################################################

# Filter the data to get records of year greater than or equal to 1980 and less than or equal to 2002
# Aggregate the weekly values for each state into average incidence per capita which is the average of all weekly incidences per capita of that state
hep_states_incidences_df = hep.filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("state_name").agg(func.avg("incidence_per_capita").alias("hep_avg_incidence_per_capita")).orderBy("state_name")

mea_states_incidences_df = mea.filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("state_name").agg(func.avg("incidence_per_capita").alias("mea_avg_incidence_per_capita")).orderBy("state_name")

mum_states_incidences_df = mum.filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("state_name").agg(func.avg("incidence_per_capita").alias("mum_avg_incidence_per_capita")).orderBy("state_name")

# Join the 3 data frames based on state
state_incidences_inner_join = hep_states_incidences_df.join(mea_states_incidences_df, on=["state_name"], how="inner").join(mum_states_incidences_df, on=["state_name"], how="inner").orderBy("state_name", ascending=True)

# Calculate the average incidences per capita for each state which is an overall average of the average incidences per capita of all 3 diseases
state_avg_incidences_df = state_incidences_inner_join.withColumn("avg_incidences", (col("hep_avg_incidence_per_capita") + col("mea_avg_incidence_per_capita") + col("mum_avg_incidence_per_capita")) / 3).select("state_name", "avg_incidences")

# Find the top 10 states having the highest average incidences per capita
incidences_top10_df = state_avg_incidences_df.orderBy("avg_incidences", ascending=False).select("state_name").limit(10)

state_disease_incidences_top10_df = state_incidences_inner_join.join(incidences_top10_df, on=["state_name"], how="inner").orderBy("state_name", ascending=True)

state_avg_incidences_top10_df = state_avg_incidences_df.join(incidences_top10_df, on=["state_name"], how="inner").orderBy("avg_incidences", ascending=False)

# Save data in MySQL table state_disease_incidences having columns: state_name, hep_avg_incidence_per_capita, mea_avg_incidence_per_capita, and mea_avg_incidence_per_capita
state_disease_incidences_top10_df.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='state_disease_incidences', user='hduser', password='Hadoop@123').mode('overwrite').save()

# Save data in MySQL table state_avg_incidences having columns: state_name and avg_incidences
state_avg_incidences_top10_df.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='state_avg_incidences', user='hduser', password='Hadoop@123').mode('overwrite').save()

##################################################

# Create a new column disease having the names of the diseases as values
# Filter the data to get records of year greater than or equal to 1980 and less than or equal to 2002
# Aggregate the weekly values into total cases which is the sum of all weekly cases
hep_overall_cases_df = hep.withColumn("disease", func.lit("Hepatitis A")).filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("disease").agg(func.sum("cases").alias("total_cases"))

mea_overall_cases_df = mea.withColumn("disease", func.lit("Measles")).filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("disease").agg(func.sum("cases").alias("total_cases"))

mum_overall_cases_df = mum.withColumn("disease", func.lit("Mumps")).filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("disease").agg(func.sum("cases").alias("total_cases"))

# Union of the 3 disease counts
overall_cases_df = hep_overall_cases_df.union(mea_overall_cases_df).union(mum_overall_cases_df)

# Save data in MySQL table overall_cases having columns: disease and total_cases
overall_cases_df.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='overall_cases', user='hduser', password='Hadoop@123').mode('overwrite').save()

##################################################

# Create a new column disease having the names of the diseases as values
# Filter the data to get records of year greater than or equal to 1980 and less than or equal to 2002
# Aggregate the weekly values into average incidences which is the average of all weekly incidences per capita
hep_overall_incidences_df = hep.withColumn("disease", func.lit("Hepatitis A")).filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("disease").agg(func.avg("incidence_per_capita").alias("avg_incidences"))

mea_overall_incidences_df = mea.withColumn("disease", func.lit("Measles")).filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("disease").agg(func.avg("incidence_per_capita").alias("avg_incidences"))

mum_overall_incidences_df = mum.withColumn("disease", func.lit("Mumps")).filter(func.col("week").substr(1,4)>=1980).filter(func.col("week").substr(1,4)<=2002).groupBy("disease").agg(func.avg("incidence_per_capita").alias("avg_incidences"))

# Union of the 3 disease counts
overall_incidences_df = hep_overall_incidences_df.union(mea_overall_incidences_df).union(mum_overall_incidences_df)

# Save data in MySQL table overall_incidences having columns: disease and avg_incidences
overall_incidences_df.write.format('jdbc').options(url='jdbc:mysql://localhost:3306/pdaproject', driver='com.mysql.cj.jdbc.Driver', dbtable='overall_incidences', user='hduser', password='Hadoop@123').mode('overwrite').save()

##################################################

spark_session.stop()


