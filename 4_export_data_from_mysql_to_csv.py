#!/usr/bin/env python
import pymysql
import pandas as pd

# Connect to MySQL database
db_conn = pymysql.connect(host='localhost', port=3306, user='hduser', password='Hadoop@123', database='pdaproject')

# Fetch data of year_disease_cases and write to CSV file
year_disease_cases_df = pd.read_sql_query('select * from year_disease_cases', con = db_conn)
year_disease_cases_df.to_csv("year_disease_cases.csv", index=False)

# Fetch data of year_total_cases and write to CSV file
year_total_cases_df = pd.read_sql_query('select * from year_total_cases', con = db_conn)
year_total_cases_df.to_csv("year_total_cases.csv", index=False)

# Fetch data of year_disease_incidences and write to CSV file
year_disease_incidences_df = pd.read_sql_query('select * from year_disease_incidences', con = db_conn)
year_disease_incidences_df.to_csv("year_disease_incidences.csv", index=False)

# Fetch data of year_avg_incidences and write to CSV file
year_avg_incidences_df = pd.read_sql_query('select * from year_avg_incidences', con = db_conn)
year_avg_incidences_df.to_csv("year_avg_incidences.csv", index=False)

# Fetch data of state_disease_cases and write to CSV file
state_disease_cases_df = pd.read_sql_query('select * from state_disease_cases', con = db_conn)
state_disease_cases_df.to_csv("state_disease_cases.csv", index=False)

# Fetch data of state_total_cases and write to CSV file
state_total_cases_df = pd.read_sql_query('select * from state_total_cases', con = db_conn)
state_total_cases_df.to_csv("state_total_cases.csv", index=False)

# Fetch data of state_disease_incidences and write to CSV file
state_disease_incidences_df = pd.read_sql_query('select * from state_disease_incidences', con = db_conn)
state_disease_incidences_df.to_csv("state_disease_incidences.csv", index=False)

# Fetch data of state_avg_incidences and write to CSV file
state_avg_incidences_df = pd.read_sql_query('select * from state_avg_incidences', con = db_conn)
state_avg_incidences_df.to_csv("state_avg_incidences.csv", index=False)

# Fetch data of overall_cases and write to CSV file
overall_cases_df = pd.read_sql_query('select * from overall_cases', con = db_conn)
overall_cases_df.to_csv("overall_cases.csv", index=False)

# Fetch data of overall_incidences and write to CSV file
overall_incidences_df = pd.read_sql_query('select * from overall_incidences', con = db_conn)
overall_incidences_df.to_csv("overall_incidences.csv", index=False)

