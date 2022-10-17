#!/usr/bin/python3
import os
from datetime import datetime

import findspark

findspark.init() 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

spark = SparkSession.builder \
    .master('local') \
    .appName('covid-etl').getOrCreate()

# Extract
case_df = spark.read.csv(os.path.join(project_dir, "data/Indonesia_coronavirus_daily_data.csv"), header=True,
                         inferSchema=True)
# case_df.printSchema()
# case_df.show()
zone_df = spark.read.csv(os.path.join(project_dir, "data/zone_reference.csv"), header=True, inferSchema=True)
# zone_df.printSchema()
# zone_df.show()

# Transform
grouped_case_df = case_df.filter(to_date(col('Date'), 'dd/mm/yyyy') >= datetime.strptime('01-01-2021', '%d-%m-%Y')) \
    .groupby('Province').agg(sum('Daily_Case').alias("Total_Case"))

# grouped_case_df.show()

summary_df = grouped_case_df.join(zone_df,
                                  (grouped_case_df['Total_Case'] >= zone_df['Min_Total_case']) &
                                  (grouped_case_df['Total_Case'] <= zone_df['Max_Total_Case']),
                                  how='inner').select('Province', 'Total_Case', 'Zone')

# summary_df.show()

# Load
partition_date = datetime.today().strftime("%Y%m%d")

summary_df.repartition(1).write.csv(os.path.join(project_dir, f"output/summary_by_province_{partition_date}"),
                                    mode='overwrite',
                                    header=True)

db_conn: str = "jdbc:postgresql://localhost:5432/mydb"
table_name: str = "summary_by_province"
properties: dict = {
    "user": "postgres",
    "password": "abc123",
    "driver": "org.postgresql.Driver"
}

try:
    summary_df.repartition(1).write.mode("overwrite").jdbc(db_conn, table=table_name, mode='overwrite',
                                                           properties=properties)
    print('Data has been loaded to PostgresSQL!')
except Exception as exp:
    print(f'ERROR : Failed to load data to PostgresSQL : {exp}')
    raise

