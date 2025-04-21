from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("Social Media User Analysis") \
        .getOrCreate()

def read_parquet_file(spark):
    return spark.read.parquet("users.parquet")

# soru 1 – genel yas ve kullanim suresi ortalamasi
def question_1_spark_sql(df):
    df.createOrReplaceTempView("users")
    spark.sql("""
        SELECT gender,
               ROUND(AVG(DATEDIFF(current_date(), TO_DATE(dob)) / 365), 1) AS avg_age,
               ROUND(AVG(DATEDIFF(current_date(), TO_DATE(registered)) / 365), 1) AS avg_usage_years
        FROM users
        GROUP BY gender
    """).show()

def question_1_dataframe_api(df):
    df.withColumn("age", round(datediff(current_date(), to_date("dob")) / 365, 1)) \
      .withColumn("usage_years", round(datediff(current_date(), to_date("registered")) / 365, 1)) \
      .groupBy("gender") \
      .agg(
          round(avg("age"), 1).alias("avg_age"),
          round(avg("usage_years"), 1).alias("avg_usage_years")
      ).show()

# soru 2 – ulkelere gore cinsiyet bazinda yas ve kullanim suresi ortalamasi
def question_2_spark_sql(df):
    df.createOrReplaceTempView("users")
    spark.sql("""
        SELECT country, gender,
               ROUND(AVG(DATEDIFF(current_date(), TO_DATE(dob)) / 365), 1) AS avg_age,
               ROUND(AVG(DATEDIFF(current_date(), TO_DATE(registered)) / 365), 1) AS avg_usage_years
        FROM users
        GROUP BY country, gender
        ORDER BY country, gender
    """).show()

def question_2_dataframe_api(df):
    df.withColumn("age", round(datediff(current_date(), to_date("dob")) / 365, 1)) \
      .withColumn("usage_years", round(datediff(current_date(), to_date("registered")) / 365, 1)) \
      .groupBy("country", "gender") \
      .agg(
          round(avg("age"), 1).alias("avg_age"),
          round(avg("usage_years"), 1).alias("avg_usage_years")
      ).orderBy("country", "gender") \
      .show()

# soru 3 – ulkelere gore en yasli 3 erkek ve 3 kadin user
def question_3_spark_sql(df):
    df.createOrReplaceTempView("users")
    spark.sql("""
        WITH user_age AS (
            SELECT *, DATEDIFF(current_date(), TO_DATE(dob)) / 365 AS age
            FROM users
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY country, gender ORDER BY age DESC) AS rn
            FROM user_age
        )
        SELECT country, gender, first_name, last_name, ROUND(age, 1) AS age
        FROM ranked
        WHERE rn <= 3
        ORDER BY country, gender, age DESC
    """).show()

def question_3_dataframe_api(df):
    df_with_age = df.withColumn("age", datediff(current_date(), to_date("dob")) / 365)

    window_spec = Window.partitionBy("country", "gender").orderBy(col("age").desc())

    df_with_age.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3) \
        .select("country", "gender", "first_name", "last_name", round("age", 1).alias("age")) \
        .orderBy("country", "gender", "age", ascending=[True, True, False]) \
        .show()

if __name__ == "__main__":
    spark = create_spark_session()
    df = read_parquet_file(spark)

    print("Soru 1: genel yas ve kullanim suresi ortalamasi (Spark SQL)")
    question_1_spark_sql(df)

    print("Soru 1: genel yas ve kullanim suresi ortalamasi (DataFrame API)")
    question_1_dataframe_api(df)

    print("Soru 2: ulkeye gore yas ve kullanim suresi ortalamasi (Spark SQL)")
    question_2_spark_sql(df)

    print("Soru 2: ulkeye gore yas ve kullanim suresi ortalamasi (DataFrame API)")
    question_2_dataframe_api(df)

    print("Soru 3: ulkeye gore en yasli 3 erkek ve 3 kadin user (Spark SQL)")
    question_3_spark_sql(df)

    print("Soru 3: ulkeye gore en yasli 3 erkek ve 3 kadin user (DataFrame API)")
    question_3_dataframe_api(df)
