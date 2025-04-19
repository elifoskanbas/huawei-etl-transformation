from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, to_date, datediff, row_number
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("SocialMediaUserAnalysis") \
        .getOrCreate()

def read_parquet_file(spark, file_path="user_data.parquet"):
    df = spark.read.parquet(file_path)
    
    # Tarihleri dönüştür ve yaş ve kullanım süresini hesapla
    df = df.withColumn("dob_date", to_date(col("dob.date"))) \
           .withColumn("registered_date", to_date(col("registered.date"))) \
           .withColumn("current_date", to_date(lit("2025-04-18"))) \
           .withColumn("age", (datediff(col("current_date"), col("dob_date")) / 365.25).cast("int")) \
           .withColumn("membership_duration", (datediff(col("current_date"), col("registered_date")) / 365.25).cast("int"))
    
    df.createOrReplaceTempView("users")
    return df

# Soru 1
def question_1_spark_sql():
    print("Soru 1 - Spark SQL:")
    spark.sql("""
        SELECT gender,
               ROUND(AVG(age), 2) AS avg_age,
               ROUND(AVG(membership_duration), 2) AS avg_membership_duration
        FROM users
        GROUP BY gender
    """).show()

def question_1_dataframe_api(df):
    print("Soru 1 - DataFrame API:")
    df.groupBy("gender") \
      .agg(
          round(avg("age"), 2).alias("avg_age"),
          round(avg("membership_duration"), 2).alias("avg_membership_duration")
      ).show()

# Soru 2
def question_2_spark_sql():
    print("Soru 2 - Spark SQL:")
    spark.sql("""
        SELECT location.country AS country,
               gender,
               ROUND(AVG(age), 2) AS avg_age,
               ROUND(AVG(membership_duration), 2) AS avg_membership_duration
        FROM users
        GROUP BY location.country, gender
    """).show()

def question_2_dataframe_api(df):
    print("Soru 2 - DataFrame API:")
    df.groupBy("location.country", "gender") \
      .agg(
          round(avg("age"), 2).alias("avg_age"),
          round(avg("membership_duration"), 2).alias("avg_membership_duration")
      ).show()

# Soru 3
def question_3_spark_sql():
    print("Soru 3 - Spark SQL:")
    spark.sql("""
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY gender, location.country ORDER BY age DESC) AS rn
            FROM users
        ) t
        WHERE rn <= 3
        ORDER BY location.country, gender, rn
    """).show(truncate=False)

def question_3_dataframe_api(df):
    print("Soru 3 - DataFrame API:")
    window_spec = Window.partitionBy("gender", "location.country").orderBy(col("age").desc())

    df.withColumn("rn", row_number().over(window_spec)) \
      .filter(col("rn") <= 3) \
      .orderBy("location.country", "gender", "rn") \
      .select("name.first", "name.last", "gender", "location.country", "age", "rn") \
      .show(truncate=False)

if __name__ == "__main__":
    spark = create_spark_session()
    df = read_parquet_file(spark)

    # Soru 1
    question_1_spark_sql()
    question_1_dataframe_api(df)

    # Soru 2
    question_2_spark_sql()
    question_2_dataframe_api(df)

    # Soru 3
    question_3_spark_sql()
    question_3_dataframe_api(df)

    spark.stop()