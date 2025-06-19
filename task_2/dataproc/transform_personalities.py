from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when
from pyspark.sql.types import IntegerType, DecimalType, StringType, BooleanType
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("Parquet ETL with Logging to S3").getOrCreate()

source_path = "s3a://personalities/2025/06/18/personality.csv"
target_path = "s3a://personalities/cleaned/personality_clean.parquet"

try:
    print(f"Чтение данных из: {source_path}")
    df = spark.read.option("inferSchema", "true").csv(source_path)

    df = df.withColumn("id", col("_c0").cast(IntegerType())) \
           .withColumn("time_spent_alone", col("_c1").cast(DecimalType(10, 2))) \
           .withColumn("stage_fear", when(col("_c2") == "Yes", True).when(col("_c2") == "No", False).otherwise(None))\
           .withColumn("social_event_attendance", to_date(col("_c3").cast(DecimalType(10, 2)))) \
           .withColumn("going_outside", col("_c4").cast(DecimalType(10, 2))) \
           .withColumn("drained_after_socializing", when(col("_c5") == "Yes", True).when(col("_c5") == "No", False).otherwise(None)) \
           .withColumn("friends_circle_size", col("_c6").cast(DecimalType(10, 2))) \
           .withColumn("post_frequency", col("_c7").cast(DecimalType(10, 2))) \
           .withColumn("personality", to_date(col("_c8").cast(StringType())))

    df = df.na.drop()
    df.write.mode("overwrite").parquet(target_path)

    print(f"Данные успешно сохранены в {target_path}.")
except Exception as e:
    print("Error:", e)

spark.stop()
