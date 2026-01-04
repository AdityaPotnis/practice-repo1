from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lit, to_date, round
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import os

# ---------------------------------------------------
# Config
# ---------------------------------------------------
TRAIN_SESSIONS = 210
MODEL_PATH = "model/linear_nifty_model"


# ---------------------------------------------------
# Start Spark Session in local mode
# ---------------------------------------------------
# os.environ["HADOOP_HOME"] = "C:\spark-3.5.1-bin-hadoop3"
# os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
spark = SparkSession.builder \
    .appName("Nifty50_Model_Training") \
    .master("local[*]") \
    .getOrCreate()
    # .config("spark.hadoop.io.native.lib.available", "false")

# ---------------------------------------------------
# Load historical OHLC data (append-only)
# ---------------------------------------------------
# use relative path so the script works when run from the repo root (e.g. in CI or GitHub Actions)
csv_file = r'data/NIFTY 50-data.csv'
# read with header and infer schema for proper types
df = spark.read.option("header", True).option("inferSchema", True).csv(csv_file)
df = df.toDF(*[c.strip() for c in df.columns])
# df = df.withColumn("Trading Date",to_date(df["Trading Date"],"yyyy-MM-dd"))

df = df.drop("Shares Traded", "Turnover (₹ Cr)")

# window_spec = Window.orderBy("Trading Date")
# df = df.withColumn("Trading Date", to_date(col("Trading Date"), "yyyy-MM-dd"))
df = df.withColumn("Prev Close", lag(col("Close" )).over(Window.orderBy("Trading Date")))
df = df.withColumn("Change1", col("Close") - col("Prev Close"))
df = df.withColumn("Change", round(col("Change1"), 2))
df = df.withColumn("Perc Change", round((col("Change1") / col("Prev Close")) * 100, 2))
# df = df.withColumn("Moving Average1", (lag("Close", 1).over(Window.orderBy("Trading Date")) + lag("Close", 2).over(Window.orderBy("Trading Date")) + lag("Close", 2).over(Window.orderBy("Trading Date"))) / 3)
# df = df.withColumn("Moving Average", round(col("Moving Average1"), 2))
df = df.select("Trading Date", "Open" , "High" ,"Low", "Close", "Prev Close", "Change","Perc Change")
df = df.na.drop()  # drop rows with missing lag values


# ---------------------------------------------------
# Select last N trading sessions
# ---------------------------------------------------
# train_cutoff = to_date(lit('2025-11-30'), 'yyyy-MM-dd')

# train_df = df.filter(col("Trading Date") <= train_cutoff)
# test_df = df.filter(col("Trading Date") > train_cutoff)

train_df = df.orderBy(col("Trading Date").desc()).limit(TRAIN_SESSIONS)

# ---------------------------------------------------
# Assemble features
# ---------------------------------------------------
assembler = VectorAssembler(
    inputCols=["Open" , "High" ,"Low", "Close", "Prev Close", "Change"],
    outputCol="features"
)

train_data = assembler.transform(train_df).select("features", col("Close").alias("label"))
# test_data = assembler.transform(test_df).select("Trading Date","features", col("Close").alias("label"))


# ---------------------------------------------------
# Train model
# ---------------------------------------------------
lr = LinearRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_data)

# # === 6. Evaluate on test data ===
# predictions = lr_model.transform(test_data)

# evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
# rmse = evaluator.evaluate(predictions)
# print(f"Root Mean Squared Error (RMSE) on test data = {rmse:.4f}")
# print("\nSample predictions vs actual:")
# predictions.select("Trading Date", "label", "prediction").show(truncate=False)

# ---------------------------------------------------
# Save model (Spark-managed directory)
# ---------------------------------------------------
# model_dir = "model/linear_nifty_model"
# if os.path.exists(model_dir):
#     import shutil
#     shutil.rmtree(model_dir)  # overwrite existing


lr_model.write().overwrite().save(MODEL_PATH)

print("Model trained and saved")

spark.stop()
