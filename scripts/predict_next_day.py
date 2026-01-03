from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, lag, lit, to_date, round, current_date
from pyspark.sql.window import Window
import pandas as pd
import datetime
import os

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("NIFTY_Prediction") \
    .getOrCreate()

# Load historical data
df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("data/NIFTY 50-1-year.csv")
df = df.toDF(*[c.strip() for c in df.columns])

df = df.drop("Shares Traded", "Turnover (₹ Cr)")

df = df.withColumn("Prev Close", lag(col("Close" )).over(Window.orderBy("Trading Date")))
df = df.withColumn("Change1", col("Close") - col("Prev Close"))
df = df.withColumn("Change", round(col("Change1"), 2))
df = df.withColumn("Perc Change", round((col("Change1") / col("Prev Close")) * 100, 2))
df = df.withColumn("Moving Average1", (lag("Close", 1).over(Window.orderBy("Trading Date")) + lag("Close", 2).over(Window.orderBy("Trading Date")) + lag("Close", 2).over(Window.orderBy("Trading Date"))) / 3)
df = df.withColumn("Moving Average", round(col("Moving Average1"), 2))
df = df.select("Trading Date", "Open" , "High" ,"Low", "Close", "Prev Close", "Change","Perc Change","Moving Average")
df = df.na.drop()

# Use last available day as proxy features
latest = df.orderBy("Trading Date", ascending=False).limit(1)

assembler = VectorAssembler(
    inputCols=["Prev Close", "Change", "Moving Average"],
    outputCol="features"
)

latest_features = assembler.transform(latest)

# Load model
model = LinearRegressionModel.load("model/linear_nifty_model")


prediction_df = model.transform(latest_features).select(current_date().alias("Prediction Date"), round(col("prediction"),2).alias("Predicted Close"))

# prediction = model.transform(latest_features).select("prediction").collect()[0][0]

# Save prediction
# output = pd.DataFrame([{
#     "prediction_for_next_day_close": round(float(prediction), 2)
# }])

today = datetime.datetime.now().strftime("%Y%m%d")
output_path = f"data/predictions/nifty_prediction_{today}"

# os.makedirs("data/predictions", exist_ok=True)
prediction_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

spark.stop()
print("Prediction generated and saved")
