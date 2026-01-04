from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pow, sqrt, avg
import datetime

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("NIFTY_Evaluation") \
    .getOrCreate()


today = datetime.datetime.now().strftime("%Y%m%d")
pred_file = f"data/predictions/nifty_prediction_{today}"

pred = spark.read.option("header", True).option("inferSchema", True) \
    .csv(pred_file)

actual = spark.read.option("header", True).option("inferSchema", True) \
    .csv("data/NIFTY 50-data.csv")

pred = pred.toDF(*[c.replace(" ", "_").lower() for c in pred.columns])
actual = actual.toDF(*[c.replace(" ", "_").lower() for c in actual.columns])

joined = pred.join(
    actual,
    pred.prediction_date == actual.trading_date,
    "inner"
)
joined.show(10)
rmse_df = joined.withColumn(
    "RMSE",
    sqrt((pow(col("predicted_close") - col("close"), 2)))
)

rmse_df.write.option("header", True).csv(pred_file)

rmse = rmse_df.select("RMSE").collect()[0][0]

print(f"RMSE for {today}: {rmse}")

spark.stop()
