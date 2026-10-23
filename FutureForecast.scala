
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import java.time.LocalDate
import java.time.temporal.ChronoUnit

object FutureForecast {

  def main(args: Array[String]): Unit = {

    val horizon =
      if (args.nonEmpty) args(0).toInt else 7

    require(
      horizon >= 1 && horizon <= 90,
      "Forecast horizon must be between 1 and 90 days."
    )

    val base =
      "C:/Users/alfre/INT315-Sales-Analysis/output/"

    val spark = SparkSession.builder()
      .appName("RetailPulse Future Forecast")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val model =
      RandomForestRegressionModel.load(
        base + "random_forest_model"
      )

    val history = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(base + "daily_sales_history")
      .select(
        col("SalesDate").cast("string"),
        col("ActualRevenue").cast("double")
      )
      .orderBy("SalesDate")
      .collect()
      .map(row => (
        LocalDate.parse(row.getString(0)),
        row.getAs[Double](1)
      ))
      .toBuffer

    require(
      history.size >= 7,
      "At least seven days of historical sales are required."
    )

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "DayIndex",
        "DayOfWeek",
        "Month",
        "DayOfMonth",
        "Lag1",
        "Lag7",
        "RollingMean7"
      ))
      .setOutputCol("features")

    val firstDate = history.head._1
    val lastDate = history.last._1

    val forecastRows = (1 to horizon).map { step =>

      val date = lastDate.plusDays(step)

      val lag1 = history.last._2
      val lag7 = history(history.size - 7)._2

      val rollingMean7 =
        history.takeRight(7).map(_._2).sum / 7.0

      val dayIndex =
        ChronoUnit.DAYS.between(firstDate, date).toDouble

      val dayOfWeek = date.getDayOfWeek.getValue.toDouble
      val month = date.getMonthValue.toDouble
      val dayOfMonth = date.getDayOfMonth.toDouble

      val input = Seq((
        dayIndex,
        dayOfWeek,
        month,
        dayOfMonth,
        lag1,
        lag7,
        rollingMean7
      )).toDF(
        "DayIndex",
        "DayOfWeek",
        "Month",
        "DayOfMonth",
        "Lag1",
        "Lag7",
        "RollingMean7"
      )

      val features = assembler.transform(input)
      val prediction =
        math.max(0.0, model.transform(features)
          .first().getAs[Double]("Prediction"))

      history += ((date, prediction))

      (date.toString, prediction)
    }

    val forecast = forecastRows.toDF(
      "ForecastDate",
      "PredictedRevenue"
    )

    forecast
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(base + "future_forecast")

    println(s"Forecast horizon: $horizon days")
    println(s"Last historical date: $lastDate")
    println("Future sales predictions:")
    forecast.show(horizon, truncate = false)
    println("Forecast saved to output/future_forecast")

    spark.stop()
  }
}
