import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.regression.LinearRegression

object SalesAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("INT315 Sales Data Analysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    println("\n==========================================")
    println(" INT315 - SALES DATA ANALYSIS USING SPARK")
    println("==========================================\n")

    val filePath =
      "C:/Users/alfre/INT315-Sales-Analysis/data/OnlineRetail.csv"

    // STEP 1: Load the original dataset
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(filePath)

    println("--- ORIGINAL DATASET ---")
    println(s"Original record count: ${rawDF.count()}")
    rawDF.printSchema()

    // STEP 2: Inspect missing values
    println("\n--- MISSING VALUE COUNTS ---")

    val missingCounts = rawDF.columns.map { column =>
      sum(
        when(
          col(column).isNull ||
          trim(col(column).cast("string")) === "",
          1
        ).otherwise(0)
      ).alias(column)
    }

    rawDF.select(missingCounts: _*).show(truncate = false)

    // STEP 3: Clean invalid sales records
    val cleanedDF = rawDF
      .withColumn("InvoiceNo", trim(col("InvoiceNo").cast("string")))
      .withColumn("Description", trim(col("Description")))
      .withColumn("Quantity", col("Quantity").cast("double"))
      .withColumn("UnitPrice", col("UnitPrice").cast("double"))
      .filter(col("InvoiceNo").isNotNull)
      .filter(col("InvoiceNo") =!= "")
      .filter(!upper(col("InvoiceNo")).startsWith("C"))
      .filter(col("Description").isNotNull)
      .filter(col("Description") =!= "")
      .filter(col("Quantity").isNotNull && col("Quantity") > 0)
      .filter(col("UnitPrice").isNotNull && col("UnitPrice") > 0)

    // STEP 4: Calculate line-item sales value
    val salesDF = cleanedDF
      .withColumn(
        "Revenue",
        round(col("Quantity") * col("UnitPrice"), 2)
      )
      .cache()

    val originalCount = rawDF.count()
    val cleanedCount = salesDF.count()
    val removedCount = originalCount - cleanedCount

    println("\n--- DATA CLEANING SUMMARY ---")
    println(s"Original records: $originalCount")
    println(s"Valid sales records: $cleanedCount")
    println(s"Records excluded: $removedCount")

    println("\n--- CLEANED DATA SAMPLE ---")
    salesDF.select(
      "InvoiceNo",
      "StockCode",
      "Description",
      "Quantity",
      "UnitPrice",
      "Revenue",
      "Country"
    ).show(10, truncate = false)

    // STEP 5: Calculate overall sales statistics
    println("\n--- OVERALL SALES SUMMARY ---")

    val summaryDF = salesDF.agg(
      count("*").alias("Valid_Transactions"),
      sum("Quantity").alias("Total_Units_Sold"),
      round(sum("Revenue"), 2).alias("Total_Sales_Value"),
      round(avg("Revenue"), 2).alias("Average_Line_Value")
    )

    summaryDF.show(truncate = false)

    summaryDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("C:/Users/alfre/INT315-Sales-Analysis/output/sql_summary")

    // STEP 6: Save cleaned data for later analysis
    salesDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv("C:/Users/alfre/INT315-Sales-Analysis/output/cleaned_sales")

      
    // STEP 7: RDD OPERATIONS
    println("\n==========================================")
    println(" RDD ANALYSIS")
    println("==========================================")

    // Convert the cleaned DataFrame into an RDD of Rows
    val salesRDD = salesDF.rdd

    println("\n--- RDD COUNT ---")
    println(s"Total records in RDD: ${salesRDD.count()}")

    // FILTER: Select sales records with revenue greater than 100
    val highValueRDD = salesRDD.filter { row =>
      row.getAs[Double]("Revenue") > 100.0
    }

    println("\n--- HIGH-VALUE SALES ---")
    println(s"Sales lines with revenue > 100: ${highValueRDD.count()}")

    // MAP: Convert each record to (Country, Revenue)
    val countryRevenueRDD = salesRDD.map { row =>
      (
        row.getAs[String]("Country"),
        row.getAs[Double]("Revenue")
      )
    }

    println("\n--- MAP OPERATION SAMPLE ---")
    countryRevenueRDD.take(5).foreach(println)

    // REDUCEBYKEY: Add revenue for each country
    val revenueByCountryRDD = countryRevenueRDD.reduceByKey(_ + _)

    println("\n--- REDUCEBYKEY: TOP 10 COUNTRIES ---")
    revenueByCountryRDD
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach { case (country, revenue) =>
        println(f"$country%-25s GBP $revenue%,.2f")
      }

    println("\n--- RDD ANALYSIS COMPLETE ---")

    
    // STEP 8: SPARK SQL ANALYSIS
    println("\n==========================================")
    println(" SPARK SQL ANALYSIS")
    println("==========================================")

    import org.apache.spark.sql.types.TimestampType

    // Prepare a month column that works with the inferred date type
    
    // Parse invoice dates robustly
    val invoiceDateString =
      trim(col("InvoiceDate").cast("string"))

    
    val parsedInvoiceDate = coalesce(
      to_timestamp(invoiceDateString, "dd-MM-yyyy HH:mm"),
      to_timestamp(invoiceDateString, "d-M-yyyy H:mm"),
      to_timestamp(invoiceDateString, "yyyy-MM-dd HH:mm:ss")
    )

    val salesWithMonth = salesDF
      .withColumn("ParsedInvoiceDate", parsedInvoiceDate)
      .withColumn(
        "SalesMonth",
        date_format(col("ParsedInvoiceDate"), "yyyy-MM")
      )

    println("\n--- DATE PARSING CHECK ---")
    salesWithMonth.select(
      "InvoiceDate",
      "ParsedInvoiceDate",
      "SalesMonth"
    ).show(10, truncate = false)

    println(
      "Records with unparsed dates: " +
      salesWithMonth.filter(
        col("ParsedInvoiceDate").isNull
      ).count()
    )


    // Register a temporary SQL view
    salesWithMonth.createOrReplaceTempView("retail_sales")

    // QUERY 1: Top 10 products by sales value
    val topProducts = spark.sql("""
      SELECT
        Description,
        ROUND(SUM(Revenue), 2) AS TotalRevenue,
        SUM(Quantity) AS UnitsSold
      FROM retail_sales
      GROUP BY Description
      ORDER BY TotalRevenue DESC
      LIMIT 10
    """)

    println("\n--- TOP 10 PRODUCTS BY SALES VALUE ---")
    topProducts.show(10, truncate = false)

    // QUERY 2: Top 10 countries by sales value
    val countrySales = spark.sql("""
      SELECT
        Country,
        ROUND(SUM(Revenue), 2) AS TotalRevenue,
        COUNT(*) AS SalesLines
      FROM retail_sales
      GROUP BY Country
      ORDER BY TotalRevenue DESC
      LIMIT 10
    """)

    println("\n--- TOP 10 COUNTRIES BY SALES VALUE ---")
    countrySales.show(10, truncate = false)

    // QUERY 3: Monthly sales trends
    val monthlySales = spark.sql("""
      SELECT
        SalesMonth,
        ROUND(SUM(Revenue), 2) AS TotalRevenue,
        COUNT(*) AS SalesLines
      FROM retail_sales
      WHERE SalesMonth IS NOT NULL
      GROUP BY SalesMonth
      ORDER BY SalesMonth
    """)

    println("\n--- MONTHLY SALES TRENDS ---")
    monthlySales.show(20, truncate = false)

    // Save SQL results for the dashboard and report
    val outputPath =
      "C:/Users/alfre/INT315-Sales-Analysis/output/"

    topProducts.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(outputPath + "sql_top_products")

    countrySales.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(outputPath + "sql_country_sales")

    monthlySales.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(outputPath + "sql_monthly_sales")

    println("\nSpark SQL results saved successfully.")
    println("--- SPARK SQL ANALYSIS COMPLETE ---")
    println("\nCleaned dataset saved to output/cleaned_sales")
    println("--- STEP 4 COMPLETE ---")

    
    // STEP 9: GRAPHX ANALYSIS
    println("\n==========================================")
    println(" GRAPHX COUNTRY-PRODUCT NETWORK ANALYSIS")
    println("==========================================")

    // Aggregate sales by country and product
    val countryProductSales = salesDF
      .filter(
        col("Country").isNotNull &&
        col("Description").isNotNull
      )
      .groupBy("Country", "Description")
      .agg(round(sum("Revenue"), 2).alias("SalesValue"))
      .filter(col("SalesValue") > 0)
      .cache()

    // Collect distinct country and product names to assign IDs
    val countryNames = countryProductSales
      .select("Country")
      .distinct()
      .collect()
      .map(_.getString(0))
      .sorted

    val productNames = countryProductSales
      .select("Description")
      .distinct()
      .collect()
      .map(_.getString(0))
      .sorted

    // Create unique vertex IDs
    val countryVertices = countryNames.zipWithIndex.map {
      case (name, index) =>
        (index.toLong + 1L, ("COUNTRY", name))
    }

    val productOffset = countryNames.length.toLong

    val productVertices = productNames.zipWithIndex.map {
      case (name, index) =>
        (productOffset + index.toLong + 1L, ("PRODUCT", name))
    }

    val vertices: RDD[(VertexId, (String, String))] =
      sc.parallelize(countryVertices.toSeq ++ productVertices.toSeq)

    // Create lookup maps for edge construction
    val countryIds = countryNames.zipWithIndex.map {
      case (name, index) => name -> (index.toLong + 1L)
    }.toMap

    val productIds = productNames.zipWithIndex.map {
      case (name, index) =>
        name -> (productOffset + index.toLong + 1L)
    }.toMap

    // Build country-to-product edges with sales value as weight
    val edgeRows = countryProductSales.select(
      "Country",
      "Description",
      "SalesValue"
    ).collect()

    val edges: RDD[Edge[Double]] = sc.parallelize(
      edgeRows.map { row =>
        Edge(
          countryIds(row.getString(0)),
          productIds(row.getString(1)),
          row.getAs[Double]("SalesValue")
        )
      }.toSeq
    )

    // Construct the graph
    val retailGraph = Graph(vertices, edges)

    println("\n--- GRAPH STATISTICS ---")
    println(s"Number of vertices: ${retailGraph.vertices.count()}")
    println(s"Number of edges: ${retailGraph.edges.count()}")

    println("\n--- SAMPLE GRAPH VERTICES ---")
    retailGraph.vertices.take(10).foreach(println)

    println("\n--- TOP 10 COUNTRY-PRODUCT RELATIONSHIPS ---")

    val topGraphEdges = retailGraph.edges
      .sortBy(_.attr, ascending = false)
      .take(10)

    topGraphEdges.foreach { edge =>
      val country = countryIds.find(_._2 == edge.srcId)
        .map(_._1).getOrElse("Unknown")

      val product = productIds.find(_._2 == edge.dstId)
        .map(_._1).getOrElse("Unknown")

      println(
        f"$country%-25s | $product%-40s | GBP ${edge.attr}%,.2f"
      )
    }

    println("\n--- GRAPHX ANALYSIS COMPLETE ---")

    // Save graph relationship results for reporting
    import spark.implicits._

    val graphResults = topGraphEdges.map { edge =>
      (
        countryIds.find(_._2 == edge.srcId).map(_._1).getOrElse("Unknown"),
        productIds.find(_._2 == edge.dstId).map(_._1).getOrElse("Unknown"),
        edge.attr
      )
    }

    graphResults.toSeq.toDF(
      "Country",
      "Product",
      "SalesValue"
    ).coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("C:/Users/alfre/INT315-Sales-Analysis/output/graphx_results")

    countryProductSales.unpersist()

    println("GraphX results saved to output/graphx_results")

    
    // STEP 10: MACHINE LEARNING - DAILY SALES FORECASTING
    
    // STEP 10: ENHANCED MLlib SALES PREDICTION
    println("\n==========================================")
    println(" ENHANCED SPARK MLlib MODEL COMPARISON")
    println("==========================================")

    // 1. Aggregate revenue by calendar day
    val dailySales = salesWithMonth
      .filter(col("ParsedInvoiceDate").isNotNull)
      .groupBy(to_date(col("ParsedInvoiceDate")).alias("SalesDate"))
      .agg(sum("Revenue").alias("ActualRevenue"))
      .filter(col("SalesDate").isNotNull)

    // 2. Create a complete calendar and fill missing dates with zero.
    val dateBounds = dailySales.agg(
      min("SalesDate").alias("MinDate"),
      max("SalesDate").alias("MaxDate")
    ).first()

    val minDate = dateBounds.getDate(0)
    val maxDate = dateBounds.getDate(1)

    val calendar = spark.sql(
      s"""SELECT explode(sequence(
         | DATE '${minDate.toString}',
         | DATE '${maxDate.toString}',
         | INTERVAL 1 DAY
         |)) AS SalesDate""".stripMargin
    )

    val completeDailySales = calendar
      .join(dailySales, Seq("SalesDate"), "left")
      .withColumn(
        "ActualRevenue",
        coalesce(col("ActualRevenue"), lit(0.0))
      )
      .orderBy("SalesDate")

    // Save complete daily sales history for future forecasting
      completeDailySales
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("C:/Users/alfre/INT315-Sales-Analysis/output/daily_sales_history")

      println("Daily sales history saved successfully.")

    // 3. Construct lag and rolling features.
    // Every rolling feature uses only preceding dates.
    val historyWindow = Window
      .orderBy(col("SalesDate").cast("timestamp"))
      .rowsBetween(-7, -1)

    val orderedWindow = Window
      .orderBy(col("SalesDate").cast("timestamp"))

    val featuredSales = completeDailySales
      .withColumn(
        "DayIndex",
        datediff(col("SalesDate"), lit(minDate)).cast("double")
      )
      .withColumn(
        "DayOfWeek",
        dayofweek(col("SalesDate")).cast("double")
      )
      .withColumn(
        "Month",
        month(col("SalesDate")).cast("double")
      )
      .withColumn(
        "DayOfMonth",
        dayofmonth(col("SalesDate")).cast("double")
      )
      .withColumn(
        "Lag1",
        lag(col("ActualRevenue"), 1).over(orderedWindow)
      )
      .withColumn(
        "Lag7",
        lag(col("ActualRevenue"), 7).over(orderedWindow)
      )
      .withColumn(
        "RollingMean7",
        avg(col("ActualRevenue")).over(historyWindow)
      )
      .filter(
        col("Lag1").isNotNull &&
        col("Lag7").isNotNull &&
        col("RollingMean7").isNotNull
      )

    val featureColumns = Array(
      "DayIndex",
      "DayOfWeek",
      "Month",
      "DayOfMonth",
      "Lag1",
      "Lag7",
      "RollingMean7"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")

    val modelData = assembler.transform(featuredSales)
      .select("SalesDate", "features", "ActualRevenue")
      .cache()

    // 4. Chronological train/test split.
    // Test on the most recent 20% of dates.
    val observationCount = modelData.count()
    val testDays = math.max(
      1L,
      math.ceil(observationCount * 0.20).toLong
    )

    val testStartDate = java.sql.Date.valueOf(
      maxDate.toLocalDate.minusDays(testDays - 1)
    )

    val trainingData = modelData
      .filter(col("SalesDate") < lit(testStartDate))
      .cache()

    val testData = modelData
      .filter(col("SalesDate") >= lit(testStartDate))
      .cache()

    println(s"Total model observations: $observationCount")
    println(s"Training observations: ${trainingData.count()}")
    println(s"Testing observations: ${testData.count()}")
    println(s"Test period begins: $testStartDate")

    // 5. Train Random Forest.
    val rf = new RandomForestRegressor()
      .setLabelCol("ActualRevenue")
      .setFeaturesCol("features")
      .setPredictionCol("Prediction")
      .setNumTrees(100)
      .setMaxDepth(7)
      .setSeed(42)

    val rfModel = rf.fit(trainingData)

    // Save the trained Random Forest model for future forecasting
    // Save the trained Random Forest model for future forecasting
    rfModel.write
      .overwrite()
      .save("C:/Users/alfre/INT315-Sales-Analysis/output/random_forest_model")

    println("Random Forest model saved successfully.")

    // 6. Train Linear Regression as a baseline.
    val lr = new LinearRegression()
      .setLabelCol("ActualRevenue")
      .setFeaturesCol("features")
      .setPredictionCol("Prediction")
      .setMaxIter(100)
      .setRegParam(0.1)

    val lrModel = lr.fit(trainingData)

    // 7. Evaluate both models on exactly the same test dates.
    val evaluators = Seq(
      new RegressionEvaluator()
        .setLabelCol("ActualRevenue")
        .setPredictionCol("Prediction")
        .setMetricName("rmse"),
      new RegressionEvaluator()
        .setLabelCol("ActualRevenue")
        .setPredictionCol("Prediction")
        .setMetricName("mae"),
      new RegressionEvaluator()
        .setLabelCol("ActualRevenue")
        .setPredictionCol("Prediction")
        .setMetricName("r2")
    )

    def evaluateModel(
        modelName: String,
        model: org.apache.spark.ml.PipelineStage
    ): (String, Double, Double, Double) = {

      val fittedPredictions = model match {
        case m: org.apache.spark.ml.regression.RandomForestRegressionModel =>
          m.transform(testData)
        case m: org.apache.spark.ml.regression.LinearRegressionModel =>
          m.transform(testData)
      }

      val rmse = evaluators(0).evaluate(fittedPredictions)
      val mae = evaluators(1).evaluate(fittedPredictions)
      val r2 = evaluators(2).evaluate(fittedPredictions)

      println(s"\n--- $modelName ---")
      println(f"RMSE: $rmse%.2f")
      println(f"MAE:  $mae%.2f")
      println(f"R2:   $r2%.4f")

      (modelName, rmse, mae, r2)
    }

    val rfMetrics = evaluateModel("Random Forest", rfModel)
    val lrMetrics = evaluateModel("Linear Regression", lrModel)

    // Choose the model with lower test RMSE.
    val bestModelName =
      if (rfMetrics._2 <= lrMetrics._2) "Random Forest"
      else "Linear Regression"

    val bestModel: org.apache.spark.ml.Model[_] =
      if (bestModelName == "Random Forest") rfModel
      else lrModel

    println(s"\nSelected model by test RMSE: $bestModelName")

    val finalPredictions = bestModel
      .transform(testData)
      .select(
        col("SalesDate"),
        col("ActualRevenue"),
        col("Prediction").alias("PredictedRevenue")
      )
      .orderBy(col("SalesDate"))

    println("\n--- ACTUAL VS PREDICTED SALES ---")
    finalPredictions.show(15, truncate = false)

    // 8. Save selected-model predictions and both model metrics.
    finalPredictions.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(
        "C:/Users/alfre/INT315-Sales-Analysis/output/ml_predictions"
      )

    val metricsRows = Seq(
      ("Random Forest", rfMetrics._2, rfMetrics._3, rfMetrics._4),
      ("Linear Regression", lrMetrics._2, lrMetrics._3, lrMetrics._4)
    )

    metricsRows.toDF("Model", "RMSE", "MAE", "R2")
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(
        "C:/Users/alfre/INT315-Sales-Analysis/output/ml_metrics"
      )

    println("\nEnhanced ML results saved successfully.")

    trainingData.unpersist()
    testData.unpersist()
    modelData.unpersist()




    salesDF.unpersist()
    spark.stop()
  }
}
