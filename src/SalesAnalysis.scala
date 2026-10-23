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

    spark.sparkContext.setLogLevel("WARN")
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
    println("\n==========================================")
    println(" SPARK MLlib: DAILY SALES FORECASTING")
    println("==========================================")

    // Aggregate transaction-level data into daily sales
    val dailySales = salesWithMonth
      .filter(col("ParsedInvoiceDate").isNotNull)
      .groupBy(to_date(col("ParsedInvoiceDate")).alias("SalesDate"))
      .agg(
        round(sum("Revenue"), 2).alias("ActualRevenue")
      )
      .filter(col("SalesDate").isNotNull)
      .orderBy("SalesDate")
      .cache()

    val dailyCount = dailySales.count()

    println(s"Daily observations: $dailyCount")

    // Create calendar features
    val firstDate = dailySales
      .agg(min("SalesDate"))
      .first()
      .getDate(0)

    val featuredSales = dailySales
      .withColumn(
        "DayIndex",
        datediff(col("SalesDate"), lit(firstDate)).cast("double")
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
      .cache()

    // Assemble model features
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "DayIndex",
        "DayOfWeek",
        "Month",
        "DayOfMonth"
      ))
      .setOutputCol("features")

    val modelData = assembler.transform(featuredSales)
      .select(
        "SalesDate",
        "features",
        "ActualRevenue"
      )
      .cache()

    // Chronological split: train on earlier dates,
    // test on the most recent approximately 20% of dates.
    val maxDate = dailySales
      .agg(max("SalesDate"))
      .first()
      .getDate(0)

    val testStartDate = java.sql.Date.valueOf(
      maxDate.toLocalDate.minusDays(
        math.max(1L, math.ceil(dailyCount * 0.20).toLong)
      )
    )

    val trainingData = modelData
      .filter(col("SalesDate") < lit(testStartDate))

    val testData = modelData
      .filter(col("SalesDate") >= lit(testStartDate))

    println(s"Training observations: ${trainingData.count()}")
    println(s"Testing observations: ${testData.count()}")
    println(s"Test period begins: $testStartDate")

    // Train Random Forest regression model
    val rf = new RandomForestRegressor()
      .setLabelCol("ActualRevenue")
      .setFeaturesCol("features")
      .setPredictionCol("PredictedRevenue")
      .setNumTrees(50)
      .setMaxDepth(5)
      .setSeed(42)

    val rfModel = rf.fit(trainingData)

    // Predict on held-out future dates
    val predictions = rfModel.transform(testData)
      .select(
        "SalesDate",
        "ActualRevenue",
        "PredictedRevenue"
      )
      .orderBy("SalesDate")
      .cache()

    println("\n--- SAMPLE DAILY PREDICTIONS ---")
    predictions.show(10, truncate = false)

    // Evaluate regression performance
    val rmseEvaluator = new RegressionEvaluator()
      .setLabelCol("ActualRevenue")
      .setPredictionCol("PredictedRevenue")
      .setMetricName("rmse")

    val maeEvaluator = new RegressionEvaluator()
      .setLabelCol("ActualRevenue")
      .setPredictionCol("PredictedRevenue")
      .setMetricName("mae")

    val r2Evaluator = new RegressionEvaluator()
      .setLabelCol("ActualRevenue")
      .setPredictionCol("PredictedRevenue")
      .setMetricName("r2")

    val rmse = rmseEvaluator.evaluate(predictions)
    val mae = maeEvaluator.evaluate(predictions)
    val r2 = r2Evaluator.evaluate(predictions)

    println("\n--- MODEL EVALUATION ---")
    println(f"RMSE: $rmse%.2f")
    println(f"MAE:  $mae%.2f")
    println(f"R2:   $r2%.4f")

    // Export actual versus predicted results
    val mlOutput =
      "C:/Users/alfre/INT315-Sales-Analysis/output/ml_predictions"

    predictions.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(mlOutput)

    // Export model metrics
    val metricsDF = spark.createDataFrame(Seq(
      ("Random Forest Regression", rmse, mae, r2,
       trainingData.count(), testData.count())
    )).toDF(
      "Model",
      "RMSE",
      "MAE",
      "R2",
      "TrainingObservations",
      "TestingObservations"
    )

    metricsDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("C:/Users/alfre/INT315-Sales-Analysis/output/ml_metrics")

    println("\nMLlib predictions and metrics saved successfully.")
    println("--- MLlib ANALYSIS COMPLETE ---")

    predictions.unpersist()
    modelData.unpersist()
    featuredSales.unpersist()
    dailySales.unpersist()



    salesDF.unpersist()
    spark.stop()
  }
}
