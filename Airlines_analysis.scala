// # Departure delay prediction
// 
// ###Objective :
//  Using the Airline dataset [http://stat-computing.org/dataexpo/2009/the-data.html](http://stat-computing.org/dataexpo/2009/the-data.html) for 2007 and 2008.
//  Predict 2008 **Departure** delays using 2007 data as a training set.
// 
//  Data is enriched with weather information from NOAA [http://www.noaa.gov/](http://www.noaa.gov/)
//  
//  Predictions limited to Chicago O'Hare (ORD ; weather station : USW00094846)


// #### push files to HDFS
import sys.process._
"hadoop fs -rm -r -f /tmp/mlamairesse/flights" !
"hadoop fs -rm -r -f /tmp/mlamairesse/weather" !

"hdfs dfs -mkdir -p /tmp/mlamairesse/flights" !
"hdfs dfs -mkdir -p /tmp/mlamairesse/weather" !

"hdfs dfs -put data/flights_ORD_2007.csv /tmp/mlamairesse/flights" !
"hdfs dfs -put data/flights_ORD_2008.csv /tmp/mlamairesse/flights" !
"hdfs dfs -put data/weather2007_USW00094846.csv /tmp/mlamairesse/weather" !

//check 
"hdfs dfs -ls /tmp/mlamairesse/flights" !

val flights_data = "/tmp/mlamairesse/flights"
val weather_data = "/tmp/mlamairesse/weather"

// ####create context
import org.apache.spark.sql.SparkSession

val spark = SparkSession.
  builder().
  appName("Spark SQL basic example").
//  config("spark.some.config.option", "some-value").
  getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._


// ### Pre Processing 
// #### Flights data
//  load and clean the flights dataset

//create UDF to facilitate data prep
import java.text.SimpleDateFormat;
//import java.util.Date;
import org.apache.spark.sql.functions.udf

//data location
val Flight_Data = "../data/flights"
val Weather_Data = "../data/airlines"



spark.udf.register("toDateString", (year: String, month: String, day: String ) => {
    
    val Y = "%04d".format(year.toInt)
    val M = "%02d".format(month.toInt)
    val D = "%02d".format(day.toInt)
    
    Y+M+D
    
} )


spark.udf.register("get_hour_base10", (time: String) => {
    var temp_time = time
    
    if(time.length == 3) {
        temp_time = 0+time
    }
    
    val hour = temp_time.dropRight(2).toDouble
    val minute = temp_time.takeRight(2).toDouble
    
    val dec_time = hour + ( minute / 60 )

     dec_time
} )


spark.udf.register("delay_label", (Delay:  String) => {
    var label = 0
    
    if (Delay != null) {
        var delayInt = Delay.toInt
        if (delayInt > 15) {label = 1} 
    }
    
    label 
} )

// COMMAND ----------

//load flight data
import org.apache.spark.sql._
import org.apache.spark.sql.types._

//filter data on flights departing from ORD (chicago)
val flightsRaw_df = spark.read.format("csv").
  option("delimiter",",").
  option("quote","").
  option("header", "true").
  option("nullValue", "NA").
//  .option("dateFormat", "")
  load(flights_data).
  filter($"Origin" === "ORD" ).cache()

flightsRaw_df.createOrReplaceTempView("flights_raw")

display(flightsRaw_df)

// COMMAND ----------

//Check for missing values 

flightsRaw_df.columns.foreach(col => {
    val count = flightsRaw_df.filter(flightsRaw_df(col).isNull).count()
    println(col +": "+ count)
})

// COMMAND ----------

//cleanup and enrich flights table with label
val sql_statement = 
"""SELECT cast(Year as int) Year, 
        cast(Month as int) Month, 
        cast(DayofMonth as int) Day, 
        cast(DayOfWeek as int) DayOfWeek, 
        toDateString(Year, Month, DayofMonth) DateString, 
        get_hour_base10(CRSDepTime) decimal_DepTime, 
        UniqueCarrier,  
        cast(FlightNum as int) FlightNum,  
        IFNULL(TailNum, 'N/A') AS TailNum, 
        Origin ,  
        Dest , 
        cast(Distance as int) Distance, 
        IFNULL(cast(DepDelay as int ), 0) delay, 
        delay_label(DepDelay) label  
        FROM flights_raw """

val flights_df = spark.sql(sql_statement).cache()

flights_df.createOrReplaceTempView("flights")

//display(flights_df)


// #### Weather data
//  Load, filter and pivot data

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val WeatherSchema = StructType( Array(
    StructField("station", StringType, true),
    StructField("DateString", StringType, true),
    StructField("metric", StringType, true),
    StructField("value", IntegerType, true),
    StructField("t1", StringType, true),
    StructField("t2", StringType, true),
    StructField("t3", StringType, true),
    StructField("time", StringType, true)
))

//filter data for ORD => Weather station num USW00094846
val weathterRaw_df = sqlContext.read.format("csv")
  .option("delimiter",",")
  .option("quote","")
  .option("header", "false")
  .schema(WeatherSchema)
  .load("/FileStore/airlines_dataset/weather/")
  .filter($"station" === "USW00094846" )

weathterRaw_df.createOrReplaceTempView("weather_raw")

display(weathterRaw_df)


/*pivot metrics on date
### Metrics ###
PRCP = Precipitation (tenths of mm)
SNOW = Snowfall (mm)
SNWD = Snow depth (mm)
TMAX = Maximum temperature (tenths of degrees C)
TMIN = Minimum temperature (tenths of degrees C)
*/

val weather_pivot_df = weathterRaw_df
    .groupBy("DateString")
    .pivot("metric", Seq("PRCP","SNOW","SNWD","TMAX","TMIN"))
    .avg("value")
    .sort("DateString")
    .cache()
    
weather_pivot_df.createOrReplaceTempView("weather_pivot")

display(weather_pivot_df)


// #### Putting it all togheter (flights and data)

// join the dataframes 

val joinedDF = flights_df.join(weather_pivot_df, flights_df("DateString") === weather_pivot_df("DateString"))
    .drop(weather_pivot_df("DateString"))
    .cache()

joinedDF.createOrReplaceTempView("joined")

//display(joinedDF)


//cleanup datasets from mem : 
// temp tables
spark.catalog.uncacheTable("flights_raw")
spark.catalog.uncacheTable("flights")
spark.catalog.uncacheTable("weather_raw")
spark.catalog.uncacheTable("weather_pivot")

// cached datasets
flightsRaw_df.unpersist()
flights_df.unpersist()
weather_pivot_df.unpersist()



//check for Nulls
joinedDF.columns.foreach(col => {
    val count = joinedDF.filter(joinedDF(col).isNull).count()
    println(col +": "+ count)
})

// #### Feature creation - Categorical feature encoding


import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline

// UniqueCarrier encoding
val carrierIndexer = new StringIndexer()
  .setInputCol("UniqueCarrier")
  .setOutputCol("indexedCarrier")
  .setHandleInvalid("keep")
  .fit(joinedDF)

  
//val debugDFCarrierIndexed = carrierIndexer.transform(joinedDF)
//debugDFCarrierIndexed.select("UniqueCarrier","indexedCarrier").show(3)
 
 
// TailNum encoding
val TailNumIndexer = new StringIndexer()
  .setInputCol("TailNum")
  .setOutputCol("indexedTailNum")
  .setHandleInvalid("keep")
  .fit(joinedDF)
  
//val debugDFTailNumIndexed = TailNumIndexer.transform(joinedDF)
//debugDFTailNumIndexed.select("TailNum","indexedTailNum").show(3)
 
 
// Dest encoding
val DestIndexer = new StringIndexer()
  .setInputCol("Dest")
  .setOutputCol("indexedDest")
  .setHandleInvalid("keep")
  .fit(joinedDF)
  
//val debugDFDestIndexed = DestIndexer.transform(joinedDF)
//debugDFDestIndexed.select("Dest","indexedDest").show(3)

val encodingPipeline = new Pipeline()
  .setStages(Array( carrierIndexer , TailNumIndexer ,DestIndexer))
  
val features_model = encodingPipeline.fit(joinedDF)
val featuresDF = features_model.transform(joinedDF)
    .persist()
featuresDF.createOrReplaceTempView("features")

display(featuresDF)

// ### Model Definition

// ##### Split Train / Test
// use 2007 data to train and predict 2008
// Split the data and keep only the requiered features

val trainDF = spark.sql("""
SELECT Month, Day, DayOfWeek, decimal_DepTime, indexedCarrier , FlightNum , indexedTailNum, indexedDest, Distance, delay , TMIN, TMAX, PRCP, SNOW, PRCP, label 
FROM features 
WHERE Year = 2007""" ).persist()

val testDF = sqlContext.sql("""
SELECT Month, Day, DayOfWeek, decimal_DepTime, indexedCarrier , FlightNum , indexedTailNum, indexedDest, Distance, delay , TMIN, TMAX, PRCP, SNOW, PRCP, label 
FROM features 
WHERE Year = 2008""" ).persist()

featuresDF.unpersist()

// ####Feature assembly

import org.apache.spark.ml.feature.VectorAssembler

//create the features array 
val assembler = new VectorAssembler()
  .setInputCols(Array(
    "Month",
    "Day" ,
    "DayOfWeek",
    "decimal_DepTime",
    "indexedCarrier",
    "FlightNum",
    "indexedTailNum",
    "indexedDest",
    "Distance",
    "TMIN",
    "PRCP",
    "SNOW",
    "PRCP"
    ))
  .setOutputCol("features_array")

//val debugFeaturesDF =   assembler.transform(trainDF)
//debugFeaturesDF.show(3)
  
//create the label array
val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("labelIndexed")
  .fit(trainDF)
  
//val debugDFlabelIndexer =  labelIndexer.transform(trainDF)
//debugDFlabelIndexer.select("Month", "Day", "DayOfWeek", "label" , "labelIndexed").show(3)

//### Setup Model Pipeline


import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline

//Select Model 
val model = new LogisticRegression()
  .setMaxIter(40)
  .setFeaturesCol("features_array")
  .setLabelCol("labelIndexed")

//Create evaluation pipeline
val pipeline = new Pipeline()
  .setStages(Array(assembler, labelIndexer,  model))

// COMMAND ----------

// MAGIC %md Train and Test

// COMMAND ----------

//Train the Model
val linRegModel = pipeline.fit(trainDF)

//Evaluate predictions on test data
val predictions = linRegModel.transform(testDF)
predictions.createOrReplaceTempView("testPredictions")

display(predictions)

// COMMAND ----------

def eval_metrics(labelsAndPreds: DataFrame ) : Tuple2[Array[Double], Array[Double]] = {
    val tp = labelsAndPreds.filter( "label = 1 and prediction = 1" ).count.toDouble
    val tn = labelsAndPreds.filter( "label = 0 and prediction = 0" ).count.toDouble
    val fp = labelsAndPreds.filter( "label = 0 and prediction = 1" ).count.toDouble
    val fn = labelsAndPreds.filter( "label = 1 and prediction = 0" ).count.toDouble

    val precision   = tp / (tp+fp)
    val recall      = tp / (tp+fn)
    val F_1         = 2*((precision*recall) / (precision+recall))
    val accuracy    = (tp+tn) / (tp+tn+fp+fn)
    
    new Tuple2(Array(tp, tn, fp, fn), Array(precision, recall, F_1, accuracy))
}
//val metrics_test_0 = sqlContext.sql("SELECT labelIndexed label, prediction FROM testPredictions where prediction = 0")
//val metrics_test_1 = sqlContext.sql("SELECT labelIndexed label, prediction FROM testPredictions where prediction = 1")

val metrics = eval_metrics(sqlContext.sql("SELECT labelIndexed label, prediction FROM testPredictions"))

val precision  = metrics._2(0) //- ( metrics._2(0) % .0001 )
val recall     = metrics._2(1) //- ( metrics._2(1) % .0001 )
val F1         = metrics._2(2) //- ( metrics._2(2) % .0001 )
val accuracy   = metrics._2(3) //- ( metrics._2(3) % .0001 )

println("Precision: " + precision )
println("Recall: " + recall)
println("F1: " + F1)
println("Accuracy: " + accuracy)

// COMMAND ----------

// improvements 
// add other models and do cross validation
// use spark ml evaluation metrics
