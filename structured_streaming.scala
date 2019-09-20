import org.apache.spark.sql.functions.expr
import sqlContext.implicits._
import java.util._
import scala.collection.JavaConverters._
import org.apache.spark.eventhubs._


import java.util.concurrent._
import org.apache.spark.sql.functions._

val connectionString =  ConnectionStringBuilder("Endpoint=sb://eventhub-namespace.servicebus.windows.net/;SharedAccessKeyName=access;SharedAccessKey=xxxxxxxxxxxxxxxxxx").setEventHubName("random_data").build

val customEventhubParameters =
  EventHubsConf(connectionString)
  .setMaxEventsPerTrigger(5)
  
//Method 1

//Extract json schema from a sample batch of data

val incomingStream1 = spark.read.format("eventhubs").options(customEventhubParameters.toMap).load().selectExpr("CAST(body AS STRING) as STRING").as[String].toDF()
					
incomingStream1.write.mode("overwrite").format("text").save("/batch")	

val smallBatchSchema = spark.read.json("/batch/batchName.txt").schema

val inputDf = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

//Apply the extracted schema to the streaming data

val dataDf = inputDf.selectExpr("CAST(body AS STRING) as json")
                    .select( from_json($"json", schema=smallBatchSchema).as("data"))
                    .select("data.*")			
dataDf.printSchema

dataDf.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()


//Method 2 - Read the contents from json

val df = eventhubs.select(($"body").cast("string"))
display(df)

val jsDF = df.select(get_json_object($"body", "$.timestamp").cast("timestamp").alias("time"),
                     get_json_object($"body", "$.temperature").alias("temp"),
                     get_json_object($"body", "$.humidity").alias("humidity"),
                     get_json_object($"body", "$.city").alias("location"))
                     
 //Method 3 - Define json schema as Struct
 
 val jsonSchema = new StructType().add("device", StringType).add("reading", StringType)


val events = inputStream.select($"enqueuedTime".cast("Timestamp").alias("enqueuedTime"),from_json($"body".cast("String"), jsonSchema).alias("sensorReading"))


val eventdetails = events.select($"enqueuedTime",$"sensorReading.device".alias("device"), $"sensorReading.reading".cast("Float").alias("reading"))


val eventAvgs = eventdetails.withWatermark("enqueuedTime", "10 seconds").groupBy(
   window($"enqueuedTime", "1 minutes"),
   $"device"
  ).avg("reading").select($"window.start", $"window.end", $"device", $"avg(reading)")


eventAvgs.writeStream.format("csv").option("checkpointLocation", "/checkpoint").option("path", "/streamoutput").outputMode("append").start().awaitTermination()
