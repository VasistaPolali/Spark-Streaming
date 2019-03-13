package com.project.app

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._
import org.apache.spark.{SparkConf, SparkContext}

class  ProcessStreams(confFile: String) {

  val config = ConfigFactory.load(confFile)


  val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  //Set Snowflake Options
  //Below config shows an example with  Snowflake as the data-destination
  //Replace "<database> with actual destination name from application.conf
  var sfOptions = Map(
    "sfURL" -> config.getString("<database>.URL"),
    "sfAccount" -> config.getString("<database>.Account"),
    "User" -> config.getString("<database>.User"),
    "sfPassword" -> config.getString("<database>.Password"),
    "sfDatabase" -> config.getString("<database>.Database"),
    "sfSchema" -> config.getString("<database>.Schema"),
    "sfWarehouse" -> config.getString("<database>.Warehouse"),
    "sfRole" -> config.getString("<database>.Role")
  )


  //Run query against SnowFlake table-This is to get the existing schema.
  def readSnowData(sqlContext: SQLContext,sfOptions: Map[String,String]):DataFrame={
    val df_columns: DataFrame = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", "select * from " + config.getString("<data_source>.Table") + " limit 1")
      .load()
    df_columns
  }

  //Function to prepate and write data to snowflake
  def writeSnowData(df_final:DataFrame,df_columns:DataFrame,sfOptions: Map[String,String]) {
    var Mapper: Map[String, String] = Map()

    val dfCols = df_final.columns.map(x => x.toUpperCase)
    dfCols.foreach(c => Mapper += (c -> c))

    //Identify difference between Snowflake table schema ad Dstream schema  `
    //to identify new columns

    val snowCols = df_columns.columns
    val cols = dfCols.diff(snowCols)

    if (!cols.isEmpty) {
      cols.map(c => SnowflakeConnector.alterSchema(c))
    }
    //Append DStream data to the table
    df_final.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", config.getString("<data_source>.Table"))
      .option("columnmap", Mapper.toString)
      .mode(SaveMode.Append)
      .save()

  }

  def createContext():(SparkContext,StreamingContext,SQLContext)={

    //Set SaprkConf
    val conf = new SparkConf().setAppName(config.getString("spark.appName"))
      .setMaster(config.getString("spark.master"))
      //.set("spark.driver.host", "localhost")

    val sc = new SparkContext(conf)

    //Load Configs

    //Init Streaming Context
    val ssc = new StreamingContext(sc, Seconds(config.getString("spark.batchInterval").toInt))
    val sqlContext = new SQLContext(sc)

    return (sc,ssc,sqlContext)

  }


  def processStream(ssc: StreamingContext,sqlContext: SQLContext){

    //Set Kinesis Stream Context
    val streamName = config.getString("kinesis.streamName")
    val endpointURL = config.getString("kinesis.endpointURL")
    val regionName = config.getString("kinesis.regionName")
    val kinesisCheckpointInterval = Seconds(config.getString("spark.batchInterval").toInt)


    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")

    val numStreams = 1

    //Init Kinesis Streams
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, config.getString("kinesis.kinesisStreamAppName"), streamName, endpointURL, regionName, InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
    }

    import org.apache.spark.sql.SQLContext


    val unionStreams = ssc.union(kinesisStreams)


    val res = unionStreams.map(byteArray => new String(byteArray))
    //org.apache.spark.streaming.dstream.UnionDStream
    print(unionStreams)
    res.print()

    //val processDstreams = new ProcessDstream(Array("application.conf"))

    val ps = new ProcessDstream

    //Process DStreams for the batch
    unionStreams.foreachRDD { rdd =>

      val df_columns = readSnowData(sqlContext,sfOptions)
      val df_final= ps.processrdd(rdd, sqlContext,config.getString("spark.cols"))
      writeSnowData(df_final,df_columns,sfOptions)

    }


    ssc.start()

    ssc.awaitTermination()




  }
}

