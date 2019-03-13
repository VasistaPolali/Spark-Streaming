package com.project.app

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream


import com.typesafe.config.ConfigFactory

object Streams extends App{

    //Load Configs
    val config = ConfigFactory.load("application.conf")

    //Set SaprkConf
    val conf = new SparkConf().setAppName(config.getString("spark.appName"))
     .setMaster(config.getString("spark.master"))
      .set("spark.driver.host", "localhost")

    val sc = new SparkContext(conf)


    //Init Streaming Context
    val ssc = new StreamingContext(sc, Seconds(config.getString("spark.batchInterval").toInt))

    //Set Kinesis Stream Context
    val streamName = config.getString("kinesis.streamName")
    val endpointURL=config.getString("kinesis.endpointURL")
    val regionName= config.getString("kinesis.regionName")
    val kinesisCheckpointInterval = Seconds(config.getString("spark.batchInterval").toInt)



    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    //val kinesisClient = new AmazonKinesisClient(credentials)
    //kinesisClient.setEndpoint(endpointURL)
    //val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size

    val numStreams = 1

    //Init Kinesis Streams
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, config.getString("kinesis.kinesisStreamAppName"), streamName, endpointURL, regionName,InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
    }

  //Vector(org.apache.spark.streaming.kinesis.KinesisInputDStream@3e6534e7)org.apache.spark.streaming.dstream.UnionDStream@670ce33118/08/29 15:16:29 WARN Worker: Received configuration for both region name as us-west-2, and Amazon Kinesis endpoint as https://kinesis.us-west-2.amazonaws.com. Amazon Kinesis endpoint will overwrite region name.

    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    val unionStreams = ssc.union(kinesisStreams)


    val res = unionStreams.map(byteArray => new String(byteArray))
  //org.apache.spark.streaming.dstream.UnionDStream
  print(unionStreams)
    res.print()


  val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  //Set Snowflake Options
  var sfOptions = Map(
    "sfURL" -> config.getString("snowflake.sfURL"),
    "sfAccount" -> config.getString("snowflake.sfAccount"),
    "sfUser" -> config.getString("snowflake.sfUser"),
    "sfPassword" -> config.getString("snowflake.sfPassword"),
    "sfDatabase" -> config.getString("snowflake.sfDatabase"),
    "sfSchema" -> config.getString("snowflake.sfSchema"),
    "sfWarehouse" -> config.getString("snowflake.sfWarehouse"),
    "sfRole" -> config.getString("snowflake.sfRole")
  )



  //Process DStreams for the batch
  unionStreams.foreachRDD{ rdd =>
      if (!rdd.isEmpty()) {
        //Convert bytearray to String
        val df_raw = sqlContext.read.json(rdd.map(byteArray => new String(byteArray)))
        val df_new = df_raw.drop("_metadata")

        //Get Coumns list to flatten
        val includeCols = config.getString("spark.cols").split(",")
        //Add messageId to Array
        val includeColsMessage = includeCols :+ "messageId"
        //Flatten selected columns
        val df_stage = df_new.select(FlattenSchema.flattenSchema(df_new.schema.fields.filter(c => includeColsMessage.contains(c.name))):_*)
        //Drop flatenned columns-parent from data frame
        val df_minus_cols = df_new.drop(includeCols:_*)
        //Join dataframe and flattened columns
        val df_final = df_minus_cols.join(df_stage,Seq("messageId"))


        //Run query against SnowFlake table-This is to get the existing schema.
        val df: DataFrame = sqlContext.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(sfOptions)
          .option("query", "select * from " + config.getString("snowflake.dbTable") + " limit 1")
          .load()

        val snowCols = df.columns
        val dfCols = df_final.columns.map(x => x.toUpperCase)


        //Identify difference between Snowflake table schema ad Dstream schema
        //to identify new columns
        val cols = dfCols.diff(snowCols)

        //Alter schema of the Snow flake table to add new columns
        if (!cols.isEmpty) {
          cols.map(c => SnowflakeConnector.alterSchema(c))
        }

        //var Mapper = scala.collection.mutable.Map.empty[String,String]
        var Mapper : Map[String, String] = Map()

        //Create ColumnMap-This helps in Insert Into, when there is num-column mismatch between the two schemas
        dfCols.foreach(c => Mapper += (c -> c))


        //Append DStream data to the table
        df_final.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(sfOptions)
          .option("dbtable", config.getString("snowflake.dbTable"))
          .option("columnmap",Mapper.toString)
          .mode(SaveMode.Append)
          .save()

      }


    }

    ssc.start()

    ssc.awaitTermination()


  }

//s3://segment-kinesis-streams/
