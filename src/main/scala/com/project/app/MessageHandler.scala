package com.project.app

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.kinesis.shaded.amazonaws.services.kinesis.model.Record
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._
import org.apache.spark.{SparkConf, SparkContext}


class MessageHandler {

  case class Message(var shardId: Option[String], partitionKey: String, seqNum: String, subSeqNum: String, json: String)

  private val messageHandler : (Record => Message) = {
    (record : Record) => {
      val partitionKey = record.getPartitionKey
      val seqNum = record.getSequenceNumber
      val subSeqNum = record.asInstanceOf[UserRecord].getSubSequenceNumber.toString
      var json = new Array[Byte](record.getData.remaining)
      val data = record.getData
      data.get(json)
      // note that we do not assign a shard-id, it must be calculated later
      new Message(None, partitionKey, seqNum, subSeqNum, new String(json, "UTF-8"))
    }
  }
}
