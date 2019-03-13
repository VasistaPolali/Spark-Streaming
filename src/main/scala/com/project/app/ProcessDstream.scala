package com.project.app

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class ProcessDstream(){

  var df_final:DataFrame = _

  //Process Stream RDD and return ad DataFrame
  def processrdd(rdd: RDD[Array[Byte]],sqlContext: SQLContext,flattenCols:String):DataFrame={
   if (!rdd.isEmpty()) {
      //Convert bytearray to String
      val df_raw = sqlContext.read.json(rdd.map(byteArray => new String(byteArray)))
      val df_new = df_raw.drop("_metadata")

      //Get Coumns list to flatten
      val includeCols = flattenCols.split(",")
      //Add messageId to Array
      val includeColsMessage = includeCols :+ "messageId"
      //Flatten selected columns
      val df_stage = df_new.select(FlattenSchema.flattenSchema(df_new.schema.fields.filter(c => includeColsMessage.contains(c.name))): _*)
      //Drop flatenned columns-parent from data frame
      val df_minus_cols = df_new.drop(includeCols: _*)
      //Join dataframe and flattened columns
      df_final = df_minus_cols.join(df_stage, Seq("messageId"))
   }
df_final


  }





}
