package com.project.app

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSpec
import org.apache.spark.sql._

//Calss to test Spark Application
class StreamingTest extends FunSpec {


  describe("#compare") {
    it("Asserts that actual and expected DataFrames are equal") {

      def changType(df: DataFrame ): Array[Column]={
        df.columns.map(c => {

          df(c).cast(StringType)

        }
        )
      }

      //Process test file
      val c = new ProcessStreams("application_test.conf")
      val context = c.createContext()
      //Mock a stream RDD
      val data = context._1.textFile("test.json").map { x => x.getBytes() }

      //Process the stream RDD
      val d = new ProcessDstream
      val df_res = d.processrdd(data, context._3, "context")

      val df_actual = df_res.select(changType(df_res):_*)

      //Create expected result Dataframe
      val df_expected= context._3.read.option("header","true").csv("src/test/resources/expected_output.csv")

      var Mapper: Map[String, String] = Map()

      val df_Cols = df_res.columns.map(x => x.toUpperCase)
      df_Cols.foreach(c => Mapper += (c -> c))

      assert(df_actual.except(df_expected).count() === 0)



    }
  }


}