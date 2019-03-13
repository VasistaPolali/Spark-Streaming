package com.project.app

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}


object FlattenSchema extends App{

  def flattenSchema(schema:Array[StructField], prefix : String = null): Array[Column]= {
    schema.flatMap(f => {

        val colName = if (prefix == null) f.name else (prefix + "." + f.name)


        f.dataType match {
          case st: StructType => flattenSchema(st.fields, colName)
          case _ => Array(col(colName).alias(colName.replace(".", "_")))

      }
      }

    )
  }

}
