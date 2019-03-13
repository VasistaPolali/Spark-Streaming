package com.project.app

import com.wbtechnology.app.Streams.config

object SnowflakeConnector extends App{

  def alterSchema(col: String){

    //Set Snowflake Options
    //Below config shows an example with  Snowflake as the data-destination
    //Replace "<database> with actual destination name from application.conf
    val username = config.getString("<database>.User")
    val password = config.getString("<database>.Password")
    val url = config.getString("<database>.URL")
    val warehouse= config.getString("<database>.Warehouse")
    val db= config.getString("<data_source>.Database")
    val schema=config.getString("<data_source>.Schema")
    val dbTable=config.getString("<database>.Table")

    val driver = "net.snowflake.client.jdbc.SnowflakeDriver"

    val url = "jdbc:snowflake://" + url + "/?warehouse=" + warehouse +
      "&db=" + db + "&schema=" + schema



    import java.sql.DriverManager
    import java.sql.Connection

    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()

      statement.executeQuery("use warehouse " + warehouse)
      statement.executeQuery("use database " + db)

      val query = "alter table " +  schema + "." + dbTable + " add column " +  col + " variant"
      //print(query)
      val resultSet = statement.executeQuery(query)
      //val resultSet = statement.executeQuery("SELECT * FROM segment_web_dev_kinesis LIMIT 1 ")


    }
    catch {
      case e => e.printStackTrace
    }

  }


}
