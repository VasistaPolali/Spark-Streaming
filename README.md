Spark Streaming Application for Kinesis Streams
The repo contains the code to Run the Spark Streaming Application for Kinesis streams with Yarn-Cluster.


####################################################################################
Spark Application
The Spark Streaming application receives Dstreams from a Kinesis receiver and loads it into a Database table.
Snowfalke Datawarehouse has been uswed as an example.

The Spark application is Agnostic of the data source into Kinesis and the schema.
 - It receives the messages from a Kinesis stream.
 - Flattens the attributes, specified in the application conf as "cols" retaining the remaining nested attribute values as it is.
 - Creates a Dataframe outof it.
 - Identifies the difference in schema between the database table and the DataFrame form the new Dstream.
 - Alters the database table to add new Columns.
 - Appends the data in the new Dstream into Snowflake table with a "df.write" operation.
 - The batch interval is specified by "batchInterval" property in application.conf
Running the Spark Application.
 -The main class is "com.project.app.Main" object.
 -Pass the "application.conf" file from resources as argument for every unique stream and environment.

 -application.conf.template
 ###################################################################################
 spark {  
  master = #Master Url, specify if not yarn-cluster
  appName = #Unique Spark application name.Has to be same for any subsequent submissions.
  batchInterval = #Stream batch interval  
  cols = # Comma separated names of the JSON attributes  to be flattened ex: "context,properties"  

}  
kinesis{  
  streamName = #Kinesis stream name
  endpointURL=#Kinesis url
  regionName= #Kinesis stream region 
  kinesisStreamAppName =#Kinesis stream app name 
}  
  
<database>{
  URL=#<database>(destination) url
  Account=#<database>(destination) account
  sfUser=#<database>(destination) User
  sfPassword =#<database>(destination) Password
  sfDatabase=#<database>(destination) Database
  sfSchema=#<database>(destination) schema
  sfWarehouse=#<database>(destination) Schema
  sfRole=#<database>(destination) Role
  dbTable=<database>(destination) Table
}
####################################################################################