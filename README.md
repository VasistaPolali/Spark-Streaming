
**Spark Streaming Application for Kinesis Streams** 

The repo contains the code to Run a Spark-Streaming application for Kinesis streams with Yarn-Cluster.

**Spark Application** 

The Spark-Streaming application receives Dstreams from a Kinesis receiver and loads it into a database table. Snowflake data warehouse has been used as an example.

The Spark application is agnostic of the data source into Kinesis and the schema.
-   It receives the messages from a Kinesis stream.
-   Flattens the attributes, specified in the application conf as "cols" retaining the remaining nested attribute values as it is.
-   Creates a dataframe out of it.
-   Identifies the difference in schema between the database table and the dataframe form the new Dstream.
-   Alters the database table to add new columns.
-   Appends the data in the new Dstream into the database table with a "df.write" operation.
-   The batch interval is specified by "batchInterval" property in application.conf 
- 
**Running the Spark Application**

-The main class is "com.project.app.Main" 
-Pass the "application.conf" file from resources as argument for every unique stream and environment.

**application.conf.template** 
```
spark {  
master = Master Url, specify if not yarn-cluster 
appName =Unique Spark application name.Has to be same for any subsequent submissions. 
batchInterval = Stream batch interval  
cols = Comma separated names of the JSON attributes to be flattened;format->"colname,colname"
}  

kinesis {  
streamName = Kinesis stream name 
endpointURL=Kinesis url 
regionName= Kinesis stream region
kinesisStreamAppName =Kinesis stream app name 
}

database { 
url=database(destination) url 
account=database(destination) account 
user=database(destination) user
password =database(destination) password 
database=database(destination) database
schema=database(destination) schema 
warehouse=database(destination) warehouse 
role=database(destination) Role 
table=database(destination) Table
 } 
