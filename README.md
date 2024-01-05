The python scripts starts a fake webhdfs server, receives the resultset as parquet binary data in parallel from all the exection nodes into the webserver and converts it to pyarrow (all in-memory).

```
+----------------+                      +-----------+                   +------------+ 
|                |                      |           |                   |            | 
| Python Script  |                      |  Vertica  |                   |  Vertica   | 
| (Runs WebHDFS) |                      |   Node    |                   | Data Nodes | 
|                |                      |           |                   |            | 
+------+---------+                      +----+------+                   +----+-------+ 
       |                                     |                               |        
       |---- Run EXPORT TO PARQET Query ---->|                               |
       |                                     |--- Sends the execution jobs ->|         
       |                                     |                               |
       |<--------- Vertica data nodes connect -------------------------------|        
       |<--------- Parquet --------------------------------------------------|        
       |<--------- Parquet --------------------------------------------------|        
       |<--------- Parquet --------------------------------------------------|        
       |<--------- Parquet --------------------------------------------------|        
       |<--------- Parquet --------------------------------------------------|        
       |<--------- Parquet --------------------------------------------------|
       |                                     |
       |                                     |
       |<----- EXPORT TO PARQET returns -----|
```