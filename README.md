How it works:

1. Start a fake webhdfs server in the same thread using FastAPI
2. Issue `EXPORT TO PARQUET(directory='hdfs://fake_webhdfs:8000/somedata') AS SELECT ...`
3. Collect the parquet binary files in the webserver in-memory
4. Convert the collect parquet tables into pyarrow tables

Limitations:
1. It can be used on a single thread only right now
2. It's pure python, receiving IO performance may not be optimal
3. Using Parquet vs Arrow still has some overhead
4. Vertica has to connect to the python script, for this it needs IP or hostname
5. It's not encrypted
6. Resultset needs to fit into memory, no streaming yet  
actually it needs to keep both the compressed and uncompressed versions in the memory for a moment

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