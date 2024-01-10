The idea is based on the [https://github.com/vertica/spark-connector](https://github.com/vertica/spark-connector) library.  

While this already can give you a huge speedup (depending on the number of nodes and query speed), using a similar solution with supporting Apache Arrow format would be a much better solution.
Obviously using it's advanced features like REE and dictionary (as Vertica may have compatible internal concepts) would make the implementation the fastest and most ergonomic.

How it works:

1. Start a fake webhdfs server in the same thread using Starlette
2. Issue `EXPORT TO PARQUET(directory='hdfs://fake_webhdfs:8000/somedata') AS SELECT ...`
3. Collect the parquet binary files in the webserver in-memory
4. Convert the collect parquet tables into pyarrow tables

Limitations:
1. It's pure python, receiving IO performance may not be optimal
2. Using Parquet vs Arrow still has some overhead
3. Vertica has to connect to the python script, for this it needs IP or hostname
4. It's not encrypted
5. Resultset needs to fit into memory, no streaming yet  
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

Inefficiencies discovered: