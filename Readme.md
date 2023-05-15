
![logo](./logo.png)

This repository contains a set of scripts and tools for running benchmarks on vanilla Redis GEO commands and RediSearch, a full-text search engine for Redis. 

The benchmarks in this repository cover a range of common Redis GEO and RediSearch operations, such as indexing, searching, and querying data. 

The results of the benchmarks can be used to compare the performance of different Redis configurations, to gain insights into the behavior of these tools,  and to identify potential bottlenecks or areas for optimization.


### Try it out

#### GeoPoints
```
wget https://s3.us-east-2.amazonaws.com/redis.benchmarks.spec/datasets/geopoint/documents.json.bz2
bzip2 -d documents.json.bz2
make build
./geo-bench load
```

#### GeoPolygons

```
wget https://s3.us-east-2.amazonaws.com/redis.benchmarks.spec/datasets/geoshape/polygons.json.bz2
bzip2 -d polygons.json.bz2
make build
./geo-bench load --input-type geoshape --input polygons.json 
```
