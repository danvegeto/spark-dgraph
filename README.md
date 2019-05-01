# Spark-DGraph Connector
This is a proof-of-concept of data exchange between the [Apache Spark](http://spark.apache.org/) Dataset API and [DGraph](https://dgraph.io/).

Currently only reading data from a DGraph query into a Spark Dataset is tested, further work should include writing from a Spark Dataset into DGraph as well as integration with Spark's graph API [GraphX](http://spark.apache.org/graphx/).

## Dependencies

**Install [SBT](https://www.scala-sbt.org/) for dependency management**

```
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt
```

## Usage

**Start DGraph**
```
sudo docker run -it -p 5080:5080 -p 6080:6080 -p 8080:8080 -p 9080:9080 -p 8000:8000 -v ~/dgraph:/dgraph --name dgraph dgraph/dgraph dgraph zero
sudo docker exec -it dgraph dgraph alpha --lru_mb 2048 --zero localhost:5080
```

**Run with SBT**
```
sbt run
```

## Examples

After a query has been loaded into `data`, a Spark Dataset of DGraph's employees, it can be explored with Spark's API.

`data.show()`

```
+--------+---------+-----------+--------------------+-------------+
|lastName|firstName|    company|               title|         city|
+--------+---------+-----------+--------------------+-------------+
|    Jain|   Manish|DGraph Labs|             Founder|San Francisco|
|  Rivera|   Martin|DGraph Labs|   Software Engineer|San Francisco|
|    Wang|    Lucas|DGraph Labs|   Software Engineer|San Francisco|
|     Mai|   Daniel|DGraph Labs|     DevOps Engineer|San Francisco|
|  Rivera|   Martin|DGraph Labs|   Software Engineer|San Francisco|
|Alvarado|   Javier|DGraph Labs|Senior Software E...|San Francisco|
|  Mangal|     Aman|DGraph Labs|Distributed Syste...|    Bengaluru|
|     Rao|  Karthic|DGraph Labs|  Developer Advocate|    Bengaluru|
|   Jarif|  Ibrahim|DGraph Labs|   Software Engineer|    Bengaluru|
| Goswami|   Ashish|DGraph Labs|Distributed Syste...|      Mathura|
| Conrado|   Michel|DGraph Labs|   Software Engineer|     Salvador|
| Cameron|    James|DGraph Labs|            Investor|       Sydney|
+--------+---------+-----------+--------------------+-------------+
```

`data.groupBy('city).count().sort('count.desc).show()`
```
+-------------+-----+
|         city|count|
+-------------+-----+
|San Francisco|    6|
|    Bengaluru|    3|
|       Sydney|    1|
|      Mathura|    1|
|     Salvador|    1|
+-------------+-----+
```

`data.groupBy('title).count().sort('count.desc).show()`
```
+--------------------+-----+
|               title|count|
+--------------------+-----+
|   Software Engineer|    5|
|Distributed Syste...|    2|
|  Developer Advocate|    1|
|             Founder|    1|
|     DevOps Engineer|    1|
|Senior Software E...|    1|
|            Investor|    1|
+--------------------+-----+
```

`data.groupBy('city, 'title).count().sort('count.desc).show()`
```
+-------------+--------------------+-----+
|         city|               title|count|
+-------------+--------------------+-----+
|San Francisco|   Software Engineer|    3|
|       Sydney|            Investor|    1|
|San Francisco|     DevOps Engineer|    1|
|San Francisco|Senior Software E...|    1|
|San Francisco|             Founder|    1|
|      Mathura|Distributed Syste...|    1|
|    Bengaluru|  Developer Advocate|    1|
|    Bengaluru|Distributed Syste...|    1|
|    Bengaluru|   Software Engineer|    1|
|     Salvador|   Software Engineer|    1|
+-------------+--------------------+-----+
```
