# Spark-DGraph Connector
This is a proof-of-concept of data exchange between the [Apache Spark](http://spark.apache.org/) [GraphX API](https://spark.apache.org/graphx/) and [DGraph](https://dgraph.io/).

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

## Example

After a query has been loaded into `graph`, it can be explored with Spark's GraphX API

```Scala
    // example of a graph operation - print out the connected components
    graph.ops.connectedComponents().vertices
      .join(graph.vertices) // join on uid
      .groupBy(_._2._1) // group by root vertex of each component
      .sortBy(_._2.toList.length, ascending = false) // sort by component size
      // generate string "Group of <size>: <p1>, <p2>, ... <pn>"
      .map(c => "Group of " + c._2.toList.length + ": " +
        c._2.map(p => p._2._2.firstName + " " + p._2._2.lastName).mkString(", "))
      .foreach(println)
```

```
Group of 6: Lucas Wang, Javier Alvarado, Martin Rivera, Daniel Mai, Manish Jain, Aman Mangal
Group of 3: Karthic Rao, Ibrahim Jarif, Ashish Goswami
Group of 2: Michel Conrado, James Cameron
```
