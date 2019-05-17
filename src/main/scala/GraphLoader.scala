import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object GraphLoader extends App
{
  //TODO DGraph query -> JSON -> RDD[Edge] -> Graph

  // suppress INFO logging
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().master("local").getOrCreate()

  val vertices = spark.sparkContext.parallelize(List(
    (1L, new Person("1", "Jain", "Manish", "DGraph Labs", "Founder", "San Francisco")),
    (2L, new Person("2", "Rivera", "Martin", "DGraph Labs", "Software Engineer", "San Francisco")),
    (3L, new Person("3", "Wang", "Lucas", "DGraph Labs", "Software Engineer", "San Francisco")),
    (4L, new Person("4", "Mai", "Daniel", "DGraph Labs", "DevOps Engineer", "San Francisco")),
    (5L, new Person("5", "Rivera", "Martin", "DGraph Labs", "Software Engineer", "San Francisco")),
    (6L, new Person("6", "Alvarado", "Javier", "DGraph Labs", "Senior Software Engineer", "San Francisco")),
    (7L, new Person("7", "Mangal", "Aman", "DGraph Labs", "Distributed Systems Engineer", "Bengaluru")),
    (8L, new Person("8", "Rao", "Karthic", "DGraph Labs", "Developer Advocate", "Bengaluru")),
    (9L, new Person("9", "Jarif", "Ibrahim", "DGraph Labs", "Software Engineer", "Bengaluru")),
    (10L, new Person("10", "Goswami", "Ashish", "DGraph Labs", "Distributed Systems Engineer", "Mathura")),
    (11L, new Person("11", "Conrado", "Michel", "DGraph Labs", "Software Engineer", "Salvador")),
    (12L, new Person("12", "Cameron", "James", "DGraph Labs", "Investor", "Sydney")
  )))

  val edges = spark.sparkContext.parallelize(List(
    Edge(1L,2L, "Friend"),
    Edge(1L,3L, "Friend"),
    Edge(1L,4L, "Friend"),
    Edge(1L,5L, "Friend"),
    Edge(6L,7L, "Friend"),
    Edge(6L,8L, "Friend"),
    Edge(6L,9L, "Friend"),
    Edge(6L,10L, "Friend"),

  ))

  val graph = Graph(vertices, edges)
  graph.vertices.foreach(println)
  graph.edges.foreach(println)

  graph.ops.connectedComponents().vertices.groupBy(_._2).foreach(println)
}
