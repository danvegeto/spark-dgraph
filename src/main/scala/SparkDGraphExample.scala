import com.google.gson.Gson
import io.dgraph.DgraphProto.Operation
import io.dgraph.DgraphClient
import io.dgraph.DgraphGrpc
import io.grpc.ManagedChannelBuilder
import io.dgraph.DgraphProto.Mutation
import com.google.protobuf.ByteString
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkDGraphExample
{
  def main(args: Array[String]): Unit =
  {
    val dgraphHost = "localhost"
    val dgraphPort = 9080
    val dgraphSchema = readFile("src/main/dgql/personSchema")
    val dgraphQuery = readFile("src/main/dgql/personQuery")
    val dgraphQueryVars = Map("$a" -> "DGraph Labs")
    val dgraphEdgeType = "friends" // corresponds to 'friends' field in Person
    val sparkMasterUrl = "local"

    // connect to DGraph instance
    val channel = ManagedChannelBuilder.forAddress(dgraphHost, dgraphPort).usePlaintext(true).build
    val stub = DgraphGrpc.newStub(channel)
    val dgraphClient = new DgraphClient(stub)

    // drop all data including schema from the dgraph instance
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build())

    // set schema
    val op = Operation.newBuilder.setSchema(dgraphSchema).build
    dgraphClient.alter(op)

    // initialize test data
    val people = getTestData

    // serialize with Gson
    val gson = new Gson
    val peopleMutations = people
      .map(gson.toJson)
      .map(p =>
        Mutation.newBuilder.setSetJson(com.google.protobuf.ByteString.copyFromUtf8(p)).build
      )

    // perform mutation transactions
    val txn = dgraphClient.newTransaction
    try {
      peopleMutations.foreach(txn.mutate)
      txn.commit()
    }
    finally txn.discard()

    // create Spark session
    val spark = SparkSession.builder().master(sparkMasterUrl).getOrCreate()

    // create deserializer with Gson
    val deserializer = (bytes: ByteString) => new Gson().fromJson(bytes.toStringUtf8, classOf[PersonQuery])

    // create graph loader for Person/PersonQuery
    val loader = new DGraphLoader[Person, PersonQuery](dgraphClient, spark, deserializer)

    // load query results into Spark Graph
    val graph = loader.loadGraph(dgraphQuery, dgraphQueryVars, dgraphEdgeType)

    // print graph data (vertices, edges, and triplets)
    graph.vertices.foreach(println)
    graph.edges.foreach(println)
    graph.triplets.foreach(println)

    // example of a graph operation - print out the connected components
    graph.ops.connectedComponents().vertices
      .join(graph.vertices) // join on uid
      .groupBy(_._2._1) // group by root vertex of each component
      .sortBy(_._2.toList.length, ascending = false) // sort by component size
      // generate string "Group of <size>: <p1>, <p2>, ... <pn>"
      .map(c => "Group of " + c._2.toList.length + ": " +
        c._2.map(p => p._2._2.firstName + " " + p._2._2.lastName).mkString(", "))
      .foreach(println)

    spark.stop()
  }

  def readFile(filePath: String): String = {
    val source = Source.fromFile(filePath)
    val lines = try source.mkString finally source.close()
    lines
  }

  def getTestData: List[Person] = {

    val people = List(
      new Person("0", "Manish", "Jain", "DGraph Labs", "Founder", "San Francisco"),
      new Person("1", "Martin", "Rivera", "DGraph Labs", "Software Engineer", "San Francisco"),
      new Person("2", "Lucas", "Wang", "DGraph Labs", "Software Engineer", "San Francisco"),
      new Person("3", "Daniel", "Mai", "DGraph Labs", "DevOps Engineer", "San Francisco"),
      new Person("4", "Javier", "Alvarado", "DGraph Labs", "Senior Software Engineer", "San Francisco"),
      new Person("5", "Aman", "Mangal", "DGraph Labs", "Distributed Systems Engineer", "Bengaluru"),
      new Person("6", "Karthic", "Rao", "DGraph Labs", "Developer Advocate", "Bengaluru"),
      new Person("7", "Ibrahim", "Jarif", "DGraph Labs", "Software Engineer", "Bengaluru"),
      new Person("8", "Ashish", "Goswami", "DGraph Labs", "Distributed Systems Engineer", "Mathura"),
      new Person("9", "Michel", "Conrado", "DGraph Labs", "Software Engineer", "Salvador"),
      new Person("10", "James", "Cameron", "DGraph Labs", "Investor", "Sydney"),
    )

    people(0).friends.add(people(1))
    people(0).friends.add(people(2))
    people(0).friends.add(people(3))
    people(0).friends.add(people(4))
    people(1).friends.add(people(2))
    people(1).friends.add(people(3))
    people(1).friends.add(people(4))
    people(2).friends.add(people(3))
    people(2).friends.add(people(4))
    people(4).friends.add(people(5))

    people(6).friends.add(people(7))
    people(6).friends.add(people(8))
    people(7).friends.add(people(8))

    people(9).friends.add(people(10))

    people
  }

}

