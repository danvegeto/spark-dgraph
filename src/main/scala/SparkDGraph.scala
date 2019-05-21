import com.google.gson.Gson
import io.dgraph.DgraphProto.Operation
import io.dgraph.DgraphClient
import io.dgraph.DgraphGrpc
import io.grpc.ManagedChannelBuilder
import io.dgraph.DgraphProto.Mutation
import java.util.Collections
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SparkDGraph
{
  def main(args: Array[String]): Unit =
  {
    // suppress INFO logging
    Logger.getLogger("org").setLevel(Level.OFF)

    // connect to DGraph instance
    val channel = ManagedChannelBuilder.forAddress("localhost", 9080).usePlaintext(true).build
    val stub = DgraphGrpc.newStub(channel)
    val dgraphClient = new DgraphClient(stub)

    // drop all data including schema from the dgraph instance
    dgraphClient.alter(Operation.newBuilder().setDropAll(true).build())

    // Person schema
    val schema =
      "uid: string @index(exact) .\n" +
      "lastName: string @index(exact) .\n" +
      "firstName: string @index(exact) .\n" +
      "company: string @index(exact) .\n" +
      "title: string @index(exact) .\n" +
      "city: string @index(exact) .\n" +
      "friend: uid @count ."

    // set schema
    val op = Operation.newBuilder.setSchema(schema).build
    dgraphClient.alter(op)

    // test data
    val people = Seq(
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

    people(6).friends.add(people(7))
    people(6).friends.add(people(8))
    people(7).friends.add(people(8))

    people(9).friends.add(people(10))

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

    // query by company
    val query =
      "query all($a: string){\n" +
        "  all(func: eq(company, $a)) {\n" +
        "    uid\n" +
        "    lastName\n" +
        "    firstName\n" +
        "    company\n" +
        "    title\n" +
        "    city\n" +
        "    friends {\n" +
        "      uid\n" +
        "      lastName\n" +
        "      firstName\n" +
        "      company\n" +
        "      title\n" +
        "      city\n" +
        "    }\n" +
        "  }\n" +
        "}\n"

    // create query transaction
    val vars = Collections.singletonMap("$a", "DGraph Labs")
    val res = dgraphClient.newReadOnlyTransaction.queryWithVars(query, vars)

    println(res.getJson.toStringUtf8)

    // deserialize with Gson
    val response = gson.fromJson(res.getJson.toStringUtf8, classOf[PersonResponse])

    // create Spark session
    val spark = SparkSession.builder().master("local").getOrCreate()

    def uid(p: Person): Long = {java.lang.Long.parseLong(p.uid.substring(2), 16)}

    val vertices = spark.sparkContext.parallelize(
      response.all.asScala.map(p => (uid(p), p)).toList
    )

    val edges = spark.sparkContext.parallelize(
      response.all.asScala.filter(_.friends != null).flatMap(p1 => p1.friends.asScala.map(p2 =>
        new Edge(uid(p1), uid(p2), "Friend")))
    )

    val graph = Graph(vertices, edges)
    graph.vertices.foreach(println)
    graph.edges.foreach(println)

    graph.ops.connectedComponents().vertices.join(graph.vertices).groupBy(_._2._1)
      .sortBy(_._2.toList.length, ascending = false)
      .map(c => "Group of " + c._2.toList.length + ": " + c._2.map(p => p._2._2.firstName + " " + p._2._2.lastName).mkString(", ")).foreach(println)
  }
}

