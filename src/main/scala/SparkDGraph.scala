import com.google.gson.Gson
import io.dgraph.DgraphProto.Operation
import io.dgraph.DgraphClient
import io.dgraph.DgraphGrpc
import io.grpc.ManagedChannelBuilder
import io.dgraph.DgraphProto.Mutation
import java.util.Collections

import org.apache.log4j.{Level, Logger}
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
      new Person("1", "Jain", "Manish", "DGraph Labs", "Founder", "San Francisco"),
      new Person("2", "Rivera", "Martin", "DGraph Labs", "Software Engineer", "San Francisco"),
      new Person("3", "Wang", "Lucas", "DGraph Labs", "Software Engineer", "San Francisco"),
      new Person("4", "Mai", "Daniel", "DGraph Labs", "DevOps Engineer", "San Francisco"),
      new Person("5", "Rivera", "Martin", "DGraph Labs", "Software Engineer", "San Francisco"),
      new Person("6", "Alvarado", "Javier", "DGraph Labs", "Senior Software Engineer", "San Francisco"),
      new Person("7", "Mangal", "Aman", "DGraph Labs", "Distributed Systems Engineer", "Bengaluru"),
      new Person("8", "Rao", "Karthic", "DGraph Labs", "Developer Advocate", "Bengaluru"),
      new Person("9", "Jarif", "Ibrahim", "DGraph Labs", "Software Engineer", "Bengaluru"),
      new Person("10", "Goswami", "Ashish", "DGraph Labs", "Distributed Systems Engineer", "Mathura"),
      new Person("11", "Conrado", "Michel", "DGraph Labs", "Software Engineer", "Salvador"),
      new Person("12", "Cameron", "James", "DGraph Labs", "Investor", "Sydney"),
    )

    people(1).friends.add(people(2))

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
        "    friend {\n" +
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
    val ppl = gson.fromJson(res.getJson.toStringUtf8, classOf[PersonDataset])

    // create Spark session
    val spark = SparkSession.builder().master("local").getOrCreate()

    // convert results list to Spark Dataset
    import spark.implicits._

    val data = ppl.all.asScala.map(p => ScalaPerson(p.uid, p.firstName, p.lastName, p.company, p.title, p.city)).toDS()

    // explore data with Spark Dataset operations
    data.show()
    data.groupBy('city).count().sort('count.desc).show()
    data.groupBy('title).count().sort('count.desc).show()
    data.groupBy('city, 'title).count().sort('count.desc).show()
  }
}

