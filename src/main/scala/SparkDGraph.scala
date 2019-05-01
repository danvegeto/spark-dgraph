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
      "lastName: string @index(exact) .\n" +
        "firstName: string @index(exact) .\n" +
        "company: string @index(exact) .\n" +
        "title: string @index(exact) .\n" +
        "city: string @index(exact) ."

    // set schema
    val op = Operation.newBuilder.setSchema(schema).build
    dgraphClient.alter(op)

    // test data
    val people = Seq(
      Person("Jain", "Manish", "DGraph Labs", "Founder", "San Francisco"),
      Person("Rivera", "Martin", "DGraph Labs", "Software Engineer", "San Francisco"),
      Person("Wang", "Lucas", "DGraph Labs", "Software Engineer", "San Francisco"),
      Person("Mai", "Daniel", "DGraph Labs", "DevOps Engineer", "San Francisco"),
      Person("Rivera", "Martin", "DGraph Labs", "Software Engineer", "San Francisco"),
      Person("Alvarado", "Javier", "DGraph Labs", "Senior Software Engineer", "San Francisco"),
      Person("Mangal", "Aman", "DGraph Labs", "Distributed Systems Engineer", "Bengaluru"),
      Person("Rao", "Karthic", "DGraph Labs", "Developer Advocate", "Bengaluru"),
      Person("Jarif", "Ibrahim", "DGraph Labs", "Software Engineer", "Bengaluru"),
      Person("Goswami", "Ashish", "DGraph Labs", "Distributed Systems Engineer", "Mathura"),
      Person("Conrado", "Michel", "DGraph Labs", "Software Engineer", "Salvador"),
      Person("Cameron", "James", "DGraph Labs", "Investor", "Sydney")
    )

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
        "    lastName\n" +
        "    firstName\n" +
        "    company\n" +
        "    title\n" +
        "    city\n" +
        "  }\n" +
        "}\n"

    // create query transaction
    val vars = Collections.singletonMap("$a", "DGraph Labs")
    val res = dgraphClient.newReadOnlyTransaction.queryWithVars(query, vars)

    // deserialize with Gson
    val ppl = gson.fromJson(res.getJson.toStringUtf8, classOf[People])

    // create Spark session
    val spark = SparkSession.builder().master("local").getOrCreate()

    // convert results list to Spark Dataset
    import spark.implicits._
    val data = ppl.all.asScala.toDS()

    // explore data with Spark Dataset operations
    data.show()
    data.groupBy('city).count().sort('count.desc).show()
    data.groupBy('title).count().sort('count.desc).show()
    data.groupBy('city, 'title).count().sort('count.desc).show()
  }
}

