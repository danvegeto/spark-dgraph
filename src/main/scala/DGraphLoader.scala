import com.google.protobuf.ByteString
import io.dgraph.DgraphClient
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class DGraphLoader[V <: DGraphVertex : ClassTag, Q <: DGraphQuery[V] : ClassTag]
  (dgraphClient: DgraphClient, spark: SparkSession, deserializer: ByteString => Q) {

  def loadGraph(query: String, vars: Map[String, String], edgeType: String): Graph[V, String] = {

    // create query transaction
    val transaction = dgraphClient.newReadOnlyTransaction.queryWithVars(query, vars.asJava)

    // deserialize with function argument provided at runtime (necessary for generic types)
    val queryResults = deserializer(transaction.getJson) // an instance of generic type Q

    // helper function to extract uid from an instance of DGraphVertex
    def uid(row: DGraphVertex): Long = {java.lang.Long.parseLong(row.getUid.substring(2), 16)}

    // create vertices rdd: RDD[(Long, V)]
    val vertices = spark.sparkContext.parallelize(
      queryResults.getAll.asScala.map(v => (uid(v), v))
    )

    // create edges rdd: RDD[Edge[String]]
    val edges = spark.sparkContext.parallelize(
      queryResults.getAll.asScala
        .filter(_.getEdges(edgeType) != null)
        .flatMap(v1 => // for each vertex v1
          v1.getEdges(edgeType).asScala // get all edges
            .map(_.asInstanceOf[V]) // cast v2 to generic type V
            .map(v2 => new Edge(uid(v1), uid(v2), edgeType))
        )
    )

    Graph(vertices, edges)
  }
}
