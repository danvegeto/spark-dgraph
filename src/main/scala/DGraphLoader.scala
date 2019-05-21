import com.google.gson.Gson
import io.dgraph.DgraphClient
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class DGraphLoader[T <: DGraphVertex](client: DgraphClient, spark: SparkSession) {

  def loadGraph[T <: DGraphVertex : ClassTag](query: String, vars: java.util.Map[String, String], edgeType: String): Graph[T, String] = {

    // create query transaction
    val transaction = client.newReadOnlyTransaction.queryWithVars(query, vars)

    // deserialize with Gson
    val gson = new Gson
    val response = gson.fromJson(transaction.getJson.toStringUtf8, classOf[DGraphQuery[T]])

    def uid(row: T): Long = {java.lang.Long.parseLong(row.getUid.substring(2), 16)}

    val vertices = spark.sparkContext.parallelize(
      response.all.asScala.map(v => (uid(v), v)).toList
    )

    val edges = spark.sparkContext.parallelize(
      response.all.asScala.filter(_.getEdges(edgeType) != null).flatMap(v1 => v1.getEdges(edgeType).asScala.map(v2 =>
        new Edge(uid(v1), uid(v2.asInstanceOf[T]), edgeType)))
    )

    Graph[T, String](vertices, edges)
  }

}
