package demo.Scala
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson
import org.bson.Document

object ReadMongo{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("myApp")
      .config("spark.mongodb.input.uri","mongodb://127.0.0.1/lol.baseADC_graph")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    import com.mongodb.spark.config._
    val readConfig = ReadConfig(Map("collection" -> "base_graph", "readPreference.name" -> "secondaryPreferred"),Some(ReadConfig(sc)))
    val nameRdd = MongoSpark.load(sc,readConfig)
//    val len = nameRdd.count()
    //上面都是读的

    class VertexProperty()
    case class UserProperty (sname:String) extends VertexProperty
    val uvrdd = nameRdd.map(
      it => (it.get("sid").toString.trim.toLong,
        UserProperty(it.get("sname").toString)
      )
    )
    case class ChampionProperty (cname:String,metric: Double) extends VertexProperty
    val cvrdd = nameRdd.map(
      it => (it.get("cid").toString.trim.toLong,
        ChampionProperty(it.get("cname").toString.trim,it.get("metric").toString.trim.toDouble))
    )
    val RDDe = nameRdd.map(
      it => Edge(it.get("sid").toString.trim.toLong,
      it.get("cid").toString.trim.toLong,
      it.get("metric").toString.trim.toDouble)
    )
    val erdd = EdgeRDD.fromEdges(RDDe)
    var graph:Graph[VertexProperty, Double] = Graph.apply()//之前说用metric做边的权重？

    spark.stop()
  }
}

