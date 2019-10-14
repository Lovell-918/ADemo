package demo.Scala
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
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
    val readConfig = ReadConfig(Map("collection" -> "baseADC_graph", "readPreference.name" -> "secondaryPreferred"),Some(ReadConfig(sc)))
    val nameRdd = MongoSpark.load(sc,readConfig)
//    val len = nameRdd.count()
    //上面都是读的

    class VertexProperty()
    case class UserProperty (sname:String) extends VertexProperty
    val uvrdd = nameRdd.map(
      it => (it.get("sid").toString.trim.toLong,UserProperty(it.get("sname").toString)
      )
    )
    case class ChampionVertex (sname:String,metric: Double) extends VertexProperty
    val cvrdd = nameRdd.map(
      it => ChampionVertex()
    )
    //这里是英雄的那个

    var graph:Graph[VertexProperty, Double] //之前说用metric做边的权重？



    //    啊这一段是之前在胡乱尝试的
    //    case class UserName ()
    case class Edge (sid:String,cname:String,num:Double)
    val erdd : RDD[Edge] = nameRdd.map[Edge](
      it => Edge(it.get("sid").toString.trim,it.get("cname").toString.trim,it.get("metric").toString.trim.toDouble)
    )

    val graph: Graph[String,String] = Graph(uvrdd,erdd)

    //    println(mutableSet.size)

//现在似乎不需要的东西
    //    val edgeArray = Array()
//    val vertexArray = Array()
//
//    val vertexRDD : RDD[(String,String)] = sc.parallelize(vertexArray)
//    val edgeRDD : RDD[Edge] = sc.parallelize(edgeArray)
    spark.stop()
  }
}

