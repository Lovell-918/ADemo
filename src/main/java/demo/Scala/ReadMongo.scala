package demo.Scala

import com.mongodb.spark.MongoSpark
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import org.graphstream.graph.{Graph => GraphStream}

object ReadMongo extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("myApp")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/lol.base_graph")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    val nameRdd = MongoSpark.load(sc)
    //    val len = nameRdd.count()
    //上面都是读的

    class VertexProperty()
    case class UserProperty(sname: String) extends VertexProperty
    case class ChampionProperty(cname: String,pos: String) extends VertexProperty
    val uvrdd: RDD[(VertexId, VertexProperty)] = nameRdd.map(
      it => (it.get("sid").toString.trim.toLong, UserProperty(it.get("sname").toString)
      )
    )
    val cvrdd: RDD[(VertexId, VertexProperty)] = nameRdd.map(
      it => (it.get("cid").toString.trim.toLong, ChampionProperty(it.get("cname").toString, it.get("pos").toString))
    )

    val vrdd: RDD[(VertexId, VertexProperty)] = uvrdd.++(cvrdd)
    //这里是英雄的那个

    var graph: Graph[VertexProperty, Double] = null //之前说用metric做边的权重？

    //    啊这一段是之前在胡乱尝试的
    //    case class UserName ()
    val erdd: RDD[Edge[Double]] = nameRdd.map(
      it => Edge(it.get("sid").toString.trim.toLong, it.get("cid").toString.trim.toLong, it.get("metric").toString.trim.toDouble)
    )

    graph = Graph(vrdd, erdd)

    graph.vertices.map(
      v => v._2.getClass match {
        case UserProperty.getClass => v._2.asInstanceOf[UserProperty].sname
      }
    )

    graph.cache()


    val visualization : SingleGraph = new SingleGraph("demo")
    visualization.addAttribute("ui.stylesheet","url(./style/demoStyle.css)")
    visualization.addAttribute("ui.quality")
    visualization.addAttribute("ui.antialias")

    for ((id,uprop) <- graph.vertices.filter{case (id,prop) => prop.isInstanceOf[UserProperty]}.collect()){
      val unode = visualization.addNode(uprop.asInstanceOf[UserProperty].sname).asInstanceOf[SingleNode]
    }

    for ((id,uprop) <- graph.vertices.filter{case (id,prop) => prop.isInstanceOf[ChampionProperty] && prop.asInstanceOf[ChampionProperty].pos.equals("辅助")}.collect()){
      val cnode = visualization.addNode(uprop.asInstanceOf[ChampionProperty].cname).asInstanceOf[SingleNode]
    }

    for (e <- graph.triplets.collect()) {
      val edge = visualization.addEdge((e.srcId+e.dstId).toString,e.srcAttr.asInstanceOf[UserProperty].sname,e.dstAttr.asInstanceOf[ChampionProperty].cname,true).asInstanceOf[AbstractEdge]
    }

    visualization.display()
//    graph.triplets.map(
//      triplet => triplet.srcAttr.asInstanceOf[UserProperty].sname + " " + triplet.attr + " " + triplet.dstAttr.asInstanceOf[ChampionProperty].cname
//    ).collect().foreach(println(_))
    spark.stop()
  }
}

