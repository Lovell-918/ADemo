package demo.Scala

import java.io.{File, PrintWriter}

import com.mongodb.spark.MongoSpark
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
    case class ChampionProperty(cname: String, pos: String) extends VertexProperty
    val uvrdd: RDD[(VertexId, VertexProperty)] = nameRdd.map(
      it => (it.get("sid").toString.trim.toLong, UserProperty(it.get("sname").toString)
      )
    )
    val cvrdd: RDD[(VertexId, VertexProperty)] = nameRdd.map(
      it => (it.get("c_pid").toString.trim.toLong, ChampionProperty(it.get("cname").toString, it.get("pos").toString))
    )

    val vrdd: RDD[(VertexId, VertexProperty)] = uvrdd.++(cvrdd)
    //这里是英雄的那个

    var graph: Graph[VertexProperty, Double] = null //之前说用metric做边的权重？

    //    啊这一段是之前在胡乱尝试的
    //    case class UserName ()
    val erdd: RDD[Edge[Double]] = nameRdd.map(
      it => Edge(it.get("sid").toString.trim.toLong, it.get("c_pid").toString.trim.toLong, it.get("metric").toString.trim.toDouble)
    )

    graph = Graph(vrdd, erdd)

    graph.cache()

    val g = graph.subgraph(epred = tri => tri.attr > 25)

    val u = java.util.UUID.randomUUID
    val sv = g.triplets.collect.map(tri => (tri.srcId,tri.srcAttr.asInstanceOf[UserProperty].sname))
    val dv = g.triplets.collect.map(tri => (tri.dstId,tri.dstAttr.asInstanceOf[ChampionProperty].cname,tri.dstAttr.asInstanceOf[ChampionProperty].pos))

    val html =
      """<!DOCTYPE html>
      <html lang="en">
      <head>
      <meta charset="utf-8">
      <title>Graph</title>
<div id='a""" + u +
        """' style='width:960px; height:500px'></div>
<style>
.node circle { fill: gray; }
.node text { font: 10px sans-serif;
     text-anchor: middle;
     fill: white; }
line.link { stroke: gray;
    stroke-width: 1.5px; }
</style>
<script src="https://d3js.org/d3.v3.min.js"></script>
</head>
<body>
<script>
var width = 2000, height = 1500;
var svg = d3.select("#a""" + u +
        """").append("svg")
.attr("width", width).attr("height", height);
var nodes = [""" + sv.map(t2 => "{id:" + t2._1 + ", name:\""+t2._2+"\"}").mkString(",") +
        """,""" + dv.map(t3 => "{id:" + t3._1 + ",name:\""+t3._2+"\",pos:\""+t3._3+"\"}").mkString(",") +
        """];
var links = [""" + g.triplets.map(
        e => "{source:nodes[" + sv.indexWhere(_._1 == e.srcId) +
          "],target:nodes[" +
          (dv.indexWhere(_._1 == e.dstId) + sv.length) + "]}").collect().mkString(",") +
        """];
var link = svg.selectAll(".link").data(links);
link.enter().insert("line", ".node").attr("class", "link");
var node = svg.selectAll(".node").data(nodes);
var nodeEnter = node.enter().append("g").attr("class", "node");
nodeEnter.append("circle").attr("r", 20);
nodeEnter.append("text").attr("dy", "0.35em")
 .text(function(d) { return d.name; });
d3.layout.force().linkDistance(50).charge(-200).chargeDistance(300)
.friction(0.95).linkStrength(0.5).size([width, height])
.on("tick", function() {
link.attr("x1", function(d) { return d.source.x; })
  .attr("y1", function(d) { return d.source.y; })
  .attr("x2", function(d) { return d.target.x; })
  .attr("y2", function(d) { return d.target.y; });
node.attr("transform", function(d) {
return "translate(" + d.x + "," + d.y + ")";
});
}).nodes(nodes).links(links).start();
</script>
</body>
</html>
 """

    val writer = new PrintWriter(new File(".\\graph\\graph.html"))
    writer.print(html)
    writer.close()

    //    val visualization: SingleGraph = new SingleGraph("demo")
    //    visualization.addAttribute("ui.stylesheet", "url(./style/demoStyle.css)")
    //    visualization.addAttribute("ui.quality")
    //    visualization.addAttribute("ui.antialias")
    //
    //    for ((id, uprop) <- graph.vertices.filter { case (id, prop) => prop.isInstanceOf[UserProperty] }.collect()) {
    //      val unode = visualization.addNode(id.toString).asInstanceOf[SingleNode]
    //      unode.addAttribute("ui.label", uprop.asInstanceOf[UserProperty].sname)
    //    }
    //
    //    for ((id, uprop) <- graph.vertices.filter { case (id, prop) => prop.isInstanceOf[ChampionProperty] }.collect()) {
    //      val cnode = visualization.addNode(id.toString).asInstanceOf[SingleNode]
    //      cnode.addAttribute("ui.label", uprop.asInstanceOf[ChampionProperty].pos + " " + uprop.asInstanceOf[ChampionProperty].cname)
    //    }
    //
    //    var c = 1
    //    for (e <- graph.triplets.collect()) {
    //      val edge = visualization.addEdge(c.toString, e.srcId.toString, e.dstId.toString, true).asInstanceOf[AbstractEdge]
    //      c = c + 1
    //    }
    //
    //    visualization.display()
    //    graph.triplets.map(
    //      triplet => triplet.srcAttr.asInstanceOf[UserProperty].sname + " " + triplet.attr + " " + triplet.dstAttr.asInstanceOf[ChampionProperty].cname
    //    ).collect().foreach(println(_))
    spark.stop()
  }

}

