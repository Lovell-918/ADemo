package demo.Scala

import java.io.{File, PrintWriter}

import com.mongodb.spark.MongoSpark
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
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

    var graph: Graph[VertexProperty, Double] = null

    val erdd: RDD[Edge[Double]] = nameRdd.map(
      it => Edge(it.get("sid").toString.trim.toLong, it.get("c_pid").toString.trim.toLong, it.get("metric").toString.trim.toDouble)
    )

    graph = Graph(vrdd, erdd)

    var g = graph.subgraph(epred = tri => tri.dstAttr.asInstanceOf[ChampionProperty].cname.equals("酒桶")&&tri.dstAttr.asInstanceOf[ChampionProperty].pos.equals("辅助"))

    g.cache()

    g = g.outerJoinVertices(g.degrees){
      case (_,prop,Some(x)) => (prop,x)
      case (_,prop,_) => (prop,0)
    }.subgraph(vpred = {case (_,(_,x)) => x>0}).mapVertices{case (_,(prop,_)) => prop}

    val u = java.util.UUID.randomUUID
//    val sv = g.triplets.collect.map(tri => (tri.srcId,tri.srcAttr.asInstanceOf[UserProperty].sname))
//    val sv = g.vertices.filter(pred = v => v._2.isInstanceOf[UserProperty]).map(v => (v._1,v._2.asInstanceOf[UserProperty].sname)).collect
//    val dv = g.triplets.collect.map(tri => (tri.dstId,tri.dstAttr.asInstanceOf[ChampionProperty].cname,tri.dstAttr.asInstanceOf[ChampionProperty].pos))
//    val dv = g.vertices.filter(pred = v => v._2.isInstanceOf[ChampionProperty]).map{
//      v =>
//        val prop = v._2.asInstanceOf[ChampionProperty]
//        (v._1,prop.cname,prop.pos)
//    }.collect
//
//    sv.foreach(println(_))
//    dv.foreach(println(_))

    var sorted = g.triplets.sortBy(tri => tri.attr,ascending = false).collect()
    sorted = sorted.dropRight(sorted.length - 30)
    val gg = sc.parallelize(sorted)

    val sv = gg.filter(v => v.srcAttr.isInstanceOf[UserProperty]).map(v => (v.srcId,v.srcAttr.asInstanceOf[UserProperty].sname)).collect
    //    val dv = g.triplets.collect.map(tri => (tri.dstId,tri.dstAttr.asInstanceOf[ChampionProperty].cname,tri.dstAttr.asInstanceOf[ChampionProperty].pos))
    val dv = gg.filter(v => v.dstAttr.isInstanceOf[ChampionProperty]).map{
      v =>
        val prop = v.dstAttr.asInstanceOf[ChampionProperty]
        (v.dstId,prop.cname,prop.pos)
    }.collect()

    sv.foreach(println(_))
    dv.foreach(println(_))

    val html =
      """<!DOCTYPE html>
                  <html lang="en">
                        <head>
                        <meta charset="utf-8">
                        <title>Graph</title>

                  <style>
                  .node circle { fill: gray; }
                  .node text { font: 10px sans-serif;
                       text-anchor: middle;
                       fill: black; }
                  line.link { stroke: gray;
                      stroke-width: 1.5px; }
                  </style>
                  <script src="https://cdn.bootcss.com/jquery/3.4.0/jquery.min.js"></script>
                  <script src="https://d3js.org/d3.v3.min.js"></script>
                  </head>
                  <body>
                  <div id='a""" + u +"""'></div>
          <script>
                                        $.ajaxSettings.async = false;
               $.getJSON("hero.json",function (hero) {
                   hs = hero
               });
          var width = $(window).width();
           var height;
           if(parent){
                 height = $(parent).height();
           }else{
                 height = $(window).height();
           }
           var svg = d3.select("#a""" + u +
                  """").append("svg")
          .attr("width", width).attr("height", height);
          var nodes = [""" + sv.map(t2 => "{id:" + t2._1 + ", name:\""+t2._2+"\",type:\"s\"}").mkString(",") +
                  """,""" + dv.map(t3 => "{id:" + t3._1 + ",name:\""+t3._2+"\",pos:\""+t3._3+"\",type:\"c\"}").mkString(",") +
                  """];
          var links = [""" + gg.map(
                  e => "{source:nodes[" + sv.indexWhere(_._1 == e.srcId) +
                    "],target:nodes[" +
                    (dv.indexWhere(_._1 == e.dstId) + sv.length) + "],metric:"+e.attr.toString+",id:"+e.srcId.toString+e.dstId.toString+"}").collect().mkString(",") +
                  """];
          var link = svg.selectAll(".link").data(links);
          link.enter().insert("line", ".node").attr("class","link");
          var node = svg.selectAll(".node").data(nodes);
          var nodeEnter = node.enter().append("g").attr("class", "node");
          nodeEnter.append("circle").attr("r", 7);
          nodeEnter.append("image")
               .attr("xlink:href", function(d, i) {
                   if (d['type'] === "c") {
                        for (var h in hs){
                            if (hs[h]['name'] === d['name']){
                                return "https://game.gtimg.cn/images/lol/act/img/champion/"+hs[h]['alias']+'.png'
                            }
                        }
                        return "./Annie.png";
                    } else {
                        return null;
                   }
               }).attr("x", -16)
               .attr("y", -16)
               .attr("width", 32)
               .attr("height", 32);
          nodeEnter.append("text").attr("dy", function(d, i) {
               if (d['type'] === "c") {
                   return "24px";
               } else {
                   return "0.5em";
               }
           })
            .text(function(d) { return d.name + (d['type'] === 'c' ? "@" + d['pos']:""); });
          d3.layout.force().linkDistance(35).charge(-150).chargeDistance(130)
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

