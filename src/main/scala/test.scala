import java.util

import org.apache.spark._
import org.apache.spark.graphx._

import scala.util.control._
import org.apache.spark.graphx.lib._
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable
import scala.reflect.ClassTag




object test {
  var type_a_c = 0
  var type_b_c = 0
  var type_c_c = 0
  var type_d_c = 0


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/new_file.txt", false  )
      .partitionBy(PartitionStrategy.RandomVertexCut)

    val vertices = graph.vertices.collect().map {
      case (vertexId, attrs) => s"($vertexId)[$attrs]"
    }

    val edges = graph.edges.collect().map(edge => s"(${edge.srcId})--[${edge.attr}]-->(${edge.dstId})")

//    println(vertices.length)
//    println(edges.length)

//    val neighbourToVertexOne = graph.collectNeighborIds(EdgeDirection.Either).lookup(1)
//    println("start")
//    neighbourToVertexOne.head.foreach(println)
//    println("end")
    type VertexSet = scala.collection.mutable.Set[VertexId]


//    graph.edges.collect().foreach(x => println(x.srcId + " -> " + x.dstId))
    def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {
      // Construct set representations of the neighborhoods
      val nbrSets: VertexRDD[VertexSet] =
        graph.collectNeighborIds(EdgeDirection.Out).mapValues { (vid, nbrs) =>
          val set = scala.collection.mutable.Set[VertexId]()
          var i = 0
          while (i < nbrs.length) {
            // prevent self cycle
            if (nbrs(i) != vid) {
              set.add(nbrs(i))
            }
            i += 1
          }
          set
        }

      // join the sets with the graph
      val setGraph: Graph[VertexSet, ED] = graph.outerJoinVertices(nbrSets) {
        (vid, _, optSet) => optSet.getOrElse(null)
      }

      val vert_list = setGraph.vertices.collect()

      val hash: mutable.HashMap[VertexId, VertexSet] = mutable.HashMap.empty

      vert_list.foreach(x => hash += (x._1 -> x._2))

      def edgeFunc(ctx: EdgeContext[VertexSet, ED, Int]) {

        val iter = ctx.dstAttr.iterator

        if (!ctx.dstAttr.contains(ctx.srcId)){

          while (iter.hasNext) {
            val vid = iter.next()

            if (hash.contains(vid)) {
              if (!hash(vid).contains(ctx.dstId)) {
                if (hash(vid).contains(ctx.srcId) && !ctx.srcAttr.contains(vid)) {
                  type_a_c += 1
                }
                if (!hash(vid).contains(ctx.srcId) && ctx.srcAttr.contains(vid)) {
                  type_b_c += 1
                }
                if (hash(vid).contains(ctx.srcId) && ctx.srcAttr.contains(vid)) {
                  type_c_c += 1
                }
                if (!hash(vid).contains(ctx.srcId) && !ctx.srcAttr.contains(vid)) {
                  type_d_c += 1
                }
              }

            }

          }

        }


      }


      val counters: VertexRDD[Int] = setGraph.aggregateMessages(edgeFunc, _ + _)

      graph.outerJoinVertices(counters) { (_, _, optCounter: Option[Int]) =>
        val dblCount = optCounter.getOrElse(0)
        // This algorithm double counts each triangle so the final count should be even
//        require(dblCount % 2 == 0, "Triangle count resulted in an invalid number of triangles.")
        dblCount / 2
      }


    }

    val count = run(graph).vertices
    count.collect()

    println(type_a_c, type_b_c, type_c_c, type_d_c)

    // type_count variables agar object mai define karu then not serializable error atai
    // but if main mai leke auu then prog chal jayega but count badega he nai


  }

}

