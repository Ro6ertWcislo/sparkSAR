package demo

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.reflect.io.Path
import demo._

object Main {

  type Coords = (Double,Double)
  def helloSentence = "Hello GeoTrellis"

  def avg(x:Double,y:Double): Double = {
    val a = (x+y)/2
  a}
  def headAvg(head:Coords,tail:Coords):Coords={
    (avg(head._1,tail._1),head._2)
  }

  def tailAvg(head:Coords,tail:Coords):Coords={
    (head._1,avg(head._2,tail._2))
  }
  def main(args: Array[String]): Unit = {
    val conf = new org.apache.spark.SparkConf()
      .set("spark.executor.memory", "4096m")
      .set("spark.memory.fraction", "0.2")
      .set("spark.kryoserializer.buffer.max", "1g")

    conf.setMaster("local[*]")
    implicit val sc:SparkContext = geotrellis.spark.util.SparkUtils.createSparkContext("sarDemo", conf)

    // musi być COG tiff inaczej outofmemory
    // gdal_translate ori.tiff out.tiff -co COMPRESS=LZW -co TILED=YES

    val rdd2: RDD[(ProjectedExtent,Tile)] = HadoopGeoTiffRDD.spatial("/home/robert/Downloads/tif2/measurement/s1b-iw-grd-vv-20190605t075801-20190605t075818-016558-01f2c1-001.tiff")
    println(rdd2)
    rdd2.sortBy{case (extent,_) => (extent.ymax,extent.xmax)}.sliding(2).flatMap{
      case Array(head,tail) => {
       head.horizontalMerge(tail)
      }
    }.collect()


    println("\nMy watch has ended")

  }
}
